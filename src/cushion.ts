/**
 * Cushion - CouchDB-inspired API on top of Deno KV
 */

import type { ViewQuery } from "./view-query.ts";
import { encodeHex } from "hex";
import { batchedAtomic } from "batched-atomic";
import {
  getDesignKey,
  getDocPrefix,
  getDocumentKey,
  getViewKey,
  getViewRefKey,
  getViewRefPrefix,
} from "./utils.ts";

// deno-lint-ignore no-explicit-any
type Document = { _id: string; _rev: string; [key: string]: any };
// deno-lint-ignore no-explicit-any
type StoredDocument = { _id: string; [key: string]: any };
type MaybeDocument = Partial<Document>;
type ViewEmitKey = Deno.KvKeyPart | Deno.KvKeyPart[];
type ViewEmitter = (key: ViewEmitKey, value?: unknown) => void;
type MapFunction = (doc: StoredDocument, emit: ViewEmitter) => void;
// deno-lint-ignore no-explicit-any
type ReduceFunction = (keys: Deno.KvKeyPart[][], values: any[]) => unknown;
type ViewLogic = {
  map: MapFunction;
  reduce?: ReduceFunction;
  signature: string;
};
type ViewState = { signature: string; state: "building" | "ready" };
type ViewRow = { value: unknown; doc: StoredDocument };

type InsertResult = { ok: boolean; id: string; rev: string };

export type MapRow<
  TValue = unknown,
  TDoc extends StoredDocument = StoredDocument,
> = {
  /**
   * The emitted key
   */
  key: Deno.KvKeyPart[];

  /**
   * The emitted value
   */
  value: TValue;

  /**
   * The document ID
   */

  id: string;
  /**
   * The full document, if requested
   */
  doc?: TDoc;
};

export type ReduceRow<TValue = unknown> = {
  /**
   * The group key, or null if not grouped
   */
  key: Deno.KvKeyPart[] | null;

  /**
   * The reduced value
   */
  value: TValue;
};

export class Cushion {
  #kv: Deno.Kv;
  #namespace: string;
  #views = new Map<string, ViewLogic>();

  /**
   * Open a Cushion database
   * @param namespace Namespace for data isolation (default: "default")
   * @param kv Optional Deno.Kv instance (will open one if not provided)
   */
  static async open(namespace = "default", kv?: Deno.Kv): Promise<Cushion> {
    const kvInstance = kv ?? await Deno.openKv();
    return new Cushion(kvInstance, namespace);
  }

  /**
   * Close the database connection
   */
  close(): void {
    this.#kv.close();
  }

  private constructor(kv: Deno.Kv, namespace: string) {
    this.#kv = kv;
    this.#namespace = namespace;
  }

  /**
   * Get a document by its ID
   * @param id Document ID to get
   * @returns Specified document, or null if not found
   */
  async get<T extends Document>(id: string): Promise<T | null> {
    const key = getDocumentKey(this.#namespace, id);
    const result = await this.#kv.get<T>(key);

    if (result.value === null) {
      return null;
    }

    return {
      ...result.value,
      _rev: result.versionstamp,
    };
  }

  /**
   * Insert a document into the database
   * If doc has _id, use it; otherwise generate one
   * Returns the inserted document with _id and _rev
   * @param doc New document to create
   */
  async insert<T extends MaybeDocument>(doc: T): Promise<InsertResult> {
    // Generate ID if not provided
    const id = doc._id || crypto.randomUUID();

    // If the document includes a _rev, we should reject it, since insert is
    // only for new documents.
    if (Object.hasOwn(doc, "_rev")) {
      throw new Error("Document must not include _rev");
    }

    const key = getDocumentKey(this.#namespace, id);
    const initialiser = { ...doc, _id: id };

    // Create a new document only if it doesn't already exist
    const result = await this.#kv.atomic()
      .check({ key, versionstamp: null })
      .set(key, initialiser)
      .commit();

    if (result.ok === false) {
      throw new Error(`Document ${id} already exists; use replace()`);
    }

    await this.#updateViewsForDoc(id, {
      ...initialiser,
      _rev: result.versionstamp,
    });

    return { ok: true, id, rev: result.versionstamp };
  }

  /**
   * Replace an existing document
   * @param id Document ID to replace
   * @param rev Current document revision
   * @param doc New document
   */
  async replace<T extends MaybeDocument>(
    id: string,
    rev: string,
    doc: T,
  ): Promise<InsertResult> {
    const key = getDocumentKey(this.#namespace, id);

    // Ensure the document includes the _id and omits the _rev,
    // which is stored as the versionstamp in Deno KV.
    const { _rev, ...rest } = doc;
    const updated: StoredDocument = { ...rest, _id: id };

    // Update the document only if the rev matches
    const result = await this.#kv.atomic()
      .check({ key, versionstamp: rev })
      .set(key, updated)
      .commit();

    if (result.ok === false) {
      throw new Error(
        `Document ${id} has been modified elsewhere (rev conflict)`,
      );
    }

    await this.#updateViewsForDoc(id, {
      ...updated,
      _rev: result.versionstamp,
    });

    return { ok: true, id, rev: result.versionstamp };
  }

  /**
   * Remove a document
   * @param id Document ID to remove
   * @param rev Current document revision
   */
  async remove(id: string, rev: string): Promise<{ ok: boolean }> {
    const key = getDocumentKey(this.#namespace, id);

    // Remove only if the user knows the current rev
    const result = await this.#kv.atomic()
      .check({ key, versionstamp: rev })
      .delete(key).commit();

    if (result.ok === false) {
      throw new Error(
        `Document ${id} has been modified elsewhere (rev conflict) or does not exist`,
      );
    }

    await this.#updateViewsForDoc(id, null);

    return { ok: true };
  }

  async #hashFunction(fn: MapFunction): Promise<string> {
    const buffer = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(fn.toString()),
    );
    return encodeHex(new Uint8Array(buffer));
  }

  /**
   * Define a view with a map function and optional reduce function
   * Automatically rebuilds if the map function has changed.
   * @param viewName Name of the view to define
   * @param map Map function to generate view entries
   * @param reduce Optional reduce function to aggregate view results. Runs at query time.
   */
  async defineView(
    viewName: string,
    map: MapFunction,
    reduce?: ReduceFunction,
  ): Promise<void> {
    // Store the map-reduce logic so we can do incremental updates later
    const signature = await this.#hashFunction(map);
    this.#views.set(viewName, { map, reduce, signature });

    // If this view is already indexed/being indexed, we dont need to repeat it.
    const design = getDesignKey(this.#namespace, signature);
    const { value } = await this.#kv.get<ViewState>(design);
    if (value?.signature === signature) {
      return;
    }

    // Otherwise, start a build
    await this.#rebuildView(viewName, map, signature);
  }

  async #rebuildView(
    viewName: string,
    map: MapFunction,
    signature: string,
  ): Promise<void> {
    // We are building this view
    const design = getDesignKey(this.#namespace, signature);
    await this.#kv.set(
      design,
      { signature, state: "building" } satisfies ViewState,
    );

    const atomic = batchedAtomic(this.#kv);

    // Remove the old view data
    const viewRefPrefix = getViewRefPrefix(this.#namespace, viewName);
    const viewPrefix = getViewKey(this.#namespace, viewName, signature);
    for (const prefix of [viewPrefix, viewRefPrefix]) {
      for await (const entry of this.#kv.list({ prefix })) {
        atomic.delete(entry.key);
      }
    }

    // Map over all the documents in the database and emit new view entries
    const prefix = getDocPrefix(this.#namespace);
    const docs = this.#kv.list<Document>({ prefix });
    for await (const { value: doc } of docs) {
      const refs: Deno.KvKey[] = [];

      const emit: ViewEmitter = (key, value) => {
        const keyParts = Array.isArray(key) ? key : [key];
        const viewKey = [...viewPrefix, ...keyParts, doc._id];
        refs.push(viewKey);
        atomic.set(viewKey, { value: value ?? null });
      };

      map(doc, emit);

      const refKey = getViewRefKey(this.#namespace, viewName, doc._id);
      atomic.set(refKey, refs);
    }

    // Mark the view as ready
    const designKey = getDesignKey(this.#namespace, signature);
    atomic.set(designKey, { signature, state: "ready" } satisfies ViewState);

    await atomic.commit();
  }

  async #updateViewsForDoc(docId: string, doc: Document | null): Promise<void> {
    const atomic = batchedAtomic(this.#kv);

    for (const [viewName, { map, signature }] of this.#views) {
      const refKey = getViewRefKey(this.#namespace, viewName, docId);

      // Delete the existing refs for this document
      const refs = await this.#kv.get<Deno.KvKey[]>(refKey);
      for (const key of refs.value ?? []) {
        atomic.delete(key);
      }

      // If the doc was deleted, then we can stop here.
      if (doc === null) {
        atomic.delete(refKey);
        continue;
      }

      // Run the new document through the map function to update the view
      const emittedKeys: Deno.KvKey[] = [];
      const viewPrefix = getViewKey(this.#namespace, viewName, signature);
      const emit: ViewEmitter = (key, value) => {
        const keyParts = Array.isArray(key) ? key : [key];
        const viewKey = [...viewPrefix, ...keyParts, docId];
        emittedKeys.push(viewKey);
        atomic.set(viewKey, { value: value ?? null, doc });
      };

      map(doc, emit);

      // Save the new refs
      atomic.set(refKey, emittedKeys);
    }

    await atomic.commit();
  }

  /**
   * Perform a query on a view.
   * Returns an async generator that yields results as they are read from the
   * database.
   * @param query ViewQuery object specifying the query parameters
   */
  async *query<T = MapRow | ReduceRow>(query: ViewQuery): AsyncGenerator<T> {
    const { viewName, ...params } = query.getParams();

    // Check the view exists
    const viewDef = this.#views.get(viewName);
    if (!viewDef) {
      throw new Error(`View "${viewName}" not defined`);
    }

    const design = getDesignKey(this.#namespace, viewDef.signature);
    const { value: viewState } = await this.#kv.get<ViewState>(design);
    if (viewState?.state === "building") {
      await this.#waitForView(viewDef.signature);
    }

    // Build selector based on query type
    let selector: Deno.KvListSelector;
    const viewPrefix = getViewKey(this.#namespace, viewName, viewDef.signature);

    switch (params.type) {
      case "key":
        selector = { prefix: [...viewPrefix, params.key] };
        break;

      case "prefix":
        selector = { prefix: [...viewPrefix, ...params.prefix] };
        break;

      case "range":
        selector = {
          start: [...viewPrefix, ...params.startKey],
          end: [...viewPrefix, ...params.endKey],
        };

        if (params.startKeyDocId) {
          selector.start = [...selector.start, params.startKeyDocId];
        }

        if (params.endKeyDocId) {
          selector.end = [...selector.end, params.endKeyDocId];
        }

        break;

      case "scan":
        selector = { prefix: [...viewPrefix] };
        break;

      case "keys":
        throw new Error("Not yet implemented: query by multiple keys");

      default:
        throw new Error();
    }

    const listOptions: Deno.KvListOptions = {
      reverse: params.descending,
      limit: params.skip ? (params.limit + params.skip) : params.limit,
    };

    const entries = this.#kv.list<ViewRow>(selector, listOptions);

    // Handle reduce case
    if (params.reduce && viewDef.reduce) {
      yield* this.#queryReduce<T>(
        entries,
        viewDef.reduce,
        viewDef.signature,
        { viewName, ...params },
      );
      return;
    }

    // Map-only case, just stream results
    let skipped = 0;
    for await (const entry of entries) {
      if (skipped < params.skip) {
        skipped += 1;
        continue;
      }

      const { value } = entry.value;
      const id = entry.key.at(-1) as string;

      const doc = {} as { doc?: StoredDocument };
      if (params.includeDocs) {
        const docKey = getDocumentKey(this.#namespace, id);
        const document = await this.#kv.get<StoredDocument>(docKey);
        doc.doc = document.value ?? undefined;
      }

      const key = entry.key.slice(viewPrefix.length, -1);
      yield { key, id, value, ...doc } as T;
    }
  }

  async *#queryReduce<T = ReduceRow>(
    entries: AsyncIterable<Deno.KvEntry<ViewRow>>,
    reduce: ReduceFunction,
    signature: string,
    params: ReturnType<ViewQuery["getParams"]>,
  ): AsyncGenerator<T> {
    const viewPrefix = getViewKey(this.#namespace, params.viewName, signature);
    const groupLevel = "groupLevel" in params ? params.groupLevel : undefined;
    const prefixLen = viewPrefix.length;

    // deno-lint-ignore no-explicit-any
    const groups = new Map<string, { keys: any[]; values: any[] }>();

    for await (const entry of entries) {
      const emittedKey = entry.key.slice(prefixLen, -1);
      const groupKey = groupLevel !== undefined
        ? JSON.stringify(
          groupLevel === 0 ? emittedKey : emittedKey.slice(0, groupLevel),
        )
        : "ALL";

      if (groups.has(groupKey) === false) {
        groups.set(groupKey, { keys: [], values: [] });
      }

      const group = groups.get(groupKey)!;
      group.keys.push(emittedKey);
      group.values.push(entry.value.value);
    }

    let skipped = 0;
    let yielded = 0;

    for (const [groupKey, { keys, values }] of groups) {
      if (skipped < params.skip) {
        skipped += 1;
        continue;
      }

      if (yielded >= params.limit) {
        break;
      }

      const key = groupKey === "ALL" ? null : JSON.parse(groupKey);
      const value = reduce(keys, values);
      yield { key, value } as T;

      yielded += 1;
    }
  }

  // Wait for a view to finish building. This could be slow if you have
  // a lot of documents
  async #waitForView(signature: string): Promise<void> {
    const design = getDesignKey(this.#namespace, signature);
    const reader = this.#kv.watch<ViewState[]>([design]).getReader();

    try {
      while (true) {
        const { value } = await reader.read();
        const [entry] = value ?? [];
        if (entry.value?.state === "ready") {
          return;
        }
      }
    } finally {
      reader.cancel();
    }
  }
}
