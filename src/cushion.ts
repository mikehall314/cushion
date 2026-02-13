/**
 * Cushion - CouchDB-inspirted API on top of Deno KV
 */

import type { ViewQuery } from "./view-query.ts";
import { encodeHex } from "jsr:@std/encoding/hex";
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
type ViewLogic = { map: MapFunction; reduce?: ReduceFunction };
type ViewState = { signature: string; state: "building" | "ready" };
type ViewRow = { value: unknown; doc: StoredDocument };

type InsertResult = { ok: boolean; id: string; rev: string };

export class Cushion {
  #kv: Deno.Kv;
  #namespace: string;
  #views = new Map<string, ViewLogic>();

  /**
   * Open a Cushion database
   * @param namespace - Namespace for data isolation (default: "default")
   * @param kv - Optional Deno.Kv instance (will create one if not provided)
   */
  static async open(namespace = "default", kv?: Deno.Kv): Promise<Cushion> {
    const kvInstance = kv ?? await Deno.openKv();
    return new Cushion(kvInstance, namespace);
  }

  close(): void {
    this.#kv.close();
  }

  private constructor(kv: Deno.Kv, namespace: string) {
    this.#kv = kv;
    this.#namespace = namespace;
  }

  /**
   * Get a document by ID
   * Returns null if document doesn't exist
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

    await this.updateViewsForDoc(id, {
      ...initialiser,
      _rev: result.versionstamp,
    });

    return { ok: true, id, rev: result.versionstamp };
  }

  /**
   * Replace an existing document
   * Requires matching rev for optimistic locking
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

    await this.updateViewsForDoc(id, { ...updated, _rev: result.versionstamp });

    return { ok: true, id, rev: result.versionstamp };
  }

  /**
   * Remove a document
   * Requires matching rev for optimistic locking
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

    await this.updateViewsForDoc(id, null);

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
   * Automatically rebuilds if the map function has changed
   */
  async defineView(
    viewName: string,
    mapper: MapFunction,
    reducer?: ReduceFunction,
  ): Promise<void> {
    // Store the map-reduce logic so we can do incremental updates later
    this.#views.set(viewName, { map: mapper, reduce: reducer });

    // This could be a new map function, in which case we need to build it. Or
    // it could have been modified since we last saw it, in which case we need
    // to rebuild it.
    const viewSignature = await this.#hashFunction(mapper);
    const design = getDesignKey(this.#namespace, viewName);

    const { value } = await this.#kv.get<ViewState>(design);

    // If the signature is up to date, we don't need to do anything
    if (value?.signature === viewSignature) {
      return;
    }

    // If the view is already being built, we don't need to do anything
    if (value?.state === "building") {
      return;
    }

    // Otherwise, kick off a rebuild
    await this.rebuildView(viewName, mapper, viewSignature);
  }

  private async rebuildView(
    viewName: string,
    mapper: MapFunction,
    signature: string,
  ): Promise<void> {
    const BATCH_SIZE = 1_000;

    // Delete all existing view entries using batched atomic deletes
    const viewPrefix = getViewKey(this.#namespace, viewName);
    const viewRefPrefix = getViewRefPrefix(this.#namespace, viewName);

    let deleteCount = 0;
    let deleteAtomic = this.#kv.atomic();

    for (const prefix of [viewPrefix, viewRefPrefix]) {
      for await (const entry of this.#kv.list({ prefix })) {
        deleteAtomic.delete(entry.key);
        deleteCount += 1;

        if (deleteCount >= BATCH_SIZE) {
          await deleteAtomic.commit();
          deleteAtomic = this.#kv.atomic();
          deleteCount = 0;
        }
      }
    }

    if (deleteCount > 0) {
      await deleteAtomic.commit();
    }

    // Load all the documents and run the map function over them
    const docPrefix = getDocPrefix(this.#namespace);
    const docs = this.#kv.list<Document>({ prefix: docPrefix });

    let batch = this.#kv.atomic();
    let batchSize = 0;

    for await (const { value: doc } of docs) {
      const emittedKeys: Deno.KvKey[] = [];

      const emit: ViewEmitter = (key, value) => {
        const keyParts = Array.isArray(key) ? key : [key];
        const viewKey = [...viewPrefix, ...keyParts, doc._id];
        emittedKeys.push(viewKey);
        batch.set(viewKey, { value: value ?? null, doc });
        batchSize += 1;
      };

      mapper(doc, emit);

      const refKey = getViewRefKey(this.#namespace, viewName, doc._id);
      batch.set(refKey, emittedKeys);
      batchSize += 1;

      if (batchSize >= BATCH_SIZE) {
        await batch.commit();
        batch = this.#kv.atomic();
        batchSize = 0;
      }
    }

    if (batchSize > 0) {
      await batch.commit();
    }

    // Mark the view as ready
    const designKey = getDesignKey(this.#namespace, viewName);
    await this.#kv.set(
      designKey,
      { signature, state: "ready" } satisfies ViewState,
    );
  }

  private async updateViewsForDoc(
    docId: string,
    doc: Document | null,
  ): Promise<void> {
    for (const [viewName, { map: mapper }] of this.#views) {
      const viewPrefix = getViewKey(this.#namespace, viewName);
      const refKey = getViewRefKey(this.#namespace, viewName, docId);

      const atomic = this.#kv.atomic();

      // Delete old emitted rows
      const existing = await this.#kv.get<Deno.KvKey[]>(refKey);
      if (existing.value) {
        for (const key of existing.value) {
          atomic.delete(key);
        }
      }

      // If the doc was deleted, then we can stop here.
      if (doc === null) {
        atomic.delete(refKey);
        await atomic.commit();
        continue;
      }

      const emittedKeys: Deno.KvKey[] = [];

      const emit: ViewEmitter = (key, value) => {
        const keyParts = Array.isArray(key) ? key : [key];
        const viewKey = [...viewPrefix, ...keyParts, docId];
        emittedKeys.push(viewKey);
        atomic.set(viewKey, { value: value ?? null, doc });
      };

      mapper(doc, emit);
      atomic.set(refKey, emittedKeys);

      await atomic.commit();
    }
  }

  async *query(viewQuery: ViewQuery): AsyncGenerator<unknown> {
    const { viewName, ...params } = viewQuery.getParams();

    // Check the view exists
    const viewDef = this.#views.get(viewName);
    if (!viewDef) {
      throw new Error(`View "${viewName}" not defined`);
    }

    const viewPrefix = getViewKey(this.#namespace, viewName);

    // Build selector based on query type
    let selector: Deno.KvListSelector;

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
      limit: params.skip ? params.limit + params.skip : params.limit,
    };

    const entries = this.#kv.list<ViewRow>(selector, listOptions);

    // Handle reduce case
    if (params.reduce && viewDef.reduce) {
      yield* this.#queryReduce(entries, viewDef.reduce, {
        viewName,
        ...params,
      });
      return;
    }

    // Map-only case, just stream results
    let skipped = 0;
    for await (const entry of entries) {
      if (skipped < params.skip) {
        skipped += 1;
        continue;
      }

      const { value, doc } = entry.value;

      yield {
        key: entry.key.slice(viewPrefix.length, -1),
        id: entry.key.at(-1) as string,
        value,
        ...(params.includeDocs ? { doc } : {}),
      };
    }
  }

  async *#queryReduce(
    entries: AsyncIterable<Deno.KvEntry<ViewRow>>,
    reduceFn: ReduceFunction,
    params: ReturnType<ViewQuery["getParams"]>,
  ): AsyncGenerator<unknown> {
    const viewPrefix = getViewKey(this.#namespace, params.viewName);
    const prefixLen = viewPrefix.length;
    const groupLevel = "groupLevel" in params ? params.groupLevel : undefined;

    // deno-lint-ignore no-explicit-any
    const groups = new Map<string, { keys: any[]; values: any[] }>();

    for await (const entry of entries) {
      const emittedKey = entry.key.slice(prefixLen, -1);
      const docId = entry.key.at(-1) as string;

      const groupKey = groupLevel !== undefined
        ? JSON.stringify(
          groupLevel === 0 ? emittedKey : emittedKey.slice(0, groupLevel),
        )
        : "ALL";

      const group = groups.get(groupKey) ?? { keys: [], values: [] };
      group.keys.push([emittedKey, docId]);
      group.values.push(entry.value);
      groups.set(groupKey, group);
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

      const result = reduceFn(keys, values);

      yield {
        key: groupKey === "ALL" ? null : JSON.parse(groupKey),
        value: result,
      };

      yielded += 1;
    }
  }
}
