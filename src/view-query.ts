type ViewQueryParameters =
  & {
    viewName: string;
    reduce: boolean;
    includeDocs: boolean;
    descending: boolean;
    limit: number;
    skip: number;
  }
  & (
    | { type: "scan"; groupLevel?: number }
    | { type: "key"; key: Deno.KvKeyPart }
    | { type: "keys"; keys: Deno.KvKeyPart[] }
    | { type: "prefix"; prefix: Deno.KvKeyPart[] }
    | {
      type: "range";
      startKey: Deno.KvKeyPart[];
      endKey: Deno.KvKeyPart[];
      startKeyDocId?: string;
      endKeyDocId?: string;
      groupLevel?: number;
    }
  );

type SortOrder = typeof ViewQuery.DESCENDING | typeof ViewQuery.ASCENDING;

export class ViewQuery {
  static readonly DESCENDING = Symbol("descending");
  static readonly ASCENDING = Symbol("ascending");

  #viewName: string;

  #limit = Infinity;
  #skip = 0;
  #reduce = false;
  #includeDocs = false;
  #descending = false;

  #key?: string;
  #keys?: string[];
  #prefix?: Deno.KvKeyPart[];
  #startKey?: Deno.KvKeyPart[];
  #endKey?: Deno.KvKeyPart[];
  #startKeyDocId?: string;
  #endKeyDocId?: string;
  #groupLevel?: number;

  private constructor(viewName: string) {
    this.#viewName = viewName;
  }

  static for(viewName: string): ViewQuery {
    return new ViewQuery(viewName);
  }

  /**
   * Query a single key
   */
  key(key: string): this {
    this.#key = key;
    return this;
  }

  /**
   * Query multiple specific keys
   */
  keys(keys: string[]): this {
    this.#keys = keys;
    return this;
  }

  /**
   * Query all keys that start with the given prefix
   */
  prefix(prefix: Deno.KvKeyPart[]): this {
    this.#prefix = prefix;
    return this;
  }

  /**
   * Query a range of keys
   * Note: startKey is inclusive, endKey is exclusive (DenoKV limitation)
   */
  range(startKey: Deno.KvKeyPart[], endKey: Deno.KvKeyPart[]): this {
    this.#startKey = startKey;
    this.#endKey = endKey;
    return this;
  }

  idRange(startId: string, endId: string): this {
    this.#startKeyDocId = startId;
    this.#endKeyDocId = endId;
    return this;
  }

  /**
   * Skip n results
   * @description Negative values are treated as zero. This should not be used
   * for large offsets as performance will degrade significantly; use range
   * queries instead.
   */
  skip(n: number): this {
    this.#skip = ~~Math.max(0, n);
    return this;
  }

  /**
   * Limit number of results
   * @description Negative values are treated as zero.
   */
  limit(n: number): this {
    this.#limit = ~~Math.max(0, n);
    return this;
  }

  /**
   * Include full documents in results
   */
  includeDocs(shouldInclude = true): this {
    this.#includeDocs = shouldInclude;
    return this;
  }

  /**
   * Set sort order
   */
  order(direction: SortOrder): this {
    this.#descending = direction === ViewQuery.DESCENDING;
    return this;
  }

  /**
   * Control whether to apply the view's reduce function
   */
  reduce(shouldReduce = true): this {
    this.#reduce = shouldReduce;
    return this;
  }

  /**
   * Control the grouping of reduced results
   */
  group(level: boolean | number = true): this {
    if (level === true || level === 0) {
      this.#reduce = true;
      this.#groupLevel = 0;
      return this;
    }

    if (level === false) {
      this.#groupLevel = undefined;
      return this;
    }

    if (typeof level === "number" && level > 0) {
      this.#reduce = true;
      this.#groupLevel = ~~level;
      return this;
    }

    throw new TypeError("Group level must be boolean or positive integer");
  }

  /**
   * Get structured query parameters
   * @description Determines the type of query based on which parameters have
   * been set (key, keys, prefix, range, or full scan)
   */
  getParams(): ViewQueryParameters {
    if (this.#key !== undefined) {
      return {
        type: "key",
        key: this.#key,
        limit: this.#limit,
        skip: this.#skip,
        includeDocs: this.#includeDocs,
        descending: this.#descending,
        reduce: this.#reduce,
        viewName: this.#viewName,
      };
    }

    if (this.#keys !== undefined) {
      return {
        type: "keys",
        keys: this.#keys,
        limit: this.#limit,
        skip: this.#skip,
        includeDocs: this.#includeDocs,
        descending: this.#descending,
        reduce: this.#reduce,
        viewName: this.#viewName,
      };
    }

    if (this.#prefix !== undefined) {
      return {
        type: "prefix",
        prefix: this.#prefix,
        limit: this.#limit,
        skip: this.#skip,
        includeDocs: this.#includeDocs,
        descending: this.#descending,
        reduce: this.#reduce,
        viewName: this.#viewName,
      };
    }

    if (this.#startKey !== undefined && this.#endKey !== undefined) {
      return {
        type: "range",
        startKey: this.#startKey,
        endKey: this.#endKey,
        startKeyDocId: this.#startKeyDocId,
        endKeyDocId: this.#endKeyDocId,
        limit: this.#limit,
        skip: this.#skip,
        includeDocs: this.#includeDocs,
        descending: this.#descending,
        reduce: this.#reduce,
        viewName: this.#viewName,
        groupLevel: this.#groupLevel,
      };
    }

    // Default: full scan
    return {
      type: "scan",
      limit: this.#limit,
      skip: this.#skip,
      includeDocs: this.#includeDocs,
      descending: this.#descending,
      reduce: this.#reduce,
      viewName: this.#viewName,
      groupLevel: this.#groupLevel,
    };
  }
}
