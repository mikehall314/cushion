# Cushion

A CouchDB-style document database built on [Deno KV](https://deno.com/kv).

Cushion gives you documents with optimistic concurrency, materialised map-reduce
views, and a query builder — backed by Deno KV.

## Quick Start

```ts
import { Cushion } from "./mod.ts";

const db = await Cushion.open();

// Insert a document
const { id, rev } = await db.insert({
  _id: "alice",
  type: "user",
  name: "Alice",
  age: 32,
  department: "engineering",
});

// Get it back
const doc = await db.get(id);

// Update it (requires current rev)
const result = await db.replace(id, rev, {
  type: "user",
  name: "Alice",
  age: 33,
  department: "engineering",
});

// Delete it
await db.remove(id, result.rev);
```

## Documents

Documents are plain objects. If you don't provide an `_id`, one will be 
generated for you. Revisions (`_rev`) are managed automatically using Deno KV's
versionstamp, giving you optimistic concurrency control for free.

```ts
// Auto-generated ID
const { id } = await db.insert({ type: "post", title: "Hello World" });

// Explicit ID
await db.insert({ _id: "post-1", type: "post", title: "Hello World" });

// Updates require the current rev
const doc = await db.get("post-1");
await db.replace(doc._id, doc._rev, { ...doc, title: "Updated" });
```

If someone else modifies a document between your read and write, the replace 
will fail with a revision conflict error.

## Views

Views are materialised map-reduce indexes, inspired by CouchDB. Define a view 
with a map function (and optional reduce), and Cushion builds and maintains the
index automatically.

```ts
import { ViewQuery, type MapRow } from "./mod.ts";

// Define a view
await db.defineView("by-dept", (doc, emit) => {
  if (doc.type !== "user") {
    return;
  }
  emit(doc.department);
});

// Query it
const query = ViewQuery.for("by-dept");
for await (const row of db.query<MapRow<string>>(query)) {
  console.log(row.key, row.id, row.value);
}
```

Views are rebuilt automatically if the map function changes. When documents are
inserted, replaced, or removed, all registered views are updated incrementally.

### Compound Keys

Emit arrays as keys for multi-dimensional sorting and range queries:

```ts
await db.defineView("by-dept-name", (doc, emit) => {
  if (doc.type !== "user") {
    return;
  }
  emit([doc.department, doc.name]);
});
```

### Emit Values

Pass a second argument to `emit` to store a value alongside the key:

```ts
await db.defineView("ages", (doc, emit) => {
  if (doc.type !== "user") {
    return;
  }
  emit(doc.name, doc.age);  
});
```

### Reduce

Add a reduce function to aggregate results:

```ts
import type { ReduceRow } from "./mod.ts";

await db.defineView("count-by-dept", (doc, emit) => {
  if (doc.type !== "user") {
    return;
  }
  emit(doc.department);
}, (keys, values) => keys.length);

// Total count
const query = ViewQuery.for("count-by-dept").reduce();
for await (const row of db.query<ReduceRow<number>>(query)) {
  console.log(row.value); // 10
}

// Count per department
for await (const row of db.query<ReduceRow<number>>(query.group())) {
  console.log(row.key, row.value); // ["engineering"] 4
}
```

Use `group(n)` with compound keys to group at a specific level.

## Querying

`ViewQuery` provides a fluent builder for constructing queries.

### Query Types

```ts
// Full scan
ViewQuery.for("by-name")

// Single key
ViewQuery.for("by-name").key("Alice")

// Prefix match (useful with compound keys)
ViewQuery.for("by-dept-name").prefix(["engineering"])

// Range (start inclusive, end exclusive)
ViewQuery.for("by-name").range(["Bob"], ["Eve"])
```

### Options

```ts
// Limit and skip. Skip is expensive, use only for small values.
ViewQuery.for("by-name").limit(10).skip(20)

// Descending order
ViewQuery.for("by-name").order(ViewQuery.DESCENDING)

// Include full documents in results
ViewQuery.for("by-name").includeDocs()

// Reduce with grouping
ViewQuery.for("count-by-dept").reduce().group(true)
ViewQuery.for("by-dept-name").reduce().group(1) // group by first key part
```

### Cursor-based pagination

Use `idRange` with `skip(1)` for pagination through rows that share the same key:

```ts
// Page 1
const query = ViewQuery.for("by-dept")
    .range(["engineering"], ["engineering\xff"])
    .limit(10);

const page1 = [];
for await (const row of db.query(query)) {
  page1.push(row);
}

// Page 2
for await (const row of db.query(query.idRange(page1.at(-1).id, "").skip(1))) {
  // ...
}
```

## Namespaces

Use namespaces to isolate data within the same KV store:

```ts
const users = await Cushion.open("users");
const logs = await Cushion.open("logs");
```

You can also inject your own `Deno.Kv` instance:

```ts
const kv = await Deno.openKv("./my-database.sqlite");
const db = await Cushion.open("default", kv);
```

## Running Tests

```bash
deno test --unstable-kv
```

## License

MIT