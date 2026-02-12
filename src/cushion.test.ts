// deno-lint-ignore-file no-explicit-any
import {
  assert,
  assertEquals,
  assertNotEquals,
  assertObjectMatch,
  assertRejects,
} from "assert";
import { afterEach, beforeEach, describe, it } from "bdd";
import { Cushion } from "./cushion.ts";
import { ViewQuery } from "./view-query.ts";

// Helper to collect async generator into array
async function collect<T>(gen: AsyncIterable<T>): Promise<T[]> {
  const results: T[] = [];
  for await (const item of gen) {
    results.push(item);
  }
  return results;
}

describe("Cushion API", () => {
  let db: Cushion;

  beforeEach(async () => {
    db = await Cushion.open("test", await Deno.openKv(":memory:"));
  });

  afterEach(() => db.close());

  // --- CRUD ---

  describe("insert", () => {
    it("generates id if not provided", async () => {
      const result = await db.insert({ type: "user", name: "Alice" });
      assert(result.ok);
      assert(result.id);
      assert(result.rev);
    });

    it("uses provided _id", async () => {
      const result = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });
      assertEquals(result.id, "alice");
    });

    it("rejects duplicate _id", async () => {
      await db.insert({ _id: "alice", type: "user", name: "Alice" });
      await assertRejects(
        () => db.insert({ _id: "alice", type: "user", name: "Alice 2" }),
        Error,
        "already exists",
      );
    });
  });

  describe("get", () => {
    it("returns document with _rev", async () => {
      const { id, rev } = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });
      const doc = await db.get(id);
      assert(doc);
      assertObjectMatch(doc, { _id: "alice", name: "Alice", _rev: rev });
    });

    it("returns null for missing document", async () => {
      const doc = await db.get("nonexistent");
      assertEquals(doc, null);
    });
  });

  describe("replace", () => {
    it("updates document with correct rev", async () => {
      const { id, rev } = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });
      const result = await db.replace(id, rev, {
        type: "user",
        name: "Alice Updated",
      });
      assert(result.ok);
      assertNotEquals(result.rev, rev);

      const doc = await db.get(id);
      assert(doc);
      assertEquals(doc.name, "Alice Updated");
    });

    it("rejects stale rev", async () => {
      const { id, rev } = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });
      await db.replace(id, rev, { type: "user", name: "Alice v2" });

      await assertRejects(
        () => db.replace(id, rev, { type: "user", name: "Alice v3" }),
        Error,
        "rev conflict",
      );
    });
  });

  describe("remove", () => {
    it("deletes document with correct rev", async () => {
      const { id, rev } = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });
      const result = await db.remove(id, rev);
      assert(result.ok);

      const doc = await db.get(id);
      assertEquals(doc, null);
    });

    it("rejects stale rev", async () => {
      const { id, rev } = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });
      await db.replace(id, rev, { type: "user", name: "Alice v2" });

      await assertRejects(
        () => db.remove(id, rev),
        Error,
      );

      const doc = await db.get(id);
      assert(doc);
    });
  });

  // --- Views ---

  describe("creating views", () => {
    it("builds view on first call", async () => {
      await db.insert({ _id: "alice", type: "user", name: "Alice", age: 32 });
      await db.insert({ _id: "bob", type: "user", name: "Bob", age: 25 });

      await db.defineView("by-name", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit(doc.name);
      });

      const rows = await collect(db.query(ViewQuery.for("by-name")));
      assertEquals(rows.length, 2);
    });

    it("skips rebuild if function unchanged", async () => {
      await db.insert({ _id: "alice", type: "user", name: "Alice" });

      let callCount = 0;
      const mapper = (doc: any, emit: any) => {
        callCount += 1;
        emit(doc.name);
      };

      await db.defineView("by-name", mapper);
      await db.defineView("by-name", mapper);

      // Should only have built the view once
      assertEquals(callCount, 1);
    });
  });

  describe("incremental view updates", () => {
    beforeEach(async () => {
      await db.defineView("by-name", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit(doc.name);
      });
    });

    it("updates on insert", async () => {
      await db.insert({ _id: "alice", type: "user", name: "Alice" });
      await db.insert({ _id: "bob", type: "user", name: "Bob" });

      const rows = await collect(db.query(ViewQuery.for("by-name")));
      assertEquals(rows.length, 2);
    });

    it("updates on replace", async () => {
      const { id, rev } = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });

      let rows = await collect(db.query(ViewQuery.for("by-name").key("Alice")));
      assertEquals(rows.length, 1);

      await db.replace(id, rev, { type: "user", name: "Alicia" });

      rows = await collect(db.query(ViewQuery.for("by-name").key("Alice")));
      assertEquals(rows.length, 0);

      rows = await collect(db.query(ViewQuery.for("by-name").key("Alicia")));
      assertEquals(rows.length, 1);
    });

    it("updates on remove", async () => {
      const { id, rev } = await db.insert({
        _id: "alice",
        type: "user",
        name: "Alice",
      });

      let rows = await collect(db.query(ViewQuery.for("by-name")));
      assertEquals(rows.length, 1);

      await db.remove(id, rev);

      rows = await collect(db.query(ViewQuery.for("by-name")));
      assertEquals(rows.length, 0);
    });
  });

  // --- Query types ---

  describe("query", () => {
    beforeEach(async () => {
      await db.defineView("by-name", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit(doc.name);
      });

      await db.insert({ type: "user", name: "Alice" });
      await db.insert({ type: "user", name: "Bob" });
      await db.insert({ type: "user", name: "Charlie" });
      await db.insert({ type: "user", name: "Diana" });
    });

    describe("scan", () => {
      it("returns all rows sorted", async () => {
        const rows = await collect(db.query(ViewQuery.for("by-name")));

        assertEquals(rows.length, 4);

        const names = rows.map((r: any) => r.key[0]);
        assertEquals(names, ["Alice", "Bob", "Charlie", "Diana"]);
      });
    });

    describe("key", () => {
      it("returns matching rows", async () => {
        const rows = await collect(
          db.query(ViewQuery.for("by-name").key("Alice")),
        );
        assertEquals(rows.length, 1);
        assertEquals((rows[0] as any).key[0], "Alice");
      });
    });

    describe("prefix", () => {
      it("returns rows matching prefix", async () => {
        await db.defineView("by-dept-name", (doc, emit) => {
          if (doc.type !== "user" || !doc.department) {
            return;
          }

          emit([doc.department, doc.name]);
        });

        await db.insert({
          type: "user",
          name: "Alice",
          department: "engineering",
        });

        await db.insert({
          type: "user",
          name: "Bob",
          department: "engineering",
        });

        await db.insert({
          type: "user",
          name: "Charlie",
          department: "sales",
        });

        const rows = await collect(
          db.query(ViewQuery.for("by-dept-name").prefix(["engineering"])),
        );
        assertEquals(rows.length, 2);
      });
    });

    describe("range", () => {
      it("returns rows in range (start inclusive, end exclusive)", async () => {
        const rows = await collect(
          db.query(ViewQuery.for("by-name").range(["Bob"], ["Diana"])),
        );
        assertEquals(rows.length, 2);

        const names = rows.map((r: any) => r.key[0]);
        assertEquals(names, ["Bob", "Charlie"]);
      });
    });

    describe("descending", () => {
      it("returns rows in reverse order", async () => {
        const rows = await collect(
          db.query(ViewQuery.for("by-name").order(ViewQuery.DESCENDING)),
        );
        const names = rows.map((r: any) => r.key[0]);
        assertEquals(names, ["Diana", "Charlie", "Bob", "Alice"]);
      });
    });

    describe("limit", () => {
      it("limits number of results", async () => {
        const rows = await collect(
          db.query(ViewQuery.for("by-name").limit(2)),
        );
        assertEquals(rows.length, 2);
      });
    });

    describe("skip", () => {
      it("skips n results", async () => {
        const rows = await collect(
          db.query(ViewQuery.for("by-name").skip(1)),
        );
        assertEquals(rows.length, 3);
        assertEquals((rows[0] as any).key[0], "Bob");
      });
    });

    describe("skip and limit", () => {
      it("skips then limits", async () => {
        const rows = await collect(
          db.query(ViewQuery.for("by-name").skip(1).limit(2)),
        );
        assertEquals(rows.length, 2);
        assertEquals((rows[0] as any).key[0], "Bob");
        assertEquals((rows[1] as any).key[0], "Charlie");
      });
    });

    describe("includeDocs", () => {
      it("includes full document when enabled", async () => {
        await db.defineView("by-name", (doc, emit) => {
          if (doc.type !== "doc-user") {
            return;
          }

          emit(doc.name);
        });

        await db.insert({
          _id: "alice",
          type: "doc-user",
          name: "Alice",
          age: 32,
        });

        const rows = await collect(
          db.query(ViewQuery.for("by-name").includeDocs()),
        );

        assertEquals(rows.length, 1);

        const row = rows[0] as any;
        assertObjectMatch(row.doc, { _id: "alice", name: "Alice", age: 32 });
      });

      it("omits doc when not enabled", async () => {
        const rows = await collect(
          db.query(ViewQuery.for("by-name")),
        );
        const row = rows[0] as any;
        assertEquals(row.doc, undefined);
      });
    });

    it("throws for undefined view", async () => {
      await assertRejects(
        async () => {
          await collect(db.query(ViewQuery.for("nonexistent")));
        },
        Error,
        "not defined",
      );
    });
  });

  // --- Reduce ---

  describe("reduce", () => {
    beforeEach(async () => {
      await db.defineView(
        "by-dept",
        (doc, emit) => {
          if (doc.type !== "user") {
            return;
          }
          emit(doc.department);
        },
        (keys, _values) => keys.length,
      );

      await db.insert({
        type: "user",
        name: "Alice",
        department: "engineering",
      });
      await db.insert({ type: "user", name: "Bob", department: "engineering" });
      await db.insert({ type: "user", name: "Charlie", department: "sales" });
    });

    it("reduces all rows without grouping", async () => {
      const rows = await collect(
        db.query(ViewQuery.for("by-dept").reduce()),
      );

      assertEquals(rows.length, 1);
      assertEquals((rows[0] as any).key, null);
      assertEquals((rows[0] as any).value, 3);
    });

    it("groups by full key with group(true)", async () => {
      const rows = await collect(
        db.query(ViewQuery.for("by-dept").reduce().group(true)),
      );

      assertEquals(rows.length, 2);

      const eng = rows.find((r: any) => r.key[0] === "engineering") as any;
      const sales = rows.find((r: any) => r.key[0] === "sales") as any;
      assertEquals(eng.value, 2);
      assertEquals(sales.value, 1);
    });

    it("groups by level with group(n)", async () => {
      await db.defineView("by-dept-name", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit([doc.department, doc.name]);
      }, (keys, _values) => keys.length);

      const rows = await collect(
        db.query(ViewQuery.for("by-dept-name").reduce().group(1)),
      );

      assertEquals(rows.length, 2);

      const eng = rows.find((r: any) => r.key[0] === "engineering") as any;
      assertEquals(eng.value, 2);

      const sales = rows.find((r: any) => r.key[0] === "sales") as any;
      assertEquals(sales.value, 1);
    });

    it("reduces on key query", async () => {
      const rows = await collect(
        db.query(ViewQuery.for("by-dept").key("engineering").reduce()),
      );
      assertEquals(rows.length, 1);
      assertEquals((rows[0] as any).value, 2);
    });
  });

  // --- Pagination ---

  describe("pagination with startKeyDocId", () => {
    it("paginates without overlap using skip(1)", async () => {
      await db.defineView("by-dept", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit(doc.department);
      });

      await db.insert({
        type: "user",
        name: "Alice",
        department: "engineering",
      });

      await db.insert({
        type: "user",
        name: "Bob",
        department: "engineering",
      });

      await db.insert({
        type: "user",
        name: "Charlie",
        department: "engineering",
      });

      await db.insert({
        type: "user",
        name: "Diana",
        department: "engineering",
      });

      const page1 = await collect(
        db.query(
          ViewQuery.for("by-dept")
            .range(["engineering"], ["engineering\xff"])
            .limit(2),
        ),
      );

      assertEquals(page1.length, 2);

      const lastId = (page1[1] as any).id;

      const page2 = await collect(
        db.query(
          ViewQuery.for("by-dept")
            .range(["engineering"], ["engineering\xff"])
            .idRange(lastId, "")
            .skip(1)
            .limit(2),
        ),
      );
      assertEquals(page2.length, 2);

      const page1Ids = page1.map((r: any) => r.id);
      const page2Ids = page2.map((r: any) => r.id);
      for (const id of page2Ids) {
        assertEquals(page1Ids.includes(id), false);
      }
    });
  });

  // --- Compound keys ---

  describe("compound keys", () => {
    it("emits and queries compound keys", async () => {
      await db.defineView("by-dept-name", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit([doc.department, doc.name]);
      });

      await db.insert({
        type: "user",
        name: "Alice",
        department: "engineering",
      });

      await db.insert({
        type: "user",
        name: "Bob",
        department: "sales",
      });

      const rows = await collect(
        db.query(ViewQuery.for("by-dept-name").prefix(["engineering"])),
      );

      assertEquals(rows.length, 1);
      assertEquals((rows[0] as any).key, ["engineering", "Alice"]);
    });
  });

  // --- Emit values ---

  describe("emit values", () => {
    it("returns emitted value", async () => {
      await db.defineView("ages", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit(doc.name, doc.age);
      });

      await db.insert({ type: "user", name: "Alice", age: 32 });

      const rows = await collect(db.query(ViewQuery.for("ages")));
      assertEquals(rows.length, 1);
      assertObjectMatch(rows[0] as any, { value: 32 });
    });
  });

  // --- Filtering ---

  describe("view filtering", () => {
    it("excludes docs not matching view filter", async () => {
      await db.defineView("by-name", (doc, emit) => {
        if (doc.type !== "user") {
          return;
        }
        emit(doc.name);
      });

      await db.insert({
        type: "user",
        name: "Alice",
      });

      await db.insert({
        type: "post",
        title: "Hello World",
      });

      const rows = await collect(db.query(ViewQuery.for("by-name")));
      assertEquals(rows.length, 1);
    });
  });

  // --- Close ---

  describe("close", () => {
    it("closes the connection", async () => {
      const db2 = await Cushion.open("test2", await Deno.openKv(":memory:"));
      db2.close();

      await assertRejects(
        () => db2.insert({ type: "test" }),
        Error,
      );
    });
  });
});
