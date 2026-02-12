import { assertObjectMatch, assertThrows } from "assert";
import { describe, it } from "bdd";
import { ViewQuery } from "./view-query.ts";

describe("ViewQuery", () => {
  describe("common fields", () => {
    it("should store the view name", () => {
      const query = ViewQuery.for("test-view");
      assertObjectMatch(query.getParams(), { viewName: "test-view" });
    });

    it("should have reduce default to false", () => {
      const query = ViewQuery.for("test-view");
      assertObjectMatch(query.getParams(), { reduce: false });
    });
  });

  describe("sorting", () => {
    it("should allow explicitly setting descending sort order", () => {
      const query = ViewQuery.for("test-view").order(ViewQuery.DESCENDING);
      assertObjectMatch(query.getParams(), { descending: true });
    });

    it("should allow explicitly setting ascending sort order", () => {
      const query = ViewQuery.for("test-view").order(ViewQuery.ASCENDING);
      assertObjectMatch(query.getParams(), { descending: false });
    });

    it("should default to ascending sort order", () => {
      const query = ViewQuery.for("test-view");
      assertObjectMatch(query.getParams(), { descending: false });
    });

    it("should allow resetting sort order ", () => {
      const query = ViewQuery.for("test-view");

      // Default to ascending
      assertObjectMatch(query.getParams(), { descending: false });

      // Reset to descending
      query.order(ViewQuery.DESCENDING);
      assertObjectMatch(query.getParams(), { descending: true });

      // Reset to ascending
      query.order(ViewQuery.ASCENDING);
      assertObjectMatch(query.getParams(), { descending: false });
    });
  });

  describe("naive pagination", () => {
    it("should allow setting skip and limit", () => {
      const query = ViewQuery.for("test-view").skip(10).limit(5);
      assertObjectMatch(query.getParams(), { skip: 10, limit: 5 });
    });

    it("should default to unbounded", () => {
      const query = ViewQuery.for("test-view");
      assertObjectMatch(query.getParams(), { skip: 0, limit: Infinity });
    });
  });

  describe("inlining documents", () => {
    it("should allow explicitly requesting documents", () => {
      const query = ViewQuery.for("test-view").includeDocs(true);
      assertObjectMatch(query.getParams(), { includeDocs: true });
    });

    it("should allow explicitly refusing documents", () => {
      const query = ViewQuery.for("test-view").includeDocs(false);
      assertObjectMatch(query.getParams(), { includeDocs: false });
    });

    it("should implicitly request documents when called without arguments", () => {
      const query = ViewQuery.for("test-view").includeDocs();
      assertObjectMatch(query.getParams(), { includeDocs: true });
    });

    it("should default to refusing documents", () => {
      const query = ViewQuery.for("test-view");
      assertObjectMatch(query.getParams(), { includeDocs: false });
    });
  });

  describe("getting view", () => {
    it("should load a single key", () => {
      const query = ViewQuery.for("test-view").key("fake-key");
      assertObjectMatch(query.getParams(), { type: "key", key: "fake-key" });
    });

    it("should load multiple keys", () => {
      const query = ViewQuery.for("test-view").keys([
        "test-key-1",
        "test-key-2",
      ]);
      assertObjectMatch(query.getParams(), {
        type: "keys",
        keys: ["test-key-1", "test-key-2"],
      });
    });

    it("should load by key prefix", () => {
      const query = ViewQuery.for("test-view").prefix(["pre"]);
      assertObjectMatch(query.getParams(), { type: "prefix", prefix: ["pre"] });
    });

    it("should load by key range", () => {
      const query = ViewQuery.for("test-view").range(["123"], ["abc"]);
      assertObjectMatch(query.getParams(), {
        type: "range",
        startKey: ["123"],
        endKey: ["abc"],
      });
    });

    it("should load by compound range", () => {
      const query = ViewQuery.for("test-view").range(["abc"], ["abc", true]);
      assertObjectMatch(query.getParams(), {
        type: "range",
        startKey: ["abc"],
        endKey: ["abc", true],
      });
    });

    it("should load range with document subkeys", () => {
      const query = ViewQuery.for("test-view")
        .range(["123"], ["123", true])
        .idRange("earlydoc", "laterdoc");

      assertObjectMatch(query.getParams(), {
        type: "range",
        startKey: ["123"],
        endKey: ["123", true],
        startKeyDocId: "earlydoc",
        endKeyDocId: "laterdoc",
      });
    });
  });

  describe("reducing and grouping", () => {
    it("should allow explicitly setting reduce flag", () => {
      const query = ViewQuery.for("test-view").reduce(true);
      assertObjectMatch(query.getParams(), { reduce: true });
    });

    it("should allow explicitly unsetting reduce flag", () => {
      const query = ViewQuery.for("test-view").reduce(false);
      assertObjectMatch(query.getParams(), { reduce: false });
    });

    it("should implicitly set reduce flag", () => {
      const query = ViewQuery.for("test-view").reduce();
      assertObjectMatch(query.getParams(), { reduce: true });
    });

    it("should explicitly setting group flag", () => {
      const query = ViewQuery.for("test-view").group(true);
      assertObjectMatch(query.getParams(), { groupLevel: 0, reduce: true });
    });

    it("should implicitly set group flag", () => {
      const query = ViewQuery.for("test-view").group();
      assertObjectMatch(query.getParams(), { groupLevel: 0, reduce: true });
    });

    it("should allow explicitly explicitly disable grouping", () => {
      const query = ViewQuery.for("test-view").group(false);
      assertObjectMatch(query.getParams(), {
        groupLevel: undefined,
        reduce: false,
      });
    });

    it("should not reset reduce when disabling grouping", () => {
      const query = ViewQuery.for("test-view").reduce().group(false);
      assertObjectMatch(query.getParams(), {
        groupLevel: undefined,
        reduce: true,
      });
    });

    it("should allow setting specific group levels", () => {
      const query = ViewQuery.for("test-view").group(1);
      assertObjectMatch(query.getParams(), { groupLevel: 1, reduce: true });
    });

    it("should discard float group level", () => {
      const query = ViewQuery.for("test-view").group(Math.PI);
      assertObjectMatch(query.getParams(), { groupLevel: 3, reduce: true });
    });

    it("should throw for invalid group levels", () => {
      assertThrows(
        () => ViewQuery.for("test-view").group(-1),
        Error,
        "Group level must be boolean or positive integer",
      );
    });
  });

  describe("operator precedence", () => {
    it("should prioritise key over keys", () => {
      const query = ViewQuery.for("test-view").prefix(["pre"]).range(["a"], [
        "b",
      ]).key("c");
      assertObjectMatch(query.getParams(), { type: "key", key: "c" });
    });

    it("should prioritise keys over prefix", () => {
      const query = ViewQuery.for("test-view").prefix(["pre"]).range(["a"], [
        "b",
      ]).keys(["x", "y"]);
      assertObjectMatch(query.getParams(), { type: "keys", keys: ["x", "y"] });
    });

    it("should prioritise prefix over range", () => {
      const query = ViewQuery.for("test-view").prefix(["pre"]).range(["a"], [
        "b",
      ]);
      assertObjectMatch(query.getParams(), { type: "prefix", prefix: ["pre"] });
    });

    it("should prioritise range over scan", () => {
      const query = ViewQuery.for("test-view").range(["a"], ["b"]);
      assertObjectMatch(query.getParams(), {
        type: "range",
        startKey: ["a"],
        endKey: ["b"],
      });
    });

    it("should default to scan when no other key parameters are set", () => {
      const query = ViewQuery.for("test-view");
      assertObjectMatch(query.getParams(), { type: "scan" });
    });
  });
});
