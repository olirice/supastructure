import {
  sortItems,
  paginate,
  decodeId,
  buildGlobalId,
  singleResultOrError,
} from "../src/generic.js";

describe("generic", () => {
  it("should sort items in ascending order", () => {
    const items = [{ value: 2 }, { value: 1 }, { value: 3 }];
    const sorted = sortItems(items, (item) => item.value, "ASC");
    expect(sorted).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
  });

  it("should sort items in descending order", () => {
    const items = [{ value: 2 }, { value: 1 }, { value: 3 }];
    const sorted = sortItems(items, (item) => item.value, "DESC");
    expect(sorted).toEqual([{ value: 3 }, { value: 2 }, { value: 1 }]);
  });

  it("should paginate items", () => {
    const items = Array.from({ length: 20 }, (_, i) => ({ id: i + 1 }));
    const result = paginate(items, {
      first: 5,
      cursorForNode: (node) => String(node.id),
    });
    expect(result.edges).toHaveLength(5);
    expect(result.pageInfo.hasNextPage).toBe(true);
  });

  it("should paginate items with first and after", () => {
    const items = [
      { id: 1, name: "item1" },
      { id: 2, name: "item2" },
      { id: 3, name: "item3" },
    ];

    // First page
    const firstPage = paginate(items, {
      first: 2,
      cursorForNode: (node) => String(node.id),
    });
    expect(firstPage.edges).toHaveLength(2);
    expect(firstPage.pageInfo.hasNextPage).toBe(true);
    expect(firstPage.pageInfo.endCursor).toBe(Buffer.from("2").toString("base64"));

    const afterCursor = firstPage.pageInfo.endCursor ?? undefined;

    // Second page
    const secondPage = paginate(items, {
      first: 2,
      after: afterCursor,
      cursorForNode: (node) => String(node.id),
    });
    expect(secondPage.edges).toHaveLength(1);
    expect(secondPage.pageInfo.hasNextPage).toBe(false);
    expect(secondPage.pageInfo.endCursor).toBe(Buffer.from("3").toString("base64"));
  });

  it("should decode a valid ID", () => {
    const id = buildGlobalId("TestType", 123);
    const decoded = decodeId(id);
    expect(decoded).toEqual({ typeName: "TestType", oid: 123 });
  });

  it("should return invalid values for an invalid ID", () => {
    const decoded = decodeId("invalid_id");
    expect(decoded?.oid).toBeNaN();
    expect(typeof decoded?.typeName).toBe("string");
  });

  it("should handle invalid base64 string in decodeId", () => {
    const invalidBase64 = "invalid_base64";
    const decoded = decodeId(invalidBase64);
    expect(decoded).toEqual({ typeName: invalidBase64, oid: NaN });
  });

  it("should build a global ID", () => {
    const id = buildGlobalId("TestType", 123);
    expect(id).toBe(Buffer.from("TestType:123").toString("base64"));
  });

  it("should return a single result or error", () => {
    const items = [{ id: 1 }];
    const result = singleResultOrError(items, "TestEntity");
    expect(result).toEqual({ id: 1 });
  });

  it("should throw an error for multiple results", () => {
    const items = [{ id: 1 }, { id: 2 }];
    expect(() => singleResultOrError(items, "TestEntity")).toThrow();
  });
});
