import { Client } from "pg";
import { PgIndex } from "../../src/types.js";
import { createIndexLoaders, indexQueries } from "../../src/loaders/pg_indexes.js";

// Mock the pg Client
jest.mock("pg", () => {
  const mockQuery = jest.fn();
  const Client = jest.fn(() => ({
    query: mockQuery,
  }));
  return { Client };
});

describe("pg_indexes", () => {
  let mockClient: {
    query: jest.Mock<Promise<{ rows: PgIndex[] }>, any[]>;
  };

  // Mock index objects to use in tests
  const mockIndex1: PgIndex = {
    indexrelid: 12345,
    indrelid: 54321,
    indexam: "btree",
    indkey: "1 2",
    indexdef: "CREATE INDEX idx1 ON test_table USING btree (col1, col2)",
  };

  const mockIndex2: PgIndex = {
    indexrelid: 67890,
    indrelid: 54321,
    indexam: "hash",
    indkey: "3",
    indexdef: "CREATE INDEX idx2 ON test_table USING hash (col3)",
  };

  const mockIndex3: PgIndex = {
    indexrelid: 11111,
    indrelid: 22222,
    indexam: "btree",
    indkey: "1",
    indexdef: "CREATE INDEX idx3 ON other_table USING btree (col1)",
  };

  beforeEach(() => {
    mockClient = {
      query: jest.fn(),
    };
    jest.clearAllMocks();
  });

  describe("indexQueries", () => {
    describe("query", () => {
      it("queries indexes with default options", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2, mockIndex3] });

        const result = await indexQueries.query(mockClient as unknown as Client);

        expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining("SELECT"), []);
        expect(result).toEqual([mockIndex1, mockIndex2, mockIndex3]);
      });

      it("queries indexes with indexOids filter", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1] });

        const result = await indexQueries.query(mockClient as unknown as Client, {
          indexOids: [12345],
        });

        expect(mockClient.query).toHaveBeenCalledWith(
          expect.stringContaining("WHERE x.indexrelid = ANY($1)"),
          [[12345]]
        );
        expect(result).toEqual([mockIndex1]);
      });

      it("queries indexes with relationOids filter", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2] });

        const result = await indexQueries.query(mockClient as unknown as Client, {
          relationOids: [54321],
        });

        expect(mockClient.query).toHaveBeenCalledWith(
          expect.stringContaining("WHERE x.indrelid = ANY($1)"),
          [[54321]]
        );
        expect(result).toEqual([mockIndex1, mockIndex2]);
      });

      it("queries indexes with schemaNames filter", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2] });

        const result = await indexQueries.query(mockClient as unknown as Client, {
          schemaNames: ["public"],
        });

        expect(mockClient.query).toHaveBeenCalledWith(
          expect.stringContaining("WHERE n.nspname = ANY($1)"),
          [["public"]]
        );
        expect(result).toEqual([mockIndex1, mockIndex2]);
      });

      it("queries indexes with includeSystemSchemas option", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1] });

        const result = await indexQueries.query(mockClient as unknown as Client, {
          includeSystemSchemas: true,
        });

        // Should NOT contain the system schemas exclusion
        const query = mockClient.query.mock.calls[0][0];
        expect(query).not.toContain(
          `n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`
        );
        expect(result).toEqual([mockIndex1]);
      });

      it("excludes system schemas by default", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1] });

        const result = await indexQueries.query(mockClient as unknown as Client, {});

        // Should contain the system schemas exclusion
        const query = mockClient.query.mock.calls[0][0];
        expect(query).toContain(
          `n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`
        );
        expect(result).toEqual([mockIndex1]);
      });

      it("combines multiple filter conditions", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1] });

        const result = await indexQueries.query(mockClient as unknown as Client, {
          indexOids: [12345],
          relationOids: [54321],
          schemaNames: ["public"],
        });

        // Check that all three conditions are in the query
        const query = mockClient.query.mock.calls[0][0];
        expect(query).toContain("x.indexrelid = ANY($1)");
        expect(query).toContain("x.indrelid = ANY($2)");
        expect(query).toContain("n.nspname = ANY($3)");

        expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining("WHERE"), [
          [12345],
          [54321],
          ["public"],
        ]);
        expect(result).toEqual([mockIndex1]);
      });
    });

    describe("byOid", () => {
      it("returns an index when found", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1] });

        const result = await indexQueries.byOid(mockClient as unknown as Client, 12345);

        expect(mockClient.query).toHaveBeenCalledWith(
          expect.stringContaining("SELECT"),
          expect.anything()
        );
        expect(result).toEqual(mockIndex1);
      });

      it("returns null when index not found", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [] });

        const result = await indexQueries.byOid(mockClient as unknown as Client, 99999);

        expect(result).toBeNull();
      });
    });

    describe("byRelationOid", () => {
      it("returns indexes for a relation", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2] });

        const result = await indexQueries.byRelationOid(mockClient as unknown as Client, 54321);

        expect(mockClient.query).toHaveBeenCalledWith(
          expect.stringContaining("SELECT"),
          expect.anything()
        );
        expect(result).toEqual([mockIndex1, mockIndex2]);
      });
    });
  });

  describe("createIndexLoaders", () => {
    describe("indexLoader", () => {
      it("loads a single index by OID", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1] });

        const { indexLoader } = createIndexLoaders(mockClient as unknown as Client);
        const result = await indexLoader.load(12345);

        expect(mockClient.query).toHaveBeenCalledTimes(1);
        expect(result).toEqual(mockIndex1);
      });

      it("loads multiple indexes by OID in a single query", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2] });

        const { indexLoader } = createIndexLoaders(mockClient as unknown as Client);
        const results = await Promise.all([indexLoader.load(12345), indexLoader.load(67890)]);

        // Should only make one query for both indexes
        expect(mockClient.query).toHaveBeenCalledTimes(1);
        expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining("WHERE"), [
          [12345, 67890],
        ]);
        expect(results).toEqual([mockIndex1, mockIndex2]);
      });

      it("returns null for non-existent indexes", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1] });

        const { indexLoader } = createIndexLoaders(mockClient as unknown as Client);
        const results = await Promise.all([indexLoader.load(12345), indexLoader.load(99999)]);

        expect(results[0]).toEqual(mockIndex1);
        expect(results[1]).toBeNull();
      });
    });

    describe("indexesByRelationLoader", () => {
      it("loads indexes by relation OID", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2] });

        const { indexesByRelationLoader } = createIndexLoaders(mockClient as unknown as Client);
        const result = await indexesByRelationLoader.load(54321);

        expect(mockClient.query).toHaveBeenCalledTimes(1);
        expect(result).toEqual([mockIndex1, mockIndex2]);
      });

      it("loads indexes for multiple relations in a single query", async () => {
        mockClient.query.mockResolvedValueOnce({
          rows: [mockIndex1, mockIndex2, mockIndex3],
        });

        const { indexesByRelationLoader } = createIndexLoaders(mockClient as unknown as Client);
        const results = await Promise.all([
          indexesByRelationLoader.load(54321),
          indexesByRelationLoader.load(22222),
        ]);

        // Should only make one query for both relations
        expect(mockClient.query).toHaveBeenCalledTimes(1);
        expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining("WHERE"), [
          [54321, 22222],
        ]);
        expect(results).toEqual([[mockIndex1, mockIndex2], [mockIndex3]]);
      });

      it("returns empty array for relations with no indexes", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2] });

        const { indexesByRelationLoader } = createIndexLoaders(mockClient as unknown as Client);
        const results = await Promise.all([
          indexesByRelationLoader.load(54321),
          indexesByRelationLoader.load(99999),
        ]);

        expect(results[0]).toEqual([mockIndex1, mockIndex2]);
        expect(results[1]).toEqual([]);
      });
    });

    describe("getAllIndexes", () => {
      it("returns all indexes", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2, mockIndex3] });

        const { getAllIndexes } = createIndexLoaders(mockClient as unknown as Client);
        const result = await getAllIndexes();

        expect(mockClient.query).toHaveBeenCalledTimes(1);
        expect(result).toEqual([mockIndex1, mockIndex2, mockIndex3]);
      });

      it("filters indexes with custom filter function", async () => {
        mockClient.query.mockResolvedValueOnce({ rows: [mockIndex1, mockIndex2, mockIndex3] });

        const { getAllIndexes } = createIndexLoaders(mockClient as unknown as Client);
        // Only get btree indexes
        const result = await getAllIndexes((index) => index.indexam === "btree");

        expect(mockClient.query).toHaveBeenCalledTimes(1);
        expect(result).toEqual([mockIndex1, mockIndex3]);
      });
    });
  });
});
