import { Client } from "pg";
import { createForeignKeyLoaders, foreignKeyQueries } from "../../src/loaders/pg_foreign_keys.js";
import { PgForeignKey } from "../../src/types.js";

// Mock the pg Client
jest.mock("pg", () => {
  const mockQuery = jest.fn();
  return {
    Client: jest.fn().mockImplementation(() => ({
      query: mockQuery,
      connect: jest.fn(),
      end: jest.fn(),
    })),
  };
});

describe("pg_foreign_keys loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Mock foreign key data
  const mockForeignKey1: PgForeignKey = {
    oid: 1,
    conname: "fk_test_1",
    conrelid: 100, // Table with foreign key
    confrelid: 200, // Referenced table
    confupdtype: "a", // NO_ACTION on update
    confdeltype: "c", // CASCADE on delete
    conkey: [1, 2], // Array of column numbers in source table
    confkey: [3, 4], // Array of column numbers in referenced table
  };

  const mockForeignKey2: PgForeignKey = {
    oid: 2,
    conname: "fk_test_2",
    conrelid: 100, // Same table as FK1
    confrelid: 300, // Different referenced table
    confupdtype: "r", // RESTRICT on update
    confdeltype: "n", // SET NULL on delete
    conkey: [5],
    confkey: [6],
  };

  const mockForeignKey3: PgForeignKey = {
    oid: 3,
    conname: "fk_test_3",
    conrelid: 300, // Different table
    confrelid: 200, // Same referenced table as FK1
    confupdtype: "d", // SET DEFAULT on update
    confdeltype: "a", // NO_ACTION on delete
    conkey: [1],
    confkey: [3],
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("foreignKeyQueries", () => {
    it("queries foreign keys with default options", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey2],
      });

      const result = await foreignKeyQueries.query(client);

      expect(result).toHaveLength(2);
      expect(result[0]).toEqual(mockForeignKey1);
      expect(result[1]).toEqual(mockForeignKey2);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("SELECT"), []);
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining("pg_catalog.pg_constraint c"),
        []
      );
    });

    it("queries foreign keys with constraint OIDs filter", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1],
      });

      const result = await foreignKeyQueries.query(client, { constraintOids: [1] });

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual(mockForeignKey1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("c.oid = ANY($1)"), [[1]]);
    });

    it("queries foreign keys with relation OIDs filter", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey2],
      });

      const result = await foreignKeyQueries.query(client, { relationOids: [100] });

      expect(result).toHaveLength(2);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("c.conrelid = ANY($1)"), [
        [100],
      ]);
    });

    it("queries foreign keys with referenced relation OIDs filter", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey3],
      });

      const result = await foreignKeyQueries.query(client, { referencedRelationOids: [200] });

      expect(result).toHaveLength(2);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("c.confrelid = ANY($1)"), [
        [200],
      ]);
    });

    it("queries foreign keys with schema names filter", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1],
      });

      const result = await foreignKeyQueries.query(client, { schemaNames: ["public"] });

      expect(result).toHaveLength(1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("n.nspname = ANY($1)"), [
        ["public"],
      ]);
    });

    it("includes system schemas when specified", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1],
      });

      const result = await foreignKeyQueries.query(client, { includeSystemSchemas: true });

      expect(result).toHaveLength(1);
      // Should not have the NOT IN clause for system schemas
      expect(mockQuery).not.toHaveBeenCalledWith(
        expect.stringContaining("n.nspname NOT IN ('pg_catalog','information_schema')"),
        []
      );
    });

    it("gets foreign key by OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1],
      });

      const result = await foreignKeyQueries.byOid(client, 1);
      expect(result).not.toBeNull();
      expect(result).toEqual(mockForeignKey1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("c.oid = ANY($1)"), [[1]]);
    });

    it("returns null when foreign key not found by OID", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [] });

      const result = await foreignKeyQueries.byOid(client, 999);
      expect(result).toBeNull();
    });

    it("gets foreign keys by relation OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey2],
      });

      const result = await foreignKeyQueries.byRelationOid(client, 100);
      expect(result).toHaveLength(2);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("c.conrelid = ANY($1)"), [
        [100],
      ]);
    });

    it("gets foreign keys by referenced relation OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey3],
      });

      const result = await foreignKeyQueries.byReferencedRelationOid(client, 200);
      expect(result).toHaveLength(2);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("c.confrelid = ANY($1)"), [
        [200],
      ]);
    });
  });

  describe("DataLoaders", () => {
    it("loads foreign key by OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1],
      });

      const { foreignKeyLoader } = createForeignKeyLoaders(client);
      const result = await foreignKeyLoader.load(1);

      expect(result).not.toBeNull();
      expect(result).toEqual(mockForeignKey1);
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining("c.oid = ANY($1)"),
        expect.anything()
      );
    });

    it("loads multiple foreign keys by OID in a single batch", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey2],
      });

      const { foreignKeyLoader } = createForeignKeyLoaders(client);
      const results = await Promise.all([foreignKeyLoader.load(1), foreignKeyLoader.load(2)]);

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual(mockForeignKey1);
      expect(results[1]).toEqual(mockForeignKey2);
      // Should be called only once for both loads
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("c.oid = ANY($1)"), [[1, 2]]);
    });

    it("loads foreign keys by relation OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey2],
      });

      const { foreignKeysByRelationLoader } = createForeignKeyLoaders(client);
      const result = await foreignKeysByRelationLoader.load(100);

      expect(result).toHaveLength(2);
      expect(result).toContainEqual(mockForeignKey1);
      expect(result).toContainEqual(mockForeignKey2);
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining("c.conrelid = ANY($1)"),
        expect.anything()
      );
    });

    it("returns empty array when no foreign keys found for relation", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [],
      });

      const { foreignKeysByRelationLoader } = createForeignKeyLoaders(client);
      const result = await foreignKeysByRelationLoader.load(999);

      expect(result).toEqual([]);
    });

    it("loads foreign keys by referenced relation OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey3],
      });

      const { foreignKeysByReferencedRelationLoader } = createForeignKeyLoaders(client);
      const result = await foreignKeysByReferencedRelationLoader.load(200);

      expect(result).toHaveLength(2);
      expect(result).toContainEqual(mockForeignKey1);
      expect(result).toContainEqual(mockForeignKey3);
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining("c.confrelid = ANY($1)"),
        expect.anything()
      );
    });

    it("gets all foreign keys", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey2, mockForeignKey3],
      });

      const { getAllForeignKeys } = createForeignKeyLoaders(client);
      const result = await getAllForeignKeys();

      expect(result).toHaveLength(3);
      expect(result).toContainEqual(mockForeignKey1);
      expect(result).toContainEqual(mockForeignKey2);
      expect(result).toContainEqual(mockForeignKey3);
    });

    it("filters foreign keys", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockForeignKey1, mockForeignKey2, mockForeignKey3],
      });

      const { getAllForeignKeys } = createForeignKeyLoaders(client);
      const result = await getAllForeignKeys((fk: PgForeignKey) => fk.conrelid === 100);

      expect(result).toHaveLength(2);
      expect(result).toContainEqual(mockForeignKey1);
      expect(result).toContainEqual(mockForeignKey2);
      expect(result).not.toContainEqual(mockForeignKey3);
    });
  });
});
