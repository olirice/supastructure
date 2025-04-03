import { Client } from "pg";
import { createAttributeLoaders, attributeQueries } from "../../src/loaders/pg_attributes.js";
import { PgAttribute, PgAttributeSchema } from "../../src/types.js";

// Mock the schema parser
jest.mock("../../src/types.js", () => {
  const actual = jest.requireActual("../../src/types.js");
  return {
    ...actual,
    PgAttributeSchema: {
      parse: jest.fn((data) => data),
    },
  };
});

// Type for mock row with schema name
type MockRowWithSchema = PgAttribute & { nspname: string };

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

describe("pg_attributes loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Sample mock attributes for testing
  const mockAttribute1: PgAttribute = {
    attrelid: 12345,
    attname: "id",
    atttypid: 23, // int4
    attnum: 1,
    attnotnull: true,
  };

  const mockAttribute2: PgAttribute = {
    attrelid: 12345,
    attname: "name",
    atttypid: 25, // text
    attnum: 2,
    attnotnull: false,
  };

  const mockAttribute3: PgAttribute = {
    attrelid: 67890,
    attname: "description",
    atttypid: 25, // text
    attnum: 1,
    attnotnull: false,
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("attributeQueries.query", () => {
    it("should query attributes with no filters", async () => {
      const mockRows = [
        { ...mockAttribute1, nspname: "public" },
        { ...mockAttribute2, nspname: "public" },
        { ...mockAttribute3, nspname: "public" },
      ] as MockRowWithSchema[];

      mockQuery.mockResolvedValueOnce({ rows: mockRows });

      const result = await attributeQueries.query(client);

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("WHERE");
      expect(mockQuery.mock.calls[0][0]).toContain("a.attnum > 0");
      expect(mockQuery.mock.calls[0][0]).toContain("NOT a.attisdropped");
      expect(mockQuery.mock.calls[0][0]).toContain("n.nspname NOT IN");
      expect(result).toHaveLength(3);
      expect(result).toEqual([mockAttribute1, mockAttribute2, mockAttribute3]);
    });

    it("should filter attributes by relation OIDs", async () => {
      const mockRows = [
        { ...mockAttribute1, nspname: "public" },
        { ...mockAttribute2, nspname: "public" },
      ] as MockRowWithSchema[];

      mockQuery.mockResolvedValueOnce({ rows: mockRows });

      const result = await attributeQueries.query(client, { relationOids: [12345] });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("a.attrelid = ANY($1)");
      expect(mockQuery.mock.calls[0][1]).toEqual([[12345]]);
      expect(result).toHaveLength(2);
      expect(result[0].attrelid).toBe(12345);
      expect(result[1].attrelid).toBe(12345);
    });

    it("should filter attributes by table names", async () => {
      const mockRows = [
        { ...mockAttribute1, nspname: "public" },
        { ...mockAttribute2, nspname: "public" },
      ] as MockRowWithSchema[];

      mockQuery.mockResolvedValueOnce({ rows: mockRows });

      const result = await attributeQueries.query(client, {
        tableNames: [{ schemaName: "public", tableName: "users" }],
      });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("n.nspname = $1 AND c.relname = $2");
      expect(mockQuery.mock.calls[0][1]).toEqual(["public", "users"]);
      expect(result).toHaveLength(2);
    });

    it("should filter attributes by column names", async () => {
      const mockRows = [{ ...mockAttribute1, nspname: "public" }] as MockRowWithSchema[];

      mockQuery.mockResolvedValueOnce({ rows: mockRows });

      const result = await attributeQueries.query(client, { columnNames: ["id"] });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("a.attname = ANY($1)");
      expect(mockQuery.mock.calls[0][1]).toEqual([["id"]]);
      expect(result).toHaveLength(1);
      expect(result[0].attname).toBe("id");
    });

    it("should include system schemas when skipSystemSchemas is false", async () => {
      const mockRows = [{ ...mockAttribute1, nspname: "pg_catalog" }] as MockRowWithSchema[];

      mockQuery.mockResolvedValueOnce({ rows: mockRows });

      await attributeQueries.query(client, { skipSystemSchemas: false });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).not.toContain("n.nspname NOT IN");
    });
  });

  describe("attributeQueries.queryByTableName", () => {
    it("should query attributes by schema and table name", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockAttribute1, mockAttribute2],
      });

      const result = await attributeQueries.queryByTableName(client, "public", "users");

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][1]).toEqual(["public", "users"]);
      expect(result).toHaveLength(2);
      expect(PgAttributeSchema.parse).toHaveBeenCalled();
    });
  });

  describe("createAttributeLoaders", () => {
    let loaders: ReturnType<typeof createAttributeLoaders>;

    beforeEach(() => {
      loaders = createAttributeLoaders(client);
    });

    describe("attributesByRelationLoader", () => {
      it("should load attributes by relation OID", async () => {
        const mockRows = [
          { ...mockAttribute1, nspname: "public" },
          { ...mockAttribute2, nspname: "public" },
        ] as MockRowWithSchema[];

        mockQuery.mockResolvedValueOnce({ rows: mockRows });

        const result = await loaders.attributesByRelationLoader.load(12345);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery.mock.calls[0][0]).toContain("a.attrelid = ANY($1)");
        expect(result).toHaveLength(2);
        expect(result![0].attrelid).toBe(12345);
        expect(result![1].attrelid).toBe(12345);
      });

      it("should batch load attributes by relation OID", async () => {
        const mockRows = [
          { ...mockAttribute1, nspname: "public" },
          { ...mockAttribute2, nspname: "public" },
          { ...mockAttribute3, nspname: "public" },
        ] as MockRowWithSchema[];

        mockQuery.mockResolvedValueOnce({ rows: mockRows });

        const [result1, result2] = await Promise.all([
          loaders.attributesByRelationLoader.load(12345),
          loaders.attributesByRelationLoader.load(67890),
        ]);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery.mock.calls[0][1]).toEqual([[12345, 67890]]);
        expect(result1).toHaveLength(2);
        expect(result2).toHaveLength(1);
        expect(result1![0].attrelid).toBe(12345);
        expect(result2![0].attrelid).toBe(67890);
      });

      it("should return null for non-existent relation OID", async () => {
        const mockRows = [
          { ...mockAttribute1, nspname: "public" },
          { ...mockAttribute2, nspname: "public" },
        ] as MockRowWithSchema[];

        mockQuery.mockResolvedValueOnce({ rows: mockRows });

        const [result1, result2] = await Promise.all([
          loaders.attributesByRelationLoader.load(12345),
          loaders.attributesByRelationLoader.load(99999),
        ]);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result1).not.toBeNull();
        expect(result2).toBeNull();
      });
    });

    describe("attributesByTableNameLoader", () => {
      it("should load attributes by table name and schema", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockAttribute1, mockAttribute2],
        });

        const result = await loaders.attributesByTableNameLoader.load({
          schemaName: "public",
          tableName: "users",
        });

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery.mock.calls[0][1]).toEqual(["public", "users"]);
        expect(result).toHaveLength(2);
      });

      it("should return null for non-existent table name", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [],
        });

        const result = await loaders.attributesByTableNameLoader.load({
          schemaName: "public",
          tableName: "nonexistent",
        });

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result).toBeNull();
      });
    });

    describe("getAllAttributes", () => {
      it("should fetch all attributes", async () => {
        const mockRows = [
          { ...mockAttribute1, nspname: "public" },
          { ...mockAttribute2, nspname: "public" },
          { ...mockAttribute3, nspname: "public" },
        ] as MockRowWithSchema[];

        mockQuery.mockResolvedValueOnce({ rows: mockRows });

        const result = await loaders.getAllAttributes();

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result).toHaveLength(3);
      });

      it("should filter attributes with the provided function", async () => {
        const mockRows = [
          { ...mockAttribute1, nspname: "public" },
          { ...mockAttribute2, nspname: "public" },
          { ...mockAttribute3, nspname: "public" },
        ] as MockRowWithSchema[];

        mockQuery.mockResolvedValueOnce({ rows: mockRows });

        const result = await loaders.getAllAttributes((attr) => attr.attrelid === 12345);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result).toHaveLength(2);
        expect(result[0].attrelid).toBe(12345);
        expect(result[1].attrelid).toBe(12345);
      });
    });
  });
});
