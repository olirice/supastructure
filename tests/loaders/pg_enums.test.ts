import { Client } from "pg";
import { createEnumLoaders, enumQueries } from "../../src/loaders/pg_enums.js";
import { PgEnum, PgEnumSchema } from "../../src/types.js";

// Mock the PgEnumSchema.parse function to return the expected structure
jest.mock("../../src/types.js", () => {
  const actual = jest.requireActual("../../src/types.js");
  return {
    ...actual,
    PgEnumSchema: {
      ...actual.PgEnumSchema,
      parse: jest.fn((data) => data), // Simply return the data as-is for tests
    },
  };
});

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

describe("pg_enums loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  beforeEach(() => {
    client = new Client();
    mockQuery = (client.query as jest.Mock);
    mockQuery.mockReset();
  });

  describe("enumQueries", () => {
    it("queries enums with default options", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const result = await enumQueries.query(client);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        enumtypid: 1,
        enumname: "test_enum",
        schemaname: "public",
        enumlabels: ["a", "b", "c"],
      });
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("SELECT"), []);
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining("AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')"),
        []
      );
    });

    it("queries enums with type ID filter", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const result = await enumQueries.query(client, { enumTypeIds: [1] });

      expect(result).toHaveLength(1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND e.enumtypid = ANY($1)"), [[1]]);
    });

    it("queries enums with schema name filter", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const result = await enumQueries.query(client, { schemaNames: ["public"] });

      expect(result).toHaveLength(1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND n.nspname = ANY($1)"), [["public"]]);
    });

    it("queries enums with enum name filter", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const result = await enumQueries.query(client, { enumNames: ["test_enum"] });

      expect(result).toHaveLength(1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.typname = ANY($1)"), [["test_enum"]]);
    });

    it("includes system schemas when all=true", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "system_enum",
            schemaname: "pg_catalog",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const result = await enumQueries.query(client, { all: true });

      expect(result).toHaveLength(1);
      expect(mockQuery).not.toHaveBeenCalledWith(
        expect.stringContaining("AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')"),
        []
      );
    });

    it("parses string enumlabels in PostgreSQL array format", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: "{a,b,c}",
          },
        ],
      });

      const result = await enumQueries.query(client);

      expect(result).toHaveLength(1);
      expect(result[0].enumlabels).toEqual(["a", "b", "c"]);
    });

    it("handles a single string as enumlabels", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: "single_value",
          },
        ],
      });

      const result = await enumQueries.query(client);

      expect(result).toHaveLength(1);
      expect(result[0].enumlabels).toEqual(["single_value"]);
    });

    it("handles non-array and non-string enumlabels as empty array", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: null,
          },
        ],
      });

      const result = await enumQueries.query(client);

      expect(result).toHaveLength(1);
      expect(result[0].enumlabels).toEqual([]);
    });

    it("handles parsing errors gracefully", async () => {
      // Create a contrived scenario that would cause an error in the parsing logic
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: "{a,b,c", // Malformed - missing closing brace
          },
        ],
      });

      const result = await enumQueries.query(client);

      expect(result).toHaveLength(1);
      // Should fall back to treating the entire string as a single label
      expect(result[0].enumlabels).toEqual(["{a,b,c"]);
    });

    it("gets enum by type ID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const result = await enumQueries.byTypeId(client, 1);
      expect(result).not.toBeNull();
      expect(result?.enumtypid).toBe(1);
    });

    it("returns null when enum not found by type ID", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [] });

      const result = await enumQueries.byTypeId(client, 999);
      expect(result).toBeNull();
    });
  });

  describe("DataLoaders", () => {
    it("loads enums by type ID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const { enumByTypeIdLoader } = createEnumLoaders(client);
      const result = await enumByTypeIdLoader.load(1);

      expect(result).not.toBeNull();
      expect(result?.enumtypid).toBe(1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND e.enumtypid = ANY($1)"), expect.anything());
    });

    it("loads enums by name", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const { enumByNameLoader } = createEnumLoaders(client);
      const result = await enumByNameLoader.load({ schemaName: "public", enumName: "test_enum" });

      expect(result).not.toBeNull();
      expect(result?.enumtypid).toBe(1);
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND n.nspname = ANY($1)"), expect.anything());
    });

    it("loads enums by name with proper field mapping", async () => {
      // This time the mock has the right field names for the map lookup
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
        ],
      });

      const { enumByNameLoader } = createEnumLoaders(client);
      const result = await enumByNameLoader.load({ schemaName: "public", enumName: "test_enum" });

      expect(result).not.toBeNull();
      expect(result?.enumtypid).toBe(1);
    });

    it("gets all enums", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum1",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
          {
            enumtypid: 2,
            enumname: "test_enum2",
            schemaname: "public",
            enumlabels: ["d", "e", "f"],
          },
        ],
      });

      const { getAllEnums } = createEnumLoaders(client);
      const result = await getAllEnums();

      expect(result).toHaveLength(2);
      expect(result[0].enumtypid).toBe(1);
      expect(result[1].enumtypid).toBe(2);
    });

    it("filters enums", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [
          {
            enumtypid: 1,
            enumname: "test_enum1",
            schemaname: "public",
            enumlabels: ["a", "b", "c"],
          },
          {
            enumtypid: 2,
            enumname: "test_enum2",
            schemaname: "public",
            enumlabels: ["d", "e", "f"],
          },
        ],
      });

      const { getAllEnums } = createEnumLoaders(client);
      const result = await getAllEnums((enum_: PgEnum) => enum_.enumtypid === 1);

      expect(result).toHaveLength(1);
      expect(result[0].enumtypid).toBe(1);
    });
  });
}); 