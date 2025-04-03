import { Client } from "pg";
import { createNamespaceLoaders, namespaceQueries } from "../../src/loaders/pg_namespaces.js";
import { PgNamespace, PgNamespaceSchema } from "../../src/types.js";

// Mock the PgNamespaceSchema.parse function
jest.mock("../../src/types.js", () => {
  const actual = jest.requireActual("../../src/types.js");
  return {
    ...actual,
    PgNamespaceSchema: {
      ...actual.PgNamespaceSchema,
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

describe("pg_namespaces loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Mock namespace objects to use in tests
  const mockNamespace1: PgNamespace = {
    oid: 2200,
    nspname: "public",
    nspowner: 10,
  };

  const mockNamespace2: PgNamespace = {
    oid: 3000,
    nspname: "custom_schema",
    nspowner: 10,
  };

  const mockNamespace3: PgNamespace = {
    oid: 11,
    nspname: "pg_catalog",
    nspowner: 10,
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("namespaceQueries", () => {
    describe("query", () => {
      it("queries namespaces with oids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1],
        });

        const result = await namespaceQueries.query(client, { oids: [2200] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockNamespace1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND oid = ANY($1)"), [
          [2200],
        ]);
        expect(PgNamespaceSchema.parse).toHaveBeenCalledWith(mockNamespace1);
      });

      it("queries namespaces with names filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace2],
        });

        const result = await namespaceQueries.query(client, { names: ["custom_schema"] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockNamespace2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND nspname = ANY($1)"), [
          ["custom_schema"],
        ]);
      });

      it("excludes system schemas when all option is true", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1, mockNamespace2],
        });

        const result = await namespaceQueries.query(client, { all: true });

        expect(result).toHaveLength(2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining(
            "AND nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')"
          ),
          []
        );
      });

      it("combines multiple filter conditions", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1],
        });

        const result = await namespaceQueries.query(client, {
          oids: [2200],
          names: ["public"],
        });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockNamespace1);

        const query = mockQuery.mock.calls[0][0];
        expect(query).toContain("AND oid = ANY($1)");
        expect(query).toContain("AND nspname = ANY($2)");
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE"), [
          [2200],
          ["public"],
        ]);
      });
    });
  });

  describe("DataLoaders", () => {
    describe("namespaceLoader", () => {
      it("loads a single namespace by OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1],
        });

        const { namespaceLoader } = createNamespaceLoaders(client);
        const result = await namespaceLoader.load(2200);

        expect(result).not.toBeNull();
        expect(result).toEqual(mockNamespace1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND oid = ANY($1)"), [
          [2200],
        ]);
      });

      it("loads multiple namespaces by OID in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1, mockNamespace2],
        });

        const { namespaceLoader } = createNamespaceLoaders(client);
        const results = await Promise.all([namespaceLoader.load(2200), namespaceLoader.load(3000)]);

        expect(results).toHaveLength(2);
        expect(results[0]).toEqual(mockNamespace1);
        expect(results[1]).toEqual(mockNamespace2);
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND oid = ANY($1)"), [
          [2200, 3000],
        ]);
      });

      it("returns null for non-existent namespaces", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1],
        });

        const { namespaceLoader } = createNamespaceLoaders(client);
        const results = await Promise.all([namespaceLoader.load(2200), namespaceLoader.load(9999)]);

        expect(results[0]).toEqual(mockNamespace1);
        expect(results[1]).toBeNull();
      });
    });

    describe("namespaceByNameLoader", () => {
      it("loads a single namespace by name", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace2],
        });

        const { namespaceByNameLoader } = createNamespaceLoaders(client);
        const result = await namespaceByNameLoader.load("custom_schema");

        expect(result).not.toBeNull();
        expect(result).toEqual(mockNamespace2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND nspname = ANY($1)"), [
          ["custom_schema"],
        ]);
      });

      it("loads multiple namespaces by name in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1, mockNamespace2],
        });

        const { namespaceByNameLoader } = createNamespaceLoaders(client);
        const results = await Promise.all([
          namespaceByNameLoader.load("public"),
          namespaceByNameLoader.load("custom_schema"),
        ]);

        expect(results).toHaveLength(2);
        expect(results[0]).toEqual(mockNamespace1);
        expect(results[1]).toEqual(mockNamespace2);
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND nspname = ANY($1)"), [
          ["public", "custom_schema"],
        ]);
      });

      it("returns null for non-existent namespace names", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1],
        });

        const { namespaceByNameLoader } = createNamespaceLoaders(client);
        const results = await Promise.all([
          namespaceByNameLoader.load("public"),
          namespaceByNameLoader.load("nonexistent"),
        ]);

        expect(results[0]).toEqual(mockNamespace1);
        expect(results[1]).toBeNull();
      });
    });

    describe("getAllNamespaces", () => {
      it("returns all namespaces (excluding system schemas)", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1, mockNamespace2],
        });

        const { getAllNamespaces } = createNamespaceLoaders(client);
        const result = await getAllNamespaces();

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockNamespace1);
        expect(result[1]).toEqual(mockNamespace2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE"), []);
      });

      it("filters namespaces with custom filter function", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockNamespace1, mockNamespace2],
        });

        const { getAllNamespaces } = createNamespaceLoaders(client);
        const result = await getAllNamespaces((ns) => ns.nspname === "public");

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockNamespace1);
      });
    });
  });
});
