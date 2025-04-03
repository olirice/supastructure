import { Client } from "pg";
import { createClassLoaders, classQueries } from "../../src/loaders/pg_classes.js";
import { PgClass, PgClassSchema } from "../../src/types.js";

// Mock the PgClassSchema.parse function
jest.mock("../../src/types.js", () => {
  const actual = jest.requireActual("../../src/types.js");
  return {
    ...actual,
    PgClassSchema: {
      ...actual.PgClassSchema,
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

describe("pg_classes loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Interface matching the internal PgClassWithSchema from the loader
  interface PgClassWithSchema extends PgClass {
    nspname: string;
  }

  // Mock class objects to use in tests
  const mockTable1: PgClassWithSchema = {
    oid: 16384,
    relname: "users",
    relnamespace: 2200,
    relkind: "r", // r = regular table
    relispopulated: true,
    relrowsecurity: true,
    nspname: "public",
  };

  const mockTable2: PgClassWithSchema = {
    oid: 16385,
    relname: "products",
    relnamespace: 2200,
    relkind: "r",
    relispopulated: true,
    relrowsecurity: false,
    nspname: "public",
  };

  const mockView: PgClassWithSchema = {
    oid: 16386,
    relname: "user_view",
    relnamespace: 16383,
    relkind: "v", // v = view
    relispopulated: true,
    relrowsecurity: false,
    nspname: "app",
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("classQueries", () => {
    describe("query", () => {
      it("queries classes with oids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1],
        });

        const result = await classQueries.query(client, { oids: [16384] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockTable1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND c.oid = ANY($1)"), [
          [16384],
        ]);
        expect(PgClassSchema.parse).toHaveBeenCalled();
      });

      it("queries classes with namespaceOids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2],
        });

        const result = await classQueries.query(client, { namespaceOids: [2200] });

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockTable1);
        expect(result[1]).toEqual(mockTable2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND c.relnamespace = ANY($1)"),
          [[2200]]
        );
      });

      it("queries classes with relkinds filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2],
        });

        const result = await classQueries.query(client, { relkinds: ["r"] });

        expect(result).toHaveLength(2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND c.relkind = ANY($1)"), [
          ["r"],
        ]);
      });

      it("queries classes with names filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1],
        });

        const result = await classQueries.query(client, {
          names: [{ schema: "public", name: "users" }],
        });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockTable1);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND (n.nspname, c.relname) IN (($1, $2))"),
          ["public", "users"]
        );
      });

      it("queries classes with multiple names filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2],
        });

        const result = await classQueries.query(client, {
          names: [
            { schema: "public", name: "users" },
            { schema: "public", name: "products" },
          ],
        });

        expect(result).toHaveLength(2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND (n.nspname, c.relname) IN (($1, $2), ($3, $4))"),
          ["public", "users", "public", "products"]
        );
      });

      it("excludes system schemas when all option is true", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2, mockView],
        });

        const result = await classQueries.query(client, { all: true });

        expect(result).toHaveLength(3);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining(
            "AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')"
          ),
          []
        );
      });

      it("combines multiple filter conditions", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1],
        });

        const result = await classQueries.query(client, {
          namespaceOids: [2200],
          relkinds: ["r"],
        });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockTable1);

        const query = mockQuery.mock.calls[0][0];
        expect(query).toContain("AND c.relnamespace = ANY($1)");
        expect(query).toContain("AND c.relkind = ANY($2)");
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringMatching(
            /AND c\.relnamespace = ANY\(\$1\)[\s\S]*AND c\.relkind = ANY\(\$2\)/
          ),
          [[2200], ["r"]]
        );
      });
    });
  });

  describe("DataLoaders", () => {
    describe("classLoader", () => {
      it("loads a single class by OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1],
        });

        const { classLoader } = createClassLoaders(client);
        const result = await classLoader.load(16384);

        expect(result).not.toBeNull();
        // nspname should be removed to match PgClass type
        const { nspname, ...expectedClass } = mockTable1;
        expect(result).toEqual(expectedClass);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND c.oid = ANY($1)"), [
          [16384],
        ]);
      });

      it("loads multiple classes by OID in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockView],
        });

        const { classLoader } = createClassLoaders(client);
        const results = await Promise.all([classLoader.load(16384), classLoader.load(16386)]);

        expect(results).toHaveLength(2);

        // nspname should be removed to match PgClass type
        const { nspname: nspname1, ...expectedClass1 } = mockTable1;
        const { nspname: nspname2, ...expectedClass2 } = mockView;

        expect(results[0]).toEqual(expectedClass1);
        expect(results[1]).toEqual(expectedClass2);
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND c.oid = ANY($1)"), [
          [16384, 16386],
        ]);
      });

      it("returns null for non-existent classes", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1],
        });

        const { classLoader } = createClassLoaders(client);
        const results = await Promise.all([classLoader.load(16384), classLoader.load(99999)]);

        const { nspname, ...expectedClass } = mockTable1;
        expect(results[0]).toEqual(expectedClass);
        expect(results[1]).toBeNull();
      });
    });

    describe("classByNameLoader", () => {
      it("loads a single class by schema and name", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1],
        });

        const { classByNameLoader } = createClassLoaders(client);
        const result = await classByNameLoader.load({ schema: "public", name: "users" });

        expect(result).not.toBeNull();
        // nspname should be removed to match PgClass type
        const { nspname, ...expectedClass } = mockTable1;
        expect(result).toEqual(expectedClass);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND (n.nspname, c.relname) IN (($1, $2))"),
          ["public", "users"]
        );
      });

      it("loads multiple classes by schema and name in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2],
        });

        const { classByNameLoader } = createClassLoaders(client);
        const results = await Promise.all([
          classByNameLoader.load({ schema: "public", name: "users" }),
          classByNameLoader.load({ schema: "public", name: "products" }),
        ]);

        expect(results).toHaveLength(2);

        // nspname should be removed to match PgClass type
        const { nspname: nspname1, ...expectedClass1 } = mockTable1;
        const { nspname: nspname2, ...expectedClass2 } = mockTable2;

        expect(results[0]).toEqual(expectedClass1);
        expect(results[1]).toEqual(expectedClass2);
        expect(mockQuery).toHaveBeenCalledTimes(1);
      });

      it("returns null for non-existent classes", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1],
        });

        const { classByNameLoader } = createClassLoaders(client);
        const results = await Promise.all([
          classByNameLoader.load({ schema: "public", name: "users" }),
          classByNameLoader.load({ schema: "public", name: "nonexistent" }),
        ]);

        const { nspname, ...expectedClass } = mockTable1;
        expect(results[0]).toEqual(expectedClass);
        expect(results[1]).toBeNull();
      });
    });

    describe("classesByNamespaceLoader", () => {
      it("loads classes by namespace OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2],
        });

        const { classesByNamespaceLoader } = createClassLoaders(client);
        const result = await classesByNamespaceLoader.load({ namespaceOid: 2200 });

        expect(result).toHaveLength(2);
        // nspname should be removed to match PgClass type
        const { nspname: nspname1, ...expectedClass1 } = mockTable1;
        const { nspname: nspname2, ...expectedClass2 } = mockTable2;

        expect(result[0]).toEqual(expectedClass1);
        expect(result[1]).toEqual(expectedClass2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND c.relnamespace = ANY($1)"),
          [[2200]]
        );
      });

      it("loads classes by namespace OID and relkind", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2],
        });

        const { classesByNamespaceLoader } = createClassLoaders(client);
        const result = await classesByNamespaceLoader.load({
          namespaceOid: 2200,
          relkind: "r",
        });

        expect(result).toHaveLength(2);
        // nspname should be removed to match PgClass type
        const { nspname: nspname1, ...expectedClass1 } = mockTable1;
        const { nspname: nspname2, ...expectedClass2 } = mockTable2;

        expect(result[0]).toEqual(expectedClass1);
        expect(result[1]).toEqual(expectedClass2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND c.relnamespace = ANY($1) AND c.relkind = ANY($2)"),
          [[2200], ["r"]]
        );
      });

      it("loads classes for multiple namespaces in separate queries", async () => {
        // First query for namespace 2200
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2],
        });

        // Second query for namespace 16383
        mockQuery.mockResolvedValueOnce({
          rows: [mockView],
        });

        const { classesByNamespaceLoader } = createClassLoaders(client);
        const results = await Promise.all([
          classesByNamespaceLoader.load({ namespaceOid: 2200 }),
          classesByNamespaceLoader.load({ namespaceOid: 16383 }),
        ]);

        expect(results).toHaveLength(2);
        expect(results[0]).toHaveLength(2); // Two tables in public schema
        expect(results[1]).toHaveLength(1); // One view in app schema

        // Should make separate queries for each namespace
        expect(mockQuery).toHaveBeenCalledTimes(2);
      });
    });

    describe("getAllClasses", () => {
      it("returns all classes (excluding system schemas)", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2, mockView],
        });

        const { getAllClasses } = createClassLoaders(client);
        const result = await getAllClasses();

        expect(result).toHaveLength(3);

        // nspname should be removed to match PgClass type
        const { nspname: nspname1, ...expectedClass1 } = mockTable1;
        const { nspname: nspname2, ...expectedClass2 } = mockTable2;
        const { nspname: nspname3, ...expectedClass3 } = mockView;

        expect(result[0]).toEqual(expectedClass1);
        expect(result[1]).toEqual(expectedClass2);
        expect(result[2]).toEqual(expectedClass3);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE"), []);
      });

      it("filters classes with custom filter function", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTable1, mockTable2, mockView],
        });

        const { getAllClasses } = createClassLoaders(client);
        const result = await getAllClasses((cls) => cls.relkind === "v");

        expect(result).toHaveLength(1);

        // nspname should be removed to match PgClass type
        const { nspname, ...expectedClass } = mockView;

        expect(result[0]).toEqual(expectedClass);
      });
    });
  });
});
