import { Client } from "pg";
import { createTypeLoaders, typeQueries } from "../../src/loaders/pg_types.js";
import { PgType, PgTypeSchema } from "../../src/types.js";

// Mock the schema parser
jest.mock("../../src/types.js", () => {
  const actual = jest.requireActual("../../src/types.js");
  return {
    ...actual,
    PgTypeSchema: {
      parse: jest.fn((data) => data),
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

describe("pg_types loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Sample mock types for testing
  const mockType1: PgType & { nspname?: string } = {
    oid: 12345,
    typname: "int4",
    typtype: "b", // base type
    typbasetype: 0,
    typelem: 0,
    typrelid: 0,
    typnamespace: 11,
    nspname: "pg_catalog",
  };

  const mockType2: PgType & { nspname?: string } = {
    oid: 67890,
    typname: "text",
    typtype: "b", // base type
    typbasetype: 0,
    typelem: 0,
    typrelid: 0,
    typnamespace: 11,
    nspname: "pg_catalog",
  };

  const mockType3: PgType & { nspname?: string } = {
    oid: 54321,
    typname: "user_type",
    typtype: "c", // composite type
    typbasetype: 0,
    typelem: 0,
    typrelid: 1234,
    typnamespace: 2200,
    nspname: "public",
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("typeQueries.query", () => {
    it("should query types with no filters", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType1, mockType2, mockType3],
      });

      const result = await typeQueries.query(client);

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("WHERE 1=1");
      expect(mockQuery.mock.calls[0][0]).toContain("n.nspname NOT IN");
      expect(result).toHaveLength(3);
      expect(PgTypeSchema.parse).toHaveBeenCalledTimes(3);
    });

    it("should filter types by OIDs", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType1],
      });

      const result = await typeQueries.query(client, { oids: [12345] });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("t.oid = ANY($1)");
      expect(mockQuery.mock.calls[0][1]).toEqual([[12345]]);
      expect(result).toHaveLength(1);
      expect(result[0].oid).toBe(12345);
    });

    it("should filter types by type names", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType1],
      });

      const result = await typeQueries.query(client, { typeNames: ["int4"] });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("t.typname = ANY($1)");
      expect(mockQuery.mock.calls[0][1]).toEqual([["int4"]]);
      expect(result).toHaveLength(1);
      expect(result[0].typname).toBe("int4");
    });

    it("should filter types by schema names", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType3],
      });

      const result = await typeQueries.query(client, { schemaNames: ["public"] });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("n.nspname = ANY($1)");
      expect(mockQuery.mock.calls[0][1]).toEqual([["public"]]);
      expect(result).toHaveLength(1);
      expect(result[0].typname).toBe("user_type");
    });

    it("should filter types by type kinds", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType3],
      });

      const result = await typeQueries.query(client, { typeKinds: ["c"] });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("t.typtype = ANY($1)");
      expect(mockQuery.mock.calls[0][1]).toEqual([["c"]]);
      expect(result).toHaveLength(1);
      expect(result[0].typtype).toBe("c");
    });

    it("should include system schemas when all is true", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType1, mockType2],
      });

      await typeQueries.query(client, { all: true });

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).not.toContain("n.nspname NOT IN");
    });
  });

  describe("typeQueries.byOid", () => {
    it("should return a type by OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType1],
      });

      const result = await typeQueries.byOid(client, 12345);

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("t.oid = ANY($1)");
      expect(mockQuery.mock.calls[0][1]).toEqual([[12345]]);
      expect(result).toEqual(mockType1);
    });

    it("should return null for non-existent OID", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [],
      });

      const result = await typeQueries.byOid(client, 99999);

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(result).toBeNull();
    });
  });

  describe("typeQueries.byNameAndSchema", () => {
    it("should return a type by name and schema", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [mockType3],
      });

      const result = await typeQueries.byNameAndSchema(client, "public", "user_type");

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery.mock.calls[0][0]).toContain("t.typname = ANY($1)");
      expect(mockQuery.mock.calls[0][0]).toContain("n.nspname = ANY($2)");
      expect(mockQuery.mock.calls[0][1]).toEqual([["user_type"], ["public"]]);
      expect(result).toEqual(mockType3);
    });

    it("should return null for non-existent type", async () => {
      mockQuery.mockResolvedValueOnce({
        rows: [],
      });

      const result = await typeQueries.byNameAndSchema(client, "public", "nonexistent");

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(result).toBeNull();
    });
  });

  describe("createTypeLoaders", () => {
    let loaders: ReturnType<typeof createTypeLoaders>;

    beforeEach(() => {
      loaders = createTypeLoaders(client);
    });

    describe("typeLoader", () => {
      it("should load a type by OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType1],
        });

        const result = await loaders.typeLoader.load(12345);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery.mock.calls[0][0]).toContain("t.oid = ANY($1)");
        expect(result).toEqual(mockType1);
      });

      it("should batch load types by OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType1, mockType2],
        });

        const [result1, result2] = await Promise.all([
          loaders.typeLoader.load(12345),
          loaders.typeLoader.load(67890),
        ]);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery.mock.calls[0][1]).toEqual([[12345, 67890]]);
        expect(result1).toEqual(mockType1);
        expect(result2).toEqual(mockType2);
      });

      it("should return null for non-existent OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType1],
        });

        const [result1, result2] = await Promise.all([
          loaders.typeLoader.load(12345),
          loaders.typeLoader.load(99999),
        ]);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result1).not.toBeNull();
        expect(result2).toBeNull();
      });
    });

    describe("typeByNameLoader", () => {
      it("should load a type by schema name and type name", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType3],
        });

        const result = await loaders.typeByNameLoader.load({
          schemaName: "public",
          typeName: "user_type",
        });

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery.mock.calls[0][1]).toEqual([["user_type"], ["public"]]);
        expect(result).toEqual(mockType3);
      });

      it("should batch load types by schema and name", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType1, mockType3],
        });

        const [result1, result2] = await Promise.all([
          loaders.typeByNameLoader.load({
            schemaName: "pg_catalog",
            typeName: "int4",
          }),
          loaders.typeByNameLoader.load({
            schemaName: "public",
            typeName: "user_type",
          }),
        ]);

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery.mock.calls[0][1]).toEqual([
          ["int4", "user_type"],
          ["pg_catalog", "public"],
        ]);
        expect(result1).toEqual(mockType1);
        expect(result2).toEqual(mockType3);
      });

      it("should return null for non-existent type", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType1],
        });

        const result = await loaders.typeByNameLoader.load({
          schemaName: "public",
          typeName: "nonexistent",
        });

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result).toBeNull();
      });
    });

    describe("getAllTypes", () => {
      it("should fetch all types", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType1, mockType2, mockType3],
        });

        const result = await loaders.getAllTypes();

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result).toHaveLength(3);
      });

      it("should filter types with the provided function", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockType1, mockType2, mockType3],
        });

        const result = await loaders.getAllTypes((type) => type.typtype === "c");

        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(result).toHaveLength(1);
        expect(result[0].typtype).toBe("c");
      });
    });
  });
});
