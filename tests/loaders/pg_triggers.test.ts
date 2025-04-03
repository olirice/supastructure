import { Client } from "pg";
import { createTriggerLoaders, triggerQueries } from "../../src/loaders/pg_triggers.js";
import { PgTrigger, PgTriggerSchema } from "../../src/types.js";

// Mock the PgTriggerSchema.parse function
jest.mock("../../src/types.js", () => {
  const actual = jest.requireActual("../../src/types.js");
  return {
    ...actual,
    PgTriggerSchema: {
      ...actual.PgTriggerSchema,
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

describe("pg_triggers loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Mock trigger objects to use in tests
  const mockTrigger1: PgTrigger = {
    oid: 16385,
    tgname: "update_timestamp",
    tgrelid: 16380,
  };

  const mockTrigger2: PgTrigger = {
    oid: 16386,
    tgname: "insert_log",
    tgrelid: 16380,
  };

  const mockTrigger3: PgTrigger = {
    oid: 16387,
    tgname: "delete_log",
    tgrelid: 16381,
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("triggerQueries", () => {
    describe("query", () => {
      it("queries triggers with oids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1],
        });

        const result = await triggerQueries.query(client, { oids: [16385] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockTrigger1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.oid = ANY($1)"), [
          [16385],
        ]);
        expect(PgTriggerSchema.parse).toHaveBeenCalledWith(mockTrigger1);
      });

      it("queries triggers with triggerNames filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1],
        });

        const result = await triggerQueries.query(client, { triggerNames: ["update_timestamp"] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockTrigger1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.tgname = ANY($1)"), [
          ["update_timestamp"],
        ]);
      });

      it("queries triggers with triggerRelids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2],
        });

        const result = await triggerQueries.query(client, { triggerRelids: [16380] });

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockTrigger1);
        expect(result[1]).toEqual(mockTrigger2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.tgrelid = ANY($1)"), [
          [16380],
        ]);
      });

      it("queries triggers with schemaNames filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1],
        });

        const result = await triggerQueries.query(client, { schemaNames: ["public"] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockTrigger1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND n.nspname = ANY($1)"), [
          ["public"],
        ]);
      });

      it("excludes system schemas when all option is true", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2, mockTrigger3],
        });

        const result = await triggerQueries.query(client, { all: true });

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
          rows: [mockTrigger1],
        });

        const result = await triggerQueries.query(client, {
          triggerRelids: [16380],
          schemaNames: ["public"],
        });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockTrigger1);

        const query = mockQuery.mock.calls[0][0];
        expect(query).toContain("AND t.tgrelid = ANY($1)");
        expect(query).toContain("AND n.nspname = ANY($2)");
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE"), [
          [16380],
          ["public"],
        ]);
      });
    });

    describe("byOid", () => {
      it("returns a trigger when found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1],
        });

        const result = await triggerQueries.byOid(client, 16385);

        expect(result).not.toBeNull();
        expect(result).toEqual(mockTrigger1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.oid = ANY($1)"), [
          [16385],
        ]);
      });

      it("returns null when trigger not found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [],
        });

        const result = await triggerQueries.byOid(client, 99999);

        expect(result).toBeNull();
      });
    });

    describe("byRelationOid", () => {
      it("returns triggers for a relation", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2],
        });

        const result = await triggerQueries.byRelationOid(client, 16380);

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockTrigger1);
        expect(result[1]).toEqual(mockTrigger2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.tgrelid = ANY($1)"), [
          [16380],
        ]);
      });
    });

    describe("byNameAndSchema", () => {
      it("returns a trigger when found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1],
        });

        const result = await triggerQueries.byNameAndSchema(client, "public", "update_timestamp");

        expect(result).not.toBeNull();
        expect(result).toEqual(mockTrigger1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE"), [
          ["update_timestamp"],
          ["public"],
        ]);
      });

      it("returns null when trigger not found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [],
        });

        const result = await triggerQueries.byNameAndSchema(client, "public", "nonexistent");

        expect(result).toBeNull();
      });
    });
  });

  describe("DataLoaders", () => {
    describe("triggerLoader", () => {
      it("loads a single trigger by OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1],
        });

        const { triggerLoader } = createTriggerLoaders(client);
        const result = await triggerLoader.load(16385);

        expect(result).not.toBeNull();
        expect(result).toEqual(mockTrigger1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.oid = ANY($1)"), [
          [16385],
        ]);
      });

      it("loads multiple triggers by OID in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger3],
        });

        const { triggerLoader } = createTriggerLoaders(client);
        const results = await Promise.all([triggerLoader.load(16385), triggerLoader.load(16387)]);

        expect(results).toHaveLength(2);
        expect(results[0]).toEqual(mockTrigger1);
        expect(results[1]).toEqual(mockTrigger3);
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.oid = ANY($1)"), [
          [16385, 16387],
        ]);
      });

      it("returns null for non-existent triggers", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1],
        });

        const { triggerLoader } = createTriggerLoaders(client);
        const results = await Promise.all([triggerLoader.load(16385), triggerLoader.load(99999)]);

        expect(results[0]).toEqual(mockTrigger1);
        expect(results[1]).toBeNull();
      });
    });

    describe("triggersByRelationLoader", () => {
      it("loads triggers by relation OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2],
        });

        const { triggersByRelationLoader } = createTriggerLoaders(client);
        const result = await triggersByRelationLoader.load(16380);

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockTrigger1);
        expect(result[1]).toEqual(mockTrigger2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.tgrelid = ANY($1)"), [
          [16380],
        ]);
      });

      it("loads triggers for multiple relations in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2, mockTrigger3],
        });

        const { triggersByRelationLoader } = createTriggerLoaders(client);
        const results = await Promise.all([
          triggersByRelationLoader.load(16380),
          triggersByRelationLoader.load(16381),
        ]);

        expect(results).toHaveLength(2);
        expect(results[0]).toHaveLength(2); // Two triggers for first relation
        expect(results[1]).toHaveLength(1); // One trigger for second relation
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND t.tgrelid = ANY($1)"), [
          [16380, 16381],
        ]);
      });

      it("returns empty array for relations with no triggers", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2],
        });

        const { triggersByRelationLoader } = createTriggerLoaders(client);
        const results = await Promise.all([
          triggersByRelationLoader.load(16380),
          triggersByRelationLoader.load(99999),
        ]);

        expect(results[0]).toHaveLength(2);
        expect(results[1]).toEqual([]);
      });
    });

    describe("getAllTriggers", () => {
      it("returns all triggers (excluding system triggers)", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2, mockTrigger3],
        });

        const { getAllTriggers } = createTriggerLoaders(client);
        const result = await getAllTriggers();

        expect(result).toHaveLength(3);
        expect(result[0]).toEqual(mockTrigger1);
        expect(result[1]).toEqual(mockTrigger2);
        expect(result[2]).toEqual(mockTrigger3);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE"), []);
      });

      it("filters triggers with custom filter function", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockTrigger1, mockTrigger2, mockTrigger3],
        });

        const { getAllTriggers } = createTriggerLoaders(client);
        const result = await getAllTriggers((t) => t.tgrelid === 16380);

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockTrigger1);
        expect(result[1]).toEqual(mockTrigger2);
      });
    });
  });
});
