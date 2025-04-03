import { Client } from "pg";
import { createPolicyLoaders, policyQueries } from "../../src/loaders/pg_policies.js";
import { PgPolicy, PgPolicySchema } from "../../src/types.js";

// Mock the PgPolicySchema.parse function
jest.mock("../../src/types.js", () => {
  const actual = jest.requireActual("../../src/types.js");
  return {
    ...actual,
    PgPolicySchema: {
      ...actual.PgPolicySchema,
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

describe("pg_policies loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Mock policy objects to use in tests
  const mockPolicy1: PgPolicy = {
    oid: 16395,
    polname: "tenant_isolation_policy",
    polrelid: 16380,
    polcmd: "SELECT",
    polroles: ["tenant_user"],
    polqual: "tenant_id = current_setting('app.tenant_id')",
    polwithcheck: null,
  };

  const mockPolicy2: PgPolicy = {
    oid: 16396,
    polname: "tenant_isolation_insert",
    polrelid: 16380,
    polcmd: "INSERT",
    polroles: ["tenant_user"],
    polqual: null,
    polwithcheck: "tenant_id = current_setting('app.tenant_id')",
  };

  const mockPolicy3: PgPolicy = {
    oid: 16397,
    polname: "admin_policy",
    polrelid: 16381,
    polcmd: "ALL",
    polroles: ["admin"],
    polqual: null,
    polwithcheck: null,
  };

  // For tests, we'll need to provide schema info separately since it's not in the type
  const mockSchemaInfo = {
    public: ["tenant_isolation_policy", "tenant_isolation_insert", "admin_policy"],
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("policyQueries", () => {
    describe("query", () => {
      it("queries policies with oids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1],
        });

        const result = await policyQueries.query(client, { oids: [16395] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockPolicy1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND p.oid = ANY($1)"), [
          [16395],
        ]);
        expect(PgPolicySchema.parse).toHaveBeenCalledWith(mockPolicy1);
      });

      it("queries policies with policyNames filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1],
        });

        const result = await policyQueries.query(client, {
          policyNames: ["tenant_isolation_policy"],
        });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockPolicy1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND p.polname = ANY($1)"), [
          ["tenant_isolation_policy"],
        ]);
      });

      it("queries policies with policyRelids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2],
        });

        const result = await policyQueries.query(client, { policyRelids: [16380] });

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockPolicy1);
        expect(result[1]).toEqual(mockPolicy2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND p.polrelid = ANY($1)"),
          [[16380]]
        );
      });

      it("queries policies with schemaNames filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2, mockPolicy3],
        });

        const result = await policyQueries.query(client, { schemaNames: ["public"] });

        expect(result).toHaveLength(3);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND n.nspname = ANY($1)"), [
          ["public"],
        ]);
      });

      it("excludes system schemas when all option is true", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2, mockPolicy3],
        });

        const result = await policyQueries.query(client, { all: true });

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
          rows: [mockPolicy1],
        });

        const result = await policyQueries.query(client, {
          policyRelids: [16380],
          schemaNames: ["public"],
        });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockPolicy1);

        const query = mockQuery.mock.calls[0][0];
        expect(query).toContain("AND p.polrelid = ANY($1)");
        expect(query).toContain("AND n.nspname = ANY($2)");
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringMatching(/AND p\.polrelid = ANY\(\$1\)[\s\S]*AND n\.nspname = ANY\(\$2\)/),
          [[16380], ["public"]]
        );
      });
    });

    describe("byOid", () => {
      it("returns a policy when found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1],
        });

        const result = await policyQueries.byOid(client, 16395);

        expect(result).not.toBeNull();
        expect(result).toEqual(mockPolicy1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND p.oid = ANY($1)"), [
          [16395],
        ]);
      });

      it("returns null when policy not found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [],
        });

        const result = await policyQueries.byOid(client, 99999);

        expect(result).toBeNull();
      });
    });

    describe("byRelationOid", () => {
      it("returns policies for a relation", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2],
        });

        const result = await policyQueries.byRelationOid(client, 16380);

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockPolicy1);
        expect(result[1]).toEqual(mockPolicy2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND p.polrelid = ANY($1)"),
          [[16380]]
        );
      });
    });

    describe("byNameAndSchema", () => {
      it("returns a policy when found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1],
        });

        const result = await policyQueries.byNameAndSchema(
          client,
          "public",
          "tenant_isolation_policy"
        );

        expect(result).not.toBeNull();
        expect(result).toEqual(mockPolicy1);
        expect(mockQuery).toHaveBeenCalledWith(expect.any(String), [
          ["tenant_isolation_policy"],
          ["public"],
        ]);
      });

      it("returns null when policy not found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [],
        });

        const result = await policyQueries.byNameAndSchema(client, "public", "nonexistent");

        expect(result).toBeNull();
      });
    });
  });

  describe("DataLoaders", () => {
    describe("policyLoader", () => {
      it("loads a single policy by OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1],
        });

        const { policyLoader } = createPolicyLoaders(client);
        const result = await policyLoader.load(16395);

        expect(result).not.toBeNull();
        expect(result).toEqual(mockPolicy1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND p.oid = ANY($1)"), [
          [16395],
        ]);
      });

      it("loads multiple policies by OID in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy3],
        });

        const { policyLoader } = createPolicyLoaders(client);
        const results = await Promise.all([policyLoader.load(16395), policyLoader.load(16397)]);

        expect(results).toHaveLength(2);
        expect(results[0]).toEqual(mockPolicy1);
        expect(results[1]).toEqual(mockPolicy3);
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("AND p.oid = ANY($1)"), [
          [16395, 16397],
        ]);
      });

      it("returns null for non-existent policies", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1],
        });

        const { policyLoader } = createPolicyLoaders(client);
        const results = await Promise.all([policyLoader.load(16395), policyLoader.load(99999)]);

        expect(results[0]).toEqual(mockPolicy1);
        expect(results[1]).toBeNull();
      });
    });

    describe("policiesByRelationLoader", () => {
      it("loads policies by relation OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2],
        });

        const { policiesByRelationLoader } = createPolicyLoaders(client);
        const result = await policiesByRelationLoader.load(16380);

        expect(result).toHaveLength(2);
        expect(result[0]).toEqual(mockPolicy1);
        expect(result[1]).toEqual(mockPolicy2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND p.polrelid = ANY($1)"),
          [[16380]]
        );
      });

      it("loads policies for multiple relations in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2, mockPolicy3],
        });

        const { policiesByRelationLoader } = createPolicyLoaders(client);
        const results = await Promise.all([
          policiesByRelationLoader.load(16380),
          policiesByRelationLoader.load(16381),
        ]);

        expect(results).toHaveLength(2);
        expect(results[0]).toHaveLength(2); // Two policies for first relation
        expect(results[1]).toHaveLength(1); // One policy for second relation
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("AND p.polrelid = ANY($1)"),
          [[16380, 16381]]
        );
      });

      it("returns empty array for relations with no policies", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2],
        });

        const { policiesByRelationLoader } = createPolicyLoaders(client);
        const results = await Promise.all([
          policiesByRelationLoader.load(16380),
          policiesByRelationLoader.load(99999),
        ]);

        expect(results[0]).toHaveLength(2);
        expect(results[1]).toEqual([]);
      });
    });

    describe("getAllPolicies", () => {
      it("returns all policies (excluding system policies)", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2, mockPolicy3],
        });

        const { getAllPolicies } = createPolicyLoaders(client);
        const result = await getAllPolicies();

        expect(result).toHaveLength(3);
        expect(result[0]).toEqual(mockPolicy1);
        expect(result[1]).toEqual(mockPolicy2);
        expect(result[2]).toEqual(mockPolicy3);
        expect(mockQuery).toHaveBeenCalledWith(expect.any(String), []);
      });

      it("filters policies with custom filter function", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockPolicy1, mockPolicy2, mockPolicy3],
        });

        const { getAllPolicies } = createPolicyLoaders(client);
        const result = await getAllPolicies((p) => p.polcmd === "SELECT");

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockPolicy1);
      });
    });
  });
});
