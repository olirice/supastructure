import { Client } from "pg";
import { createRoleLoaders, roleQueries } from "../../src/loaders/pg_roles.js";
import { PgRole } from "../../src/types.js";

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

describe("pg_roles loader", () => {
  let client: Client;
  let mockQuery: jest.Mock;

  // Mock role objects to use in tests
  const mockRole1: PgRole = {
    oid: 1001,
    rolname: "admin",
    rolsuper: true,
  };

  const mockRole2: PgRole = {
    oid: 1002,
    rolname: "user",
    rolsuper: false,
  };

  const mockRole3: PgRole = {
    oid: 1003,
    rolname: "app_user",
    rolsuper: false,
  };

  beforeEach(() => {
    client = new Client();
    mockQuery = client.query as jest.Mock;
    mockQuery.mockReset();
  });

  describe("roleQueries", () => {
    describe("query", () => {
      it("queries roles with default options", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1, mockRole2, mockRole3],
        });

        const result = await roleQueries.query(client);

        expect(result).toHaveLength(3);
        expect(result[0]).toEqual(mockRole1);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("SELECT oid, rolname, rolsuper"),
          []
        );
      });

      it("queries roles with roleOids filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1],
        });

        const result = await roleQueries.query(client, { roleOids: [1001] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockRole1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE r.oid = ANY($1)"), [
          [1001],
        ]);
      });

      it("queries roles with roleNames filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole2],
        });

        const result = await roleQueries.query(client, { roleNames: ["user"] });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockRole2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("WHERE r.rolname = ANY($1)"),
          [["user"]]
        );
      });

      it("queries roles with onlySuperusers filter", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1],
        });

        const result = await roleQueries.query(client, { onlySuperusers: true });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockRole1);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("WHERE r.rolsuper = true"),
          []
        );
      });

      it("combines multiple filter conditions", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1],
        });

        const result = await roleQueries.query(client, {
          roleOids: [1001],
          onlySuperusers: true,
        });

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockRole1);

        const query = mockQuery.mock.calls[0][0];
        expect(query).toContain("r.oid = ANY($1)");
        expect(query).toContain("r.rolsuper = true");
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE"), [[1001]]);
      });
    });

    describe("byOid", () => {
      it("returns a role when found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1],
        });

        const result = await roleQueries.byOid(client, 1001);

        expect(result).not.toBeNull();
        expect(result).toEqual(mockRole1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("SELECT"), [[1001]]);
      });

      it("returns null when role not found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [],
        });

        const result = await roleQueries.byOid(client, 9999);

        expect(result).toBeNull();
      });
    });

    describe("byName", () => {
      it("returns a role when found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole2],
        });

        const result = await roleQueries.byName(client, "user");

        expect(result).not.toBeNull();
        expect(result).toEqual(mockRole2);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("SELECT"), [["user"]]);
      });

      it("returns null when role not found", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [],
        });

        const result = await roleQueries.byName(client, "nonexistent");

        expect(result).toBeNull();
      });
    });
  });

  describe("DataLoaders", () => {
    describe("roleLoader", () => {
      it("loads a single role by OID", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1],
        });

        const { roleLoader } = createRoleLoaders(client);
        const result = await roleLoader.load(1001);

        expect(result).not.toBeNull();
        expect(result).toEqual(mockRole1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE r.oid = ANY($1)"), [
          [1001],
        ]);
      });

      it("loads multiple roles by OID in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1, mockRole2],
        });

        const { roleLoader } = createRoleLoaders(client);
        const results = await Promise.all([roleLoader.load(1001), roleLoader.load(1002)]);

        expect(results).toHaveLength(2);
        expect(results[0]).toEqual(mockRole1);
        expect(results[1]).toEqual(mockRole2);
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining("WHERE r.oid = ANY($1)"), [
          [1001, 1002],
        ]);
      });

      it("returns null for non-existent roles", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1],
        });

        const { roleLoader } = createRoleLoaders(client);
        const results = await Promise.all([roleLoader.load(1001), roleLoader.load(9999)]);

        expect(results[0]).toEqual(mockRole1);
        expect(results[1]).toBeNull();
      });
    });

    describe("roleByNameLoader", () => {
      it("loads a single role by name", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole2],
        });

        const { roleByNameLoader } = createRoleLoaders(client);
        const result = await roleByNameLoader.load("user");

        expect(result).not.toBeNull();
        expect(result).toEqual(mockRole2);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("WHERE r.rolname = ANY($1)"),
          [["user"]]
        );
      });

      it("loads multiple roles by name in a single query", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1, mockRole2],
        });

        const { roleByNameLoader } = createRoleLoaders(client);
        const results = await Promise.all([
          roleByNameLoader.load("admin"),
          roleByNameLoader.load("user"),
        ]);

        expect(results).toHaveLength(2);
        expect(results[0]).toEqual(mockRole1);
        expect(results[1]).toEqual(mockRole2);
        expect(mockQuery).toHaveBeenCalledTimes(1);
        expect(mockQuery).toHaveBeenCalledWith(
          expect.stringContaining("WHERE r.rolname = ANY($1)"),
          [["admin", "user"]]
        );
      });

      it("returns null for non-existent role names", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole2],
        });

        const { roleByNameLoader } = createRoleLoaders(client);
        const results = await Promise.all([
          roleByNameLoader.load("user"),
          roleByNameLoader.load("nonexistent"),
        ]);

        expect(results[0]).toEqual(mockRole2);
        expect(results[1]).toBeNull();
      });
    });

    describe("getAllRoles", () => {
      it("returns all roles", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1, mockRole2, mockRole3],
        });

        const { getAllRoles } = createRoleLoaders(client);
        const result = await getAllRoles();

        expect(result).toHaveLength(3);
        expect(result[0]).toEqual(mockRole1);
        expect(result[1]).toEqual(mockRole2);
        expect(result[2]).toEqual(mockRole3);
      });

      it("filters roles with custom filter function", async () => {
        mockQuery.mockResolvedValueOnce({
          rows: [mockRole1, mockRole2, mockRole3],
        });

        const { getAllRoles } = createRoleLoaders(client);
        const result = await getAllRoles((role) => role.rolsuper === true);

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual(mockRole1);
      });
    });
  });
});
