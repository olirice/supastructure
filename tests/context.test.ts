import { context, releaseClient, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";
import { Pool } from "pg";

describe("context", () => {
  it("should create a context with a new client", async () => {
    const ctx = await context(dbConfig);
    expect(ctx.resolveDatabase).toBeDefined();
    expect(ctx.resolveNamespaces).toBeDefined();
    expect(ctx.typeLoader).toBeDefined();
    expect(ctx.triggerLoader).toBeDefined();
    expect(ctx.policyLoader).toBeDefined();
    expect(ctx.namespaceByNameLoader).toBeDefined();
    await releaseClient(ctx.client);
  });

  it("should batch type lookups with DataLoader", async () => {
    const mockClient = {
      query: jest.fn().mockResolvedValue({
        rows: [
          { oid: 1, typname: "int4", typtype: "b", nspname: "pg_catalog" },
          { oid: 2, typname: "text", typtype: "b", nspname: "pg_catalog" },
        ],
      }),
      release: jest.fn(),
      connect: jest.fn(),
      end: jest.fn(),
    } as any;

    const ctx = await context(dbConfig, mockClient);

    // Load multiple types in parallel
    const [type1Promise, type2Promise] = [ctx.typeLoader.load(1), ctx.typeLoader.load(2)];

    const [type1, type2] = await Promise.all([type1Promise, type2Promise]);

    // Verify types were loaded correctly
    expect(type1).toHaveProperty("oid", 1);
    expect(type1).toHaveProperty("typname", "int4");
    expect(type2).toHaveProperty("oid", 2);
    expect(type2).toHaveProperty("typname", "text");

    // Verify only one query was executed for both types
    expect(mockClient.query).toHaveBeenCalledTimes(1);
    expect(mockClient.query.mock.calls[0][1][0]).toContain(1);
    expect(mockClient.query.mock.calls[0][1][0]).toContain(2);
  });

  it("should batch trigger lookups with DataLoader", async () => {
    const mockClient = {
      query: jest
        .fn()
        // First call is for triggerLoader (by OID)
        .mockResolvedValueOnce({
          rows: [
            { oid: 101, tgname: "trigger1", tgrelid: 1000 },
            { oid: 102, tgname: "trigger2", tgrelid: 1000 },
          ],
        })
        // Second call is for triggersByRelationLoader (by tgrelid)
        .mockResolvedValueOnce({
          rows: [
            { oid: 101, tgname: "trigger1", tgrelid: 1000 },
            { oid: 102, tgname: "trigger2", tgrelid: 1000 },
          ],
        }),
      release: jest.fn(),
      connect: jest.fn(),
      end: jest.fn(),
    } as any;

    const ctx = await context(dbConfig, mockClient);

    // Load trigger by OID
    const trigger = await ctx.triggerLoader.load(101);

    // Verify trigger was loaded correctly
    expect(trigger).toBeTruthy();
    expect(trigger).toHaveProperty("oid", 101);
    expect(trigger).toHaveProperty("tgname", "trigger1");

    // Also test triggersByRelationLoader which returns triggers by relation ID
    const triggers = await ctx.triggersByRelationLoader.load(1000);

    // Verify triggers were loaded correctly
    expect(triggers).toHaveLength(2);
    expect(triggers[0]).toHaveProperty("oid", 101);
    expect(triggers[0]).toHaveProperty("tgname", "trigger1");
    expect(triggers[1]).toHaveProperty("oid", 102);
    expect(triggers[1]).toHaveProperty("tgname", "trigger2");

    // Verify queries were executed
    expect(mockClient.query).toHaveBeenCalledTimes(2);
  });

  it("should batch policy lookups with DataLoader", async () => {
    const mockClient = {
      query: jest
        .fn()
        // First call is for policiesByRelationLoader (by relid)
        .mockResolvedValueOnce({
          rows: [
            {
              oid: 201,
              polname: "policy1",
              polrelid: 2000,
              polcmd: "r",
              polroles: [],
            },
            {
              oid: 202,
              polname: "policy2",
              polrelid: 2000,
              polcmd: "w",
              polroles: [],
            },
          ],
        })
        // Second call is for policyLoader (by oid)
        .mockResolvedValueOnce({
          rows: [
            {
              oid: 201,
              polname: "policy1",
              polrelid: 2000,
              polcmd: "r",
              polroles: [],
            },
          ],
        }),
      release: jest.fn(),
      connect: jest.fn(),
      end: jest.fn(),
    } as any;

    const ctx = await context(dbConfig, mockClient);

    // Test policiesByRelationLoader - Load policies for a table by relation OID
    const policies = await ctx.policiesByRelationLoader.load(2000);

    // Verify policies were loaded correctly
    expect(policies).toHaveLength(2);
    expect(policies[0]).toHaveProperty("oid", 201);
    expect(policies[0]).toHaveProperty("polname", "policy1");
    expect(policies[1]).toHaveProperty("oid", 202);
    expect(policies[1]).toHaveProperty("polname", "policy2");

    // Now test policyLoader - Load a single policy by its OID
    const policy = await ctx.policyLoader.load(201);

    // Verify policy was loaded correctly
    expect(policy).not.toBeNull();
    expect(policy).toHaveProperty("oid", 201);
    expect(policy).toHaveProperty("polname", "policy1");

    // Verify queries were executed
    expect(mockClient.query).toHaveBeenCalledTimes(2);
  });

  it("should batch namespace lookups by name with DataLoader", async () => {
    const mockClient = {
      query: jest.fn().mockResolvedValue({
        rows: [
          { oid: 301, nspname: "public", nspowner: 10 },
          { oid: 302, nspname: "auth", nspowner: 10 },
        ],
      }),
      release: jest.fn(),
      connect: jest.fn(),
      end: jest.fn(),
    } as any;

    const ctx = await context(dbConfig, mockClient);

    // Load multiple namespaces in parallel by name
    const [ns1Promise, ns2Promise] = [
      ctx.namespaceByNameLoader.load("public"),
      ctx.namespaceByNameLoader.load("auth"),
    ];

    const [ns1, ns2] = await Promise.all([ns1Promise, ns2Promise]);

    // Verify namespaces were loaded correctly
    expect(ns1).toHaveProperty("oid", 301);
    expect(ns1).toHaveProperty("nspname", "public");
    expect(ns2).toHaveProperty("oid", 302);
    expect(ns2).toHaveProperty("nspname", "auth");

    // Verify only one query was executed for both namespaces
    expect(mockClient.query).toHaveBeenCalledTimes(1);
    expect(mockClient.query.mock.calls[0][1][0]).toContain("public");
    expect(mockClient.query.mock.calls[0][1][0]).toContain("auth");
  });

  it("should handle errors during context creation", async () => {
    const invalidConfig = { ...dbConfig, database: "invalid_db" };
    await expect(context(invalidConfig)).rejects.toThrow();
  });
});
