import { resolvers } from "../src/resolvers.js";
import { ReqContext } from "../src/context.js";

function createTestContext(overrides: Partial<ReqContext> = {}): ReqContext {
  return {
    pg_database: { oid: 1, datname: "test_db" },
    pg_namespaces: [],
    pg_classes: [],
    pg_attributes: [],
    pg_policies: [],
    pg_roles: [],
    pg_triggers: [],
    pg_types: [],
    pg_enums: [],
    pg_index: [],
    pg_foreign_keys: [],
    client: {
      query: jest.fn().mockResolvedValue({ rows: [] }),
      release: jest.fn(),
      connect: jest.fn(),
      copyFrom: jest.fn(),
      copyTo: jest.fn(),
      end: jest.fn(),
      on: jest.fn(),
      off: jest.fn(),
    } as any,
    ...overrides,
  };
}

describe("Resolvers with null branches", () => {
  test("Table resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext({
      pg_namespaces: [{ oid: 1, nspname: "public" }],
      pg_classes: [{ oid: 123, relname: "test_table", relnamespace: 2, relkind: "r", relrowsecurity: false }],
    });

    const result = resolvers.Table.schema(
      { oid: 123, relname: "test_table", relnamespace: 2, relkind: "r", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });

  test("View resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext({
      pg_namespaces: [{ oid: 1, nspname: "public" }],
      pg_classes: [{ oid: 123, relname: "test_view", relnamespace: 2, relkind: "v", relrowsecurity: false }],
    });

    const result = resolvers.View.schema(
      { oid: 123, relname: "test_view", relnamespace: 2, relkind: "v", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });

  test("MaterializedView resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext({
      pg_namespaces: [{ oid: 1, nspname: "public" }],
      pg_classes: [{ oid: 123, relname: "test_matview", relnamespace: 2, relkind: "m", relrowsecurity: false }],
    });

    const result = resolvers.MaterializedView.schema(
      { oid: 123, relname: "test_matview", relnamespace: 2, relkind: "m", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });

  test("Index resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext({
      pg_namespaces: [{ oid: 1, nspname: "public" }],
      pg_classes: [{ oid: 123, relname: "test_index", relnamespace: 2, relkind: "i", relrowsecurity: false }],
    });

    const result = resolvers.Index.schema(
      { oid: 123, relname: "test_index", relnamespace: 2, relkind: "i", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });
});
