import { context, releaseClient, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";
import { Pool } from "pg";

describe("context", () => {
  it("should create a context with a new client", async () => {
    const ctx = await context(dbConfig);
    expect(ctx.pg_database).toBeDefined();
    expect(ctx.pg_namespaces).toBeDefined();
    await releaseClient(ctx.client);
  });

  it("should handle errors during context creation", async () => {
    const invalidConfig = { ...dbConfig, database: "invalid_db" };
    await expect(context(invalidConfig)).rejects.toThrow();
  });
});
