import { ApolloServer } from "@apollo/server";
import { resolvers } from "../src/resolvers.js";
import { gql } from "graphql-tag";
import { context, ReqContext, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";
import { Pool, PoolClient } from "pg";
import fs from "fs";

const pool = new Pool(dbConfig);
const typeDefs = gql(fs.readFileSync("src/schema.graphql", "utf8"));

async function executeTestQuery(
  testServer: ApolloServer<ReqContext>,
  query: string,
  variables = {},
  client: PoolClient
) {
  const response = await testServer.executeOperation(
    {
      query,
      variables,
    },
    { contextValue: await context(dbConfig, client) }
  );

  const errors =
    response.body.kind === "single" ? response.body.singleResult.errors : null;
  const data =
    response.body.kind === "single" ? response.body.singleResult.data : null;

  return { response, errors, data };
}

describe("resolvers", () => {
  let testServer: ApolloServer<ReqContext>;
  let client: PoolClient;

  beforeAll(() => {
    testServer = new ApolloServer({
      typeDefs,
      resolvers,
    });
  });

  beforeEach(async () => {
    client = await pool.connect();
    await client.query("begin;");
  });

  afterEach(async () => {
    client.release();
  });

  afterAll(async () => {
    await pool.end();
    await testServer.stop();
  });

  it("should fetch database details", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          database {
            id
            oid
            name
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      database: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "postgres",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // Add more tests to cover the remaining lines in resolvers.ts
  // For example, tests for fetching schemas, tables, views, materialized views, indexes, triggers, policies, types, roles, etc.
});
