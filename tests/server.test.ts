import { ApolloServer } from "@apollo/server";
import { Pool, PoolClient } from "pg";
import fs from "fs";
import { gql } from "graphql-tag";
import { resolvers } from "../src/resolvers.js";
import { context, ReqContext, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";

// Constants
const pool = new Pool(dbConfig);
const typeDefs = gql(fs.readFileSync("src/schema.graphql", "utf8"));

/**
 * Executes a GraphQL query against the test server and logs the response.
 *
 * @param {ApolloServer<ReqContext>} testServer - The Apollo Server instance under test.
 * @param {string} query - The GraphQL query string.
 * @param {Record<string, any>} [variables={}] - Optional variables for the query.
 * @param {PoolClient} client - The PostgreSQL client used for the request.
 * @returns {Promise<{ response: any, errors: any, data: any }>} - The full response, errors, and extracted data.
 */
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

  //console.log("GraphQL Response:", response.body);

  const errors =
    response.body.kind === "single" ? response.body.singleResult.errors : null;
  const data =
    response.body.kind === "single" ? response.body.singleResult.data : null;

  if (errors) {
    //console.error("GraphQL Errors:", errors);
  }

  //console.log("Data:", data);

  return { response, errors, data };
}

/**
 * GraphQL server transactional tests.
 *
 * Each test runs inside a transaction that is rolled back afterward, ensuring database isolation.
 */
describe("GraphQL Server - Transactional Tests", () => {
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
    await client.query("begin;"); // Start a transaction
  });

  afterEach(async () => {
    // Roll back the transaction so tests remain isolated
    client.release();
  });

  afterAll(async () => {
    await pool.end();
    await testServer.stop();
  });

  // =====================================
  // TEST: Fetch database details
  // =====================================
  it("fetches database details", async () => {
    // Uses the default schemaSetup (does nothing)

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

  // =====================================
  // TEST: Fetch a Specific Schema
  // =====================================
  it("fetches a specific schema by name", async () => {
    await client.query("create schema foo;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "foo") {
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
      schema: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "foo",
      }),
    });
    expect(errors).toBeUndefined();
  });
});
