import { ApolloServer } from "@apollo/server";
import { Pool, PoolClient } from "pg";
import fs from "fs";
import { gql } from "graphql-tag";
import { resolvers } from "../src/resolvers.js";
import type { ReqContext } from "../src/context.js";
import { context, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";
import { createComplexityLimitRule } from "graphql-validation-complexity";

// Constants
const pool = new Pool(dbConfig);
const typeDefs = gql(fs.readFileSync("src/schema.graphql", "utf8"));

/**
 * Executes a GraphQL query against the test server.
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

  const errors = response.body.kind === "single" ? response.body.singleResult.errors : null;
  const data = response.body.kind === "single" ? response.body.singleResult.data : null;

  return { errors, data };
}

describe("GraphQL Complexity Limit Tests", () => {
  let testServer: ApolloServer<ReqContext>;
  let client: PoolClient;

  beforeAll(() => {
    // Use a much lower complexity limit for testing
    const complexityLimitRule = createComplexityLimitRule(100, {
      scalarCost: 1, // Each scalar field costs 1 point
      objectCost: 2, // Each object field costs 2 points
      listFactor: 10, // List fields multiply cost by 10 × number of items
    });

    testServer = new ApolloServer({
      typeDefs,
      resolvers,
      validationRules: [complexityLimitRule],
    });
  });

  beforeEach(async () => {
    client = await pool.connect();
    await client.query("begin;"); // Start a transaction
  });

  afterEach(async () => {
    // Roll back the transaction so tests remain isolated
    await client.query("rollback;");
    client.release();
  });

  afterAll(async () => {
    await pool.end();
    await testServer.stop();
  });

  it("allows queries with complexity under the limit", async () => {
    // This is a simple query with low complexity
    const simpleQuery = `
      query {
        database {
          id
          name
          oid
        }
      }
    `;

    const { errors, data } = await executeTestQuery(testServer, simpleQuery, {}, client);
    expect(errors).toBeUndefined();
    expect(data).not.toBeNull();
  });

  it("rejects queries that exceed the complexity limit", async () => {
    // Create a complex query that requests multiple nested objects and lists
    const complexQuery = `
      query {
        database {
          id
          name
          # This creates a list which will increase complexity by listFactor
          schemas(first: 100) {
            edges {
              cursor
              node {
                id
                name
                oid
                # Another list, further increasing complexity
                tables(first: 100) {
                  edges {
                    cursor
                    node {
                      id
                      name
                      oid
                      # And another list, with even more complexity
                      columns {
                        nodes {
                          id
                          name
                          type {
                            ... on CompositeType {
                              id
                              name
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    `;

    const { errors, data } = await executeTestQuery(testServer, complexQuery, {}, client);

    // Log errors for debugging
    if (errors) {
      console.log("Validation errors:", JSON.stringify(errors, null, 2));
    }

    // Should have validation errors
    expect(errors).toBeDefined();
    expect(errors && errors.length).toBeGreaterThan(0);

    // Verify the error is about complexity
    const complexityError = errors?.some(
      (error) =>
        error.message.includes("exceeds complexity limit") ||
        error.message.includes("too complicated") ||
        (error.extensions?.code === "GRAPHQL_VALIDATION_FAILED" &&
          error.message.toLowerCase().includes("complex"))
    );
    expect(complexityError).toBe(true);
  });
});
