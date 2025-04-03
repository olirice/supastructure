import { ApolloServer } from "@apollo/server";
import { Pool, PoolClient } from "pg";
import fs from "fs";
import { gql } from "graphql-tag";
import { resolvers } from "../src/resolvers.js";
import type { ReqContext } from "../src/context.js";
import { context, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";
import depthLimit from "graphql-depth-limit";

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

describe("GraphQL Depth Limit Tests", () => {
  let testServer: ApolloServer<ReqContext>;
  let client: PoolClient;

  beforeAll(() => {
    testServer = new ApolloServer({
      typeDefs,
      resolvers,
      validationRules: [
        // Use the same depth limit as in production
        depthLimit(9)
      ],
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

  it("allows queries with depth <= 9", async () => {
    // This query has a depth of 9:
    // 1. Query
    // 2. database
    // 3. schemas
    // 4. nodes
    // 5. tables
    // 6. nodes
    // 7. columns
    // 8. nodes
    // 9. type
    const validQuery = `
      query {
        database {
          id
          schemas {
            nodes {
              name
              tables {
                nodes {
                  name
                  columns {
                    nodes {
                      name
                      type {
                        ... on PgTypeInterface {
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
    `;

    const { errors, data } = await executeTestQuery(testServer, validQuery, {}, client);
    expect(errors).toBeUndefined();
    expect(data).not.toBeNull();
  });

  it("rejects queries with depth > 9", async () => {
    // This query has a greater depth:
    // 1. Query
    // 2. database
    // 3. schemas
    // 4. nodes
    // 5. tables
    // 6. nodes
    // 7. columns
    // 8. nodes
    // 9. type
    // 10. attributes (exceeds limit)
    const tooDeepQuery = `
      query {
        database {
          id
          schemas {
            nodes {
              name
              tables {
                nodes {
                  name
                  columns {
                    nodes {
                      name
                      type {
                        ... on CompositeType {
                          name
                          attributes {
                            nodes {
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

    const { errors, data } = await executeTestQuery(testServer, tooDeepQuery, {}, client);
    
    // Log all errors for debugging
    if (errors) {
      console.log('Validation errors:', JSON.stringify(errors, null, 2));
    }
    
    // Should have validation errors
    expect(errors).toBeDefined();
    expect(errors && errors.length).toBeGreaterThan(0);
    
    // Verify the error is about max depth
    const depthError = errors?.some(error => 
      error.message.includes('exceeds maximum depth') || 
      error.message.includes('deeper than the maximum') ||
      (error.extensions?.code === 'GRAPHQL_VALIDATION_FAILED' && 
       error.message.toLowerCase().includes('depth'))
    );
    expect(depthError).toBe(true);
  });
}); 