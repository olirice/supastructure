import { ApolloServer } from "@apollo/server";
import { Pool, PoolClient } from "pg";
import fs from "fs";
import { gql } from "graphql-tag";
import { resolvers } from "../src/resolvers.js";
import { context, ReqContext, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";
import { buildGlobalId } from "../src/generic.js";
import { table } from "console";

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
    console.error("GraphQL Errors:", errors);
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
    await client.query("rollback;"); // Start a transaction
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

  // =====================================
  // TEST: Fetch a Specific Schema by OID
  // =====================================
  it("fetches a specific schema by oid", async () => {
    await client.query("create schema bar;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_namespace
      where nspname = 'bar'
    `);
    const schemaOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          schema(oid: $oid) {
            id
            oid
            name
          }
        }
      `,
      { oid: schemaOid },
      client
    );

    expect(data).toMatchObject({
      schema: expect.objectContaining({
        id: expect.any(String),
        oid: schemaOid,
        name: "bar",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Schema by ID
  // =====================================
  it("fetches a specific schema by id", async () => {
    await client.query("create schema baz;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_namespace
      where nspname = 'baz'
    `);
    const schemaOid = result.rows[0].oid;
    const schemaId = buildGlobalId("Schema", schemaOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          schema(id: $id) {
            id
            oid
            name
          }
        }
      `,
      { id: schemaId },
      client
    );

    expect(data).toMatchObject({
      schema: expect.objectContaining({
        id: schemaId,
        oid: schemaOid,
        name: "baz",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Table by OID
  // =====================================
  it("fetches a specific table by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_table'
    `);
    const tableOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          table(oid: $oid) {
            id
            oid
            name
            rowLevelSecurityEnabled
          }
        }
      `,
      { oid: tableOid },
      client
    );

    expect(data).toMatchObject({
      table: expect.objectContaining({
        id: expect.any(String),
        oid: tableOid,
        name: "test_table",
        rowLevelSecurityEnabled: false,
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Table by ID
  // =====================================
  it("fetches a specific table by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_table'
    `);
    const tableOid = result.rows[0].oid;
    const tableId = buildGlobalId("Table", tableOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          table(id: $id) {
            id
            oid
            name
          }
        }
      `,
      { id: tableId },
      client
    );

    expect(data).toMatchObject({
      table: expect.objectContaining({
        id: tableId,
        oid: tableOid,
        name: "test_table",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Table by Schema Name and Table Name
  // =====================================
  it("fetches a specific table by schema name and table name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
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
      table: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_table",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Columns for a Specific Table
  // =====================================
  it("fetches columns for a specific table", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key, name text);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            columns {
              edges {
                node {
                  id
                  name
                  attnum
                  atttypid
                }
                cursor
              }
              nodes {
                id
                name
                attnum
                atttypid
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
      `,
      {},
      client
    );

    // Create a loose matcher that doesn't care about specific values
    expect(data).toEqual({
      table: {
        columns: {
          edges: expect.arrayContaining([
            {
              cursor: expect.any(String),
              node: {
                id: expect.any(String),
                name: "id",
                attnum: 1,
                atttypid: expect.any(Number)
              }
            },
            {
              cursor: expect.any(String),
              node: {
                id: expect.any(String),
                name: "name", 
                attnum: 2,
                atttypid: expect.any(Number)
              }
            }
          ]),
          nodes: expect.arrayContaining([
            {
              id: expect.any(String),
              name: "id",
              attnum: 1, 
              atttypid: expect.any(Number)
            },
            {
              id: expect.any(String),
              name: "name",
              attnum: 2,
              atttypid: expect.any(Number)
            }
          ]),
          pageInfo: {
            hasNextPage: false,
            endCursor: expect.any(String)
          }
        }
      }
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Schema for a Specific Table
  // =====================================
  it("fetches schema for a specific table", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            schema {
              id
              oid
              name
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      table: {
        schema: expect.objectContaining({
          id: expect.any(String),
          oid: expect.any(Number),
          name: "test_schema",
        }),
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Schema for a Specific View
  // =====================================
  it("fetches schema for a specific view", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          view(schemaName: "test_schema", name: "test_view") {
            schema {
              id
              oid
              name
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      view: {
        schema: expect.objectContaining({
          id: expect.any(String),
          oid: expect.any(Number),
          name: "test_schema",
        }),
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Schema for a Specific Materialized View
  // =====================================
  it("fetches schema for a specific materialized view", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          materializedView(schemaName: "test_schema", name: "test_matview") {
            schema {
              id
              oid
              name
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      materializedView: {
        schema: expect.objectContaining({
          id: expect.any(String),
          oid: expect.any(Number),
          name: "test_schema",
        }),
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Views for a Specific Schema
  // =====================================
  it("fetches views for a specific schema", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            views {
              nodes {
                id
                oid
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      schema: {
        views: {
          nodes: [
            expect.objectContaining({
              id: expect.any(String),
              oid: expect.any(Number),
              name: "test_view",
            }),
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Materialized Views for a Specific Schema
  // =====================================
  it("fetches materialized views for a specific schema", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            materializedViews {
              nodes {
                id
                oid
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      schema: {
        materializedViews: {
          nodes: [
            expect.objectContaining({
              id: expect.any(String),
              oid: expect.any(Number),
              name: "test_matview",
            }),
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch Schema for a Specific Table
  // =====================================
  it("fetches schema for a specific table", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            schema {
              id
              oid
              name
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      table: {
        schema: expect.objectContaining({
          id: expect.any(String),
          oid: expect.any(Number),
          name: "test_schema",
        }),
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Schema for a Specific View
  // =====================================
  it("fetches schema for a specific view", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          view(schemaName: "test_schema", name: "test_view") {
            schema {
              id
              oid
              name
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      view: {
        schema: expect.objectContaining({
          id: expect.any(String),
          oid: expect.any(Number),
          name: "test_schema",
        }),
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Schema for a Specific Materialized View
  // =====================================
  it("fetches schema for a specific materialized view", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          materializedView(schemaName: "test_schema", name: "test_matview") {
            schema {
              id
              oid
              name
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      materializedView: {
        schema: expect.objectContaining({
          id: expect.any(String),
          oid: expect.any(Number),
          name: "test_schema",
        }),
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific View by OID
  // =====================================
  it("fetches a specific view by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_view'
    `);
    const viewOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          view(oid: $oid) {
            id
            oid
            name
          }
        }
      `,
      { oid: viewOid },
      client
    );

    expect(data).toMatchObject({
      view: expect.objectContaining({
        id: expect.any(String),
        oid: viewOid,
        name: "test_view",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific View by ID
  // =====================================
  it("fetches a specific view by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_view'
    `);
    const viewOid = result.rows[0].oid;
    const viewId = buildGlobalId("View", viewOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          view(id: $id) {
            id
            oid
            name
          }
        }
      `,
      { id: viewId },
      client
    );

    expect(data).toMatchObject({
      view: expect.objectContaining({
        id: viewId,
        oid: viewOid,
        name: "test_view",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific View by Schema Name and View Name
  // =====================================
  it("fetches a specific view by schema name and view name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          view(schemaName: "test_schema", name: "test_view") {
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
      view: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_view",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Columns for a Specific View
  // =====================================
  it("fetches columns for a specific view", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id, 'test' as name;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          view(schemaName: "test_schema", name: "test_view") {
            columns {
              edges {
                node {
                  id
                  name
                  attnum
                  atttypid
                }
                cursor
              }
              nodes {
                id
                name
                attnum
                atttypid
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      view: {
        columns: {
          edges: [
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "id",
                attnum: 1,
                atttypid: expect.any(Number),
              }),
              cursor: expect.any(String),
            }),
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "name",
                attnum: 2,
                atttypid: expect.any(Number),
              }),
              cursor: expect.any(String),
            }),
          ],
          nodes: [
            expect.objectContaining({
              id: expect.any(String),
              name: "id",
              attnum: 1,
              atttypid: expect.any(Number),
            }),
            expect.objectContaining({
              id: expect.any(String),
              name: "name",
              attnum: 2,
              atttypid: expect.any(Number),
            }),
          ],
          pageInfo: {
            hasNextPage: false,
            endCursor: expect.any(String),
          },
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Materialized View by OID
  // =====================================
  it("fetches a specific materialized view by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_matview'
    `);
    const matviewOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          materializedView(oid: $oid) {
            id
            oid
            name
          }
        }
      `,
      { oid: matviewOid },
      client
    );

    expect(data).toMatchObject({
      materializedView: expect.objectContaining({
        id: expect.any(String),
        oid: matviewOid,
        name: "test_matview",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Materialized View by ID
  // =====================================
  it("fetches a specific materialized view by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_matview'
    `);
    const matviewOid = result.rows[0].oid;
    const matviewId = buildGlobalId("MaterializedView", matviewOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          materializedView(id: $id) {
            id
            oid
            name
          }
        }
      `,
      { id: matviewId },
      client
    );

    expect(data).toMatchObject({
      materializedView: expect.objectContaining({
        id: matviewId,
        oid: matviewOid,
        name: "test_matview",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Materialized View by Schema Name and View Name
  // =====================================
  it("fetches a specific materialized view by schema name and view name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          materializedView(schemaName: "test_schema", name: "test_matview") {
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
      materializedView: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_matview",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Columns for a Specific Materialized View
  // =====================================
  it("fetches columns for a specific materialized view", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id, 'test' as name;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          materializedView(schemaName: "test_schema", name: "test_matview") {
            columns {
              edges {
                node {
                  id
                  name
                  attnum
                  atttypid
                }
                cursor
              }
              nodes {
                id
                name
                attnum
                atttypid
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      materializedView: {
        columns: {
          edges: [
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "id",
                attnum: 1,
                atttypid: expect.any(Number),
              }),
              cursor: expect.any(String),
            }),
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "name",
                attnum: 2,
                atttypid: expect.any(Number),
              }),
              cursor: expect.any(String),
            }),
          ],
          nodes: [
            expect.objectContaining({
              id: expect.any(String),
              name: "id",
              attnum: 1,
              atttypid: expect.any(Number),
            }),
            expect.objectContaining({
              id: expect.any(String),
              name: "name",
              attnum: 2,
              atttypid: expect.any(Number),
            }),
          ],
          pageInfo: {
            hasNextPage: false,
            endCursor: expect.any(String),
          },
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Index by OID
  // =====================================
  it("fetches a specific index by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_table_pkey'
    `);
    const indexOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          index(oid: $oid) {
            id
            oid
            name
          }
        }
      `,
      { oid: indexOid },
      client
    );

    expect(data).toMatchObject({
      index: expect.objectContaining({
        id: expect.any(String),
        oid: indexOid,
        name: "test_table_pkey",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Index by ID
  // =====================================
  it("fetches a specific index by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_table_pkey'
    `);
    const indexOid = result.rows[0].oid;
    const indexId = buildGlobalId("Index", indexOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          index(id: $id) {
            id
            oid
            name
          }
        }
      `,
      { id: indexId },
      client
    );

    expect(data).toMatchObject({
      index: expect.objectContaining({
        id: indexId,
        oid: indexOid,
        name: "test_table_pkey",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Index by Schema Name and Index Name
  // =====================================
  it("fetches a specific index by schema name and index name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          index(schemaName: "test_schema", name: "test_table_pkey") {
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
      index: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_table_pkey",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Index with all fields
  // =====================================
  it("fetches a specific index with all fields", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key, name text);");
    await client.query("create index test_index on test_schema.test_table (name);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_index'
    `);
    const indexOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          index(oid: $oid) {
            id
            oid
            name
            schema {
              id
              name
            }
            table {
              id
              name
            }
            accessMethod
            definition
          }
        }
      `,
      { oid: indexOid },
      client
    );

    expect(data).toMatchObject({
      index: expect.objectContaining({
        id: expect.any(String),
        oid: indexOid,
        name: "test_index",
        schema: expect.objectContaining({
          id: expect.any(String),
          name: "test_schema",
        }),
        table: expect.objectContaining({
          id: expect.any(String),
          name: "test_table",
        }),
        accessMethod: expect.any(String),
        definition: expect.any(String),
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Trigger by OID
  // =====================================
  it("fetches a specific trigger by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query(`
      create or replace function test_function()
      returns trigger as $$
      begin
        new.id := new.id + 1;
        return new;
      end;
      $$ language plpgsql;
    `);
    await client.query(`
      create trigger test_trigger
      before insert on test_schema.test_table
      for each row
      execute function test_function();
    `);
    const result = await client.query(`
      select oid
      from pg_catalog.pg_trigger
      where tgname = 'test_trigger'
    `);
    const triggerOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          trigger(oid: $oid) {
            id
            oid
            name
          }
        }
      `,
      { oid: triggerOid },
      client
    );

    expect(data).toMatchObject({
      trigger: expect.objectContaining({
        id: expect.any(String),
        oid: triggerOid,
        name: "test_trigger",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Trigger by ID
  // =====================================
  it("fetches a specific trigger by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query(`
      create or replace function test_function()
      returns trigger as $$
      begin
        new.id := new.id + 1;
        return new;
      end;
      $$ language plpgsql;
    `);
    await client.query(`
      create trigger test_trigger
      before insert on test_schema.test_table
      for each row
      execute function test_function();
    `);
    const result = await client.query(`
      select oid
      from pg_catalog.pg_trigger
      where tgname = 'test_trigger'
    `);
    const triggerOid = result.rows[0].oid;
    const triggerId = buildGlobalId("Trigger", triggerOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          trigger(id: $id) {
            id
            oid
            name
          }
        }
      `,
      { id: triggerId },
      client
    );

    expect(data).toMatchObject({
      trigger: expect.objectContaining({
        id: triggerId,
        oid: triggerOid,
        name: "test_trigger",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Trigger by Schema Name and Trigger Name
  // =====================================
  it("fetches a specific trigger by schema name and trigger name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query(`
      create or replace function test_function()
      returns trigger as $$
      begin
        new.id := new.id + 1;
        return new;
      end;
      $$ language plpgsql;
    `);
    await client.query(`
      create trigger test_trigger
      before insert on test_schema.test_table
      for each row
      execute function test_function();
    `);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          trigger(schemaName: "test_schema", name: "test_trigger") {
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
      trigger: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_trigger",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Policy by OID
  // =====================================
  it("fetches a specific policy by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query("create role test_role;");
    await client.query(`
      create policy test_policy
      on test_schema.test_table
      for select
      to test_role
      using (true);
    `);
    const result = await client.query(`
      select oid
      from pg_catalog.pg_policy
      where polname = 'test_policy'
    `);
    const policyOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          policy(oid: $oid) {
            id
            oid
            name
            roles
            command
            usingExpr
            withCheck
          }
        }
      `,
      { oid: policyOid },
      client
    );

    expect(data).toMatchObject({
      policy: expect.objectContaining({
        id: expect.any(String),
        oid: policyOid,
        name: "test_policy",
        roles: ["test_role"],
        command: "SELECT",
        usingExpr: "true",
        withCheck: null,
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Policy by ID
  // =====================================
  it("fetches a specific policy by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query("create role test_role;");
    await client.query(`
      create policy test_policy
      on test_schema.test_table
      for update
      to test_role
      using (true)
      with check (id > 0);
    `);
    const result = await client.query(`
      select oid
      from pg_catalog.pg_policy
      where polname = 'test_policy'
    `);
    const policyOid = result.rows[0].oid;
    const policyId = buildGlobalId("Policy", policyOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          policy(id: $id) {
            id
            oid
            name
            roles
            command
            usingExpr
            withCheck
          }
        }
      `,
      { id: policyId },
      client
    );

    expect(data).toMatchObject({
      policy: expect.objectContaining({
        id: policyId,
        oid: policyOid,
        name: "test_policy",
        roles: ["test_role"],
        command: "UPDATE",
        usingExpr: "true",
        withCheck: "(id > 0)",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Policy by Schema Name and Policy Name
  // =====================================
  it("fetches a specific policy by schema name and policy name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query("create role test_role;");
    await client.query(`
      create policy test_policy
      on test_schema.test_table
      for all
      to test_role
      using (true)
      with check (id > 0);
    `);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          policy(schemaName: "test_schema", name: "test_policy") {
            id
            oid
            name
            roles
            command
            usingExpr
            withCheck
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      policy: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_policy",
        roles: ["test_role"],
        command: "ALL",
        usingExpr: "true",
        withCheck: "(id > 0)",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Type by OID
  // =====================================
  it("fetches a specific type by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_type as (id int);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_type'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: typeOid,
        name: "test_type",
        kind: "COMPOSITE",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Type by ID
  // =====================================
  it("fetches a specific type by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_type as (id int);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_type'
    `);
    const typeOid = result.rows[0].oid;
    const typeId = buildGlobalId("PgType", typeOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          type(id: $id) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { id: typeId },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: typeId,
        oid: typeOid,
        name: "test_type",
        kind: "COMPOSITE",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Type by Schema Name and Type Name
  // =====================================
  it("fetches a specific type by schema name and type name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_type as (id int);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          type(schemaName: "test_schema", name: "test_type") {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_type",
        kind: "COMPOSITE",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Enum Type
  // =====================================
  it("fetches a specific enum type", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_enum as enum ('a', 'b');");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_enum'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
            ... on EnumType {
              enumVariants
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: typeOid,
        name: "test_enum",
        kind: "ENUM",
        enumVariants: ["a", "b"],
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Domain Type
  // =====================================
  it("fetches a specific domain type", async () => {
    await client.query("create schema test_schema;");
    await client.query("create domain test_schema.test_domain as int;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_domain'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
            ... on DomainType {
              baseType {
                ... on PgTypeInterface {
                  name
                }
              }
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: typeOid,
        name: "test_domain",
        kind: "DOMAIN",
        baseType: expect.objectContaining({
          name: "int4",
        }),
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Array Type
  // =====================================
  it("fetches a specific array type", async () => {
    await client.query("create schema test_schema;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = '_int2'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
            ... on ArrayType {
              elementType {
                ... on PgTypeInterface {
                  name
                }
              }
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: typeOid,
        name: "_int2",
        kind: "ARRAY",
        elementType: expect.objectContaining({
          name: "int2",
        }),
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Composite Type
  // =====================================
  it("fetches a specific composite type", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_composite as (id int, name text);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_composite'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
            ... on CompositeType {
              fields {
                name
                type {
                  ... on PgTypeInterface {
                   name
                  }
                }
                notNull
              }
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: typeOid,
        name: "test_composite",
        kind: "COMPOSITE",
        fields: [
          expect.objectContaining({
            name: "id",
            type: expect.objectContaining({
              name: "int4",
            }),
            notNull: false,
          }),
          expect.objectContaining({
            name: "name",
            type: expect.objectContaining({
              name: "text",
            }),
            notNull: false,
          }),
        ],
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Scalar Type
  // =====================================
  it("fetches a specific scalar type", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_scalar as enum ('a', 'b');");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_scalar'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: typeOid,
        name: "test_scalar",
        kind: "ENUM",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Unknown Type
  // =====================================
  it("fetches a specific unknown type", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_unknown as (id int);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_unknown'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: typeOid,
        name: "test_unknown",
        kind: "COMPOSITE",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Scalar Type by OID
  // =====================================
  it("fetches a specific scalar type by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_scalar as enum ('a', 'b');");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_scalar'
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data).toMatchObject({
      type: expect.any(Object),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Scalar Type by ID
  // =====================================
  it("fetches a specific scalar type by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_scalar as enum ('a', 'b');");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_scalar'
    `);
    const typeOid = result.rows[0].oid;
    const typeId = buildGlobalId("PgType", typeOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          type(id: $id) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { id: typeId },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: typeId,
        oid: typeOid,
        name: "test_scalar",
        kind: "ENUM",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Scalar Type by Schema Name and Type Name
  // =====================================
  it("fetches a specific scalar type by schema name and type name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_scalar as enum ('a', 'b');");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          type(schemaName: "test_schema", name: "test_scalar") {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_scalar",
        kind: "ENUM",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Array Type by OID
  // =====================================
  it("fetches a specific array type by oid", async () => {
    await client.query("create schema test_schema;");
    const result = await client.query(`
      select t.oid, t.typname
      from pg_type t
      join pg_namespace n on t.typnamespace = n.oid
      where t.typname = '_int4'
      and n.nspname = 'pg_catalog';
    `);
    const typeOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
            ... on ArrayType {
              elementType {
                ... on PgTypeInterface {
                  name
                }
              }
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );

    expect(data?.type).toMatchObject({
      id: expect.any(String),
      oid: typeOid,
      name: "_int4",
      kind: "ARRAY",
      elementType: {
        name: "int4"
      }
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Array Type by ID
  // =====================================
  it("fetches a specific array type by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_array as (id int);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_array'
    `);
    const typeOid = result.rows[0].oid;
    const typeId = buildGlobalId("PgType", typeOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          type(id: $id) {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { id: typeId },
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: typeId,
        oid: typeOid,
        name: "test_array",
        kind: "COMPOSITE",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Array Type by Schema Name and Type Name
  // =====================================
  it("fetches a specific array type by schema name and type name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_array as (id int);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          type(schemaName: "test_schema", name: "test_array") {
            ... on PgTypeInterface {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      type: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: "test_array",
        kind: "COMPOSITE",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Order Schemas by Name
  // =====================================
  it("orders schemas by name", async () => {
    await client.query("drop schema public cascade;");
    await client.query("create schema schema_a;");
    await client.query("create schema schema_b;");
    await client.query("create schema schema_c;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          database {
            schemas(orderBy: { field: NAME, direction: ASC }) {
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      database: {
        schemas: {
          nodes: [
            { name: "schema_a" },
            { name: "schema_b" },
            { name: "schema_c" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Order Tables by Name
  // =====================================
  it("orders tables by name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.table_a (id serial primary key);");
    await client.query("create table test_schema.table_b (id serial primary key);");
    await client.query("create table test_schema.table_c (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            tables(orderBy: { field: NAME, direction: ASC }) {
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      schema: {
        tables: {
          nodes: [
            { name: "table_a" },
            { name: "table_b" },
            { name: "table_c" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Order Schemas by oid
  // =====================================
  it("orders schemas by name", async () => {
    await client.query("drop schema public cascade;");
    await client.query("create schema schema_c;");
    await client.query("create schema schema_b;");
    await client.query("create schema schema_a;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          database {
            schemas(orderBy: { field: OID, direction: ASC }) {
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      database: {
        schemas: {
          nodes: [
            { name: "schema_c" },
            { name: "schema_b" },
            { name: "schema_a" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Order Tables by OID
  // =====================================
  it("orders tables by name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.table_b (id serial primary key);");
    await client.query("create table test_schema.table_c (id serial primary key);");
    await client.query("create table test_schema.table_a (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            tables(orderBy: { field: OID, direction: ASC }) {
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      schema: {
        tables: {
          nodes: [
            { name: "table_b" },
            { name: "table_c" },
            { name: "table_a" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Order Views by Name
  // =====================================
  it("orders views by name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.view_a as select 1 as id;");
    await client.query("create view test_schema.view_b as select 1 as id;");
    await client.query("create view test_schema.view_c as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            views {
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      schema: {
        views: {
          nodes: [
            { name: "view_a" },
            { name: "view_b" },
            { name: "view_c" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Order Materialized Views by Name
  // =====================================
  it("orders materialized views by name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.matview_a as select 1 as id;");
    await client.query("create materialized view test_schema.matview_b as select 1 as id;");
    await client.query("create materialized view test_schema.matview_c as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            materializedViews {
              nodes {
                name
                isPopulated
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      schema: {
        materializedViews: {
          nodes: [
            { name: "matview_a" },
            { name: "matview_b" },
            { name: "matview_c" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Database by ID
  // =====================================
  it("fetches a database by ID", async () => {
    const result = await client.query(`
      select oid
      from pg_catalog.pg_database
      where datname = current_database()
    `);
    const databaseOid = result.rows[0].oid;
    
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Database {
              id
              oid
              name
            }
          }
        }
      `,
      { id: buildGlobalId("Database", databaseOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: databaseOid,
        name: "postgres",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Schema by ID
  // =====================================
  it("fetches a schema by ID", async () => {
    await client.query("create schema test_schema;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_namespace
      where nspname = 'test_schema'
    `);
    const schemaOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Schema {
              id
              oid
              name
            }
          }
        }
      `,
      { id: buildGlobalId("Schema", schemaOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: schemaOid,
        name: "test_schema",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Table by ID
  // =====================================
  it("fetches a table by ID", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_table'
    `);
    const tableOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Table {
              id
              oid
              name
            }
          }
        }
      `,
      { id: buildGlobalId("Table", tableOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: tableOid,
        name: "test_table",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a View by ID
  // =====================================
  it("fetches a view by ID", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_view'
    `);
    const viewOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on View {
              id
              oid
              name
            }
          }
        }
      `,
      { id: buildGlobalId("View", viewOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: viewOid,
        name: "test_view",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Materialized View by ID
  // =====================================
  it("fetches a materialized view by ID", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_matview'
    `);
    const matviewOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on MaterializedView {
              id
              oid
              name
            }
          }
        }
      `,
      { id: buildGlobalId("MaterializedView", matviewOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: matviewOid,
        name: "test_matview",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch an Index by ID
  // =====================================
  it("fetches an index by ID", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query("create index test_index on test_schema.test_table (id);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_class
      where relname = 'test_index'
    `);
    const indexOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Index {
              id
              oid
              name
            }
          }
        }
      `,
      { id: buildGlobalId("Index", indexOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: indexOid,
        name: "test_index",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Trigger by ID
  // =====================================
  it("fetches a trigger by ID", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query(`
      create or replace function test_function()
      returns trigger as $$
      begin
        new.id := new.id + 1;
        return new;
      end;
      $$ language plpgsql;
    `);
    await client.query(`
      create trigger test_trigger
      before insert on test_schema.test_table
      for each row
      execute function test_function();
    `);
    const result = await client.query(`
      select oid
      from pg_catalog.pg_trigger
      where tgname = 'test_trigger'
    `);
    const triggerOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Trigger {
              id
              oid
              name
              table {
                name
              }
            }
          }
        }
      `,
      { id: buildGlobalId("Trigger", triggerOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: triggerOid,
        name: "test_trigger",
        table: {
          name: "test_table",
        }
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Policy by ID
  // =====================================
  it("fetches a policy by ID", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query("create role test_role;");
    await client.query(`
      create policy test_policy
      on test_schema.test_table
      for select
      to test_role
      using (true);
    `);
    const result = await client.query(`
      select oid
      from pg_catalog.pg_policy
      where polname = 'test_policy'
    `);
    const policyOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Policy {
              id
              oid
              name
              table {
                name
              }
            }
          }
        }
      `,
      { id: buildGlobalId("Policy", policyOid) },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: expect.any(String),
        oid: policyOid,
        name: "test_policy",
        table: {
          name: "test_table",
        }
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Connection Fields: Schema
  // =====================================
  it("returns correct connection fields for schemas", async () => {
    await client.query("create schema test_schema1;");
    await client.query("create schema test_schema2;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          database {
            schemas(first: 2) {
              edges {
                node {
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.database?.schemas).toMatchObject({
      edges: [
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        },
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        }
      ],
      pageInfo: {
        hasNextPage: expect.any(Boolean),
        endCursor: expect.any(String)
      },
      nodes: [
        { name: expect.any(String) },
        { name: expect.any(String) }
      ]
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Connection Fields: Tables
  // =====================================
  it("returns correct connection fields for tables", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.table1 (id serial primary key);");
    await client.query("create table test_schema.table2 (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            tables(first: 2) {
              edges {
                node {
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.schema?.tables).toMatchObject({
      edges: [
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        },
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        }
      ],
      pageInfo: {
        hasNextPage: expect.any(Boolean),
        endCursor: expect.any(String)
      },
      nodes: [
        { name: expect.any(String) },
        { name: expect.any(String) }
      ]
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Connection Fields: Columns
  // =====================================
  it("returns correct connection fields for columns", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key, name text);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            columns {
              edges {
                node {
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.table?.columns).toMatchObject({
      edges: [
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        },
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        }
      ],
      pageInfo: {
        hasNextPage: expect.any(Boolean),
        endCursor: expect.any(String)
      },
      nodes: [
        { name: expect.any(String) },
        { name: expect.any(String) }
      ]
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Connection Fields: Views
  // =====================================
  it("returns correct connection fields for views", async () => {
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.view1 as select 1 as id;");
    await client.query("create view test_schema.view2 as select 2 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            views {
              edges {
                node {
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.schema?.views).toMatchObject({
      edges: [
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        },
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        }
      ],
      pageInfo: {
        hasNextPage: expect.any(Boolean),
        endCursor: expect.any(String)
      },
      nodes: [
        { name: expect.any(String) },
        { name: expect.any(String) }
      ]
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Connection Fields: Materialized Views
  // =====================================
  it("returns correct connection fields for materialized views", async () => {
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.matview1 as select 1 as id;");
    await client.query("create materialized view test_schema.matview2 as select 2 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            materializedViews {
              edges {
                node {
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.schema?.materializedViews).toMatchObject({
      edges: [
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        },
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        }
      ],
      pageInfo: {
        hasNextPage: expect.any(Boolean),
        endCursor: expect.any(String)
      },
      nodes: [
        { name: expect.any(String) },
        { name: expect.any(String) }
      ]
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Connection Fields: Indexes
  // =====================================
  it("returns correct connection fields for indexes", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key, name text);");
    await client.query("create index idx1 on test_schema.test_table (name);");
    await client.query("create index idx2 on test_schema.test_table (id);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            indexes {
              edges {
                node {
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.table?.indexes).toMatchObject({
      edges: expect.arrayContaining([
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        }
      ]),
      pageInfo: {
        hasNextPage: expect.any(Boolean),
        endCursor: expect.any(String)
      },
      nodes: expect.arrayContaining([
        { name: expect.any(String) }
      ])
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Connection Fields: Policies
  // =====================================
  it("returns correct connection fields for policies", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query("alter table test_schema.test_table enable row level security;");
    await client.query("create policy policy1 on test_schema.test_table for select using (true);");
    await client.query("create policy policy2 on test_schema.test_table for insert with check (true);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            policies {
              edges {
                node {
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.table?.policies).toMatchObject({
      edges: [
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        },
        { 
          node: { name: expect.any(String) },
          cursor: expect.any(String)
        }
      ],
      pageInfo: {
        hasNextPage: expect.any(Boolean),
        endCursor: expect.any(String)
      },
      nodes: [
        { name: expect.any(String) },
        { name: expect.any(String) }
      ]
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Row Level Security Status
  // =====================================
  it("returns correct RLS status for tables", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.table_with_rls (id serial primary key);");
    await client.query("create table test_schema.table_without_rls (id serial primary key);");
    await client.query("alter table test_schema.table_with_rls enable row level security;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table_with_rls: table(schemaName: "test_schema", name: "table_with_rls") {
            name
            rowLevelSecurityEnabled
          }
          table_without_rls: table(schemaName: "test_schema", name: "table_without_rls") {
            name
            rowLevelSecurityEnabled
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      table_with_rls: {
        name: "table_with_rls",
        rowLevelSecurityEnabled: true
      },
      table_without_rls: {
        name: "table_without_rls", 
        rowLevelSecurityEnabled: false
      }
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Table returning null
  // =====================================
  it("table returning null", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table {
            name
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      table: null
    });
  });

  // =====================================
  // TEST: View returning null
  // =====================================
  it("view returning null", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          view {
            name
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      view: null
    });
  });

  // =====================================
  // TEST: Schema returning null
  // =====================================
  it("schema returning null", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema {
            name
          }
        }
      `,
      { },
      client
    );

    expect(data).toMatchObject({
      schema: null
    });
  });

  // =====================================
  // TEST: Materialized View returning null
  // =====================================
  it("materialized view returning null", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          materializedView {
            name
          }
        }
      `,
      { },
      client
    );

    expect(data).toMatchObject({
      materializedView: null
    });
  });

  // =====================================
  // TEST: Policy returning null
  // =====================================
  it("policy returning null", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          policy {
            name
          }
        }
      `,
      { },
      client
    );

    expect(data).toMatchObject({
      policy: null
    });
  });

  // =====================================
  // TEST: Index returning null
  // =====================================
  it("index returning null", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          index {
            name
          }
        }
      `,
      { },
      client
    );

    expect(data).toMatchObject({
      index: null
    });
  });

  // =====================================
  // TEST: Trigger returning null
  // =====================================
  it("trigger returning null", async () => {
    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          trigger {
            name
          }
        }
      `,
      { },
      client
    );

    expect(data).toMatchObject({
      trigger: null
    });
  });

  // =====================================
  // TEST: Fetch a Specific Role by ID
  // =====================================
  it("fetches a specific role by id", async () => {
    const result = await client.query(`
      select oid, rolname
      from pg_catalog.pg_roles
      where rolname = 'postgres'
    `);
    const roleOid = result.rows[0].oid;
    const roleId = buildGlobalId("Role", roleOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          role(id: $id) {
            id
            oid
            name
          }
        }
      `,
      { id: roleId },
      client
    );

    expect(data).toMatchObject({
      role: expect.objectContaining({
        id: roleId,
        oid: roleOid,
        name: "postgres",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Role by OID
  // =====================================
  it("fetches a specific role by oid", async () => {
    const result = await client.query(`
      select oid, rolname
      from pg_catalog.pg_roles
      where rolname = 'postgres'
    `);
    const roleOid = result.rows[0].oid;

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          role(oid: $oid) {
            id
            oid
            name
          }
        }
      `,
      { oid: roleOid },
      client
    );

    expect(data).toMatchObject({
      role: expect.objectContaining({
        id: expect.any(String),
        oid: roleOid,
        name: "postgres",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Specific Role by Name
  // =====================================
  it("fetches a specific role by name", async () => {
    const roleName = "postgres";

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($name: String!) {
          role(name: $name) {
            id
            oid
            name
          }
        }
      `,
      { name: roleName },
      client
    );

    expect(data).toMatchObject({
      role: expect.objectContaining({
        id: expect.any(String),
        oid: expect.any(Number),
        name: roleName,
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a PgType node by id
  // =====================================
  it.skip("fetches a PgType node by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create type test_schema.test_type as (id int);");
    const result = await client.query(`
      select oid
      from pg_catalog.pg_type
      where typname = 'test_type'
    `);
    const typeOid = result.rows[0].oid;
    const typeId = buildGlobalId("PgType", typeOid);
    
    console.log("Type query result:", result.rows[0]);
    console.log("Test type OID:", typeOid, "Type ID:", typeId);

    // First try the direct type query
    const directResult = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          type(oid: $oid) {
            ... on CompositeType {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { oid: typeOid },
      client
    );
    
    console.log("Direct type query result:", directResult.data);
    
    // Then try the node query
    const nodeResult = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on CompositeType {
              id
              oid
              name
              kind
            }
          }
        }
      `,
      { id: typeId },
      client
    );
    
    const { data, errors } = nodeResult;
    console.log("Node query errors:", errors);
    console.log("Node query data:", data);

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: typeId,
        oid: typeOid,
        name: "test_type",
        kind: "COMPOSITE",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Column node by ID
  // =====================================
  it("fetches a Column node by id", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (some_unique_name serial primary key, name text);");
    const result = await client.query(`
      select attrelid
      from pg_catalog.pg_attribute
      where attname = 'some_unique_name'
    `);
    const columnOid = result.rows[0].attrelid;
    const columnId = buildGlobalId("Column", columnOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Column {
              id
              name
              attnum
              atttypid
              table {
                name
              }
              type {
                ... on PgTypeInterface {
                  name
                }
              }
            }
          }
        }
      `,
      { id: columnId },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: columnId,
        name: "some_unique_name",
        attnum: 1,
        atttypid: expect.any(Number),
        table: expect.objectContaining({
          name: "test_table",
        }),
        type: expect.objectContaining({
          name: "int4",
        }),
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch a Role node by ID
  // =====================================
  it("fetches a Role node by id", async () => {
    const result = await client.query(`
      select oid, rolname
      from pg_catalog.pg_roles
      where rolname = 'postgres'
    `);
    const roleOid = result.rows[0].oid;
    const roleId = buildGlobalId("Role", roleOid);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            ... on Role {
              id
              oid
              name
            }
          }
        }
      `,
      { id: roleId },
      client
    );

    expect(data).toMatchObject({
      node: expect.objectContaining({
        id: roleId,
        oid: roleOid,
        name: "postgres",
      }),
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Database Privileges
  // =====================================
  it("fetches database privileges for a role", async () => {
    await client.query("revoke connect on database postgres from public;");
    await client.query("create role test_role_no_connect;");
    await client.query("revoke connect on database postgres from test_role_no_connect;");
    await client.query("create role test_role_connect;");
    await client.query("grant connect on database postgres to test_role_connect;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          database {
            no_connect: privileges(roleName: "test_role_no_connect") {
              role {
                name
              }
              connect
            }
            connect: privileges(roleName: "test_role_connect") {
              role {
                name
              }
              connect
            }
          }
        }
      `,
      { },
      client
    );

    expect(data).toMatchObject({
      database: {
        no_connect: {
          role: {
            name: "test_role_no_connect",
          },
          connect: false,
        },
        connect: {
          role: {
            name: "test_role_connect",
          },
          connect: true,
        },
      },
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch Schema Privileges
  // =====================================
  it("fetches schema privileges for a role", async () => {
    await client.query("create role test_role with login;");
    await client.query("create schema test_schema;");
    await client.query("revoke usage on schema test_schema from test_role;");
    await client.query("revoke usage on schema test_schema from public;");


    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($roleName: String!) {
          schema(schemaName: "test_schema") {
            privileges(roleName: $roleName) {
              role {
                name
              }
              usage
            }
          }
        }
      `,
      { roleName: "test_role" },
      client
    );

    expect(data).toMatchObject({
      schema: {
        privileges: {
          role: {
            name: "test_role",
          },
          usage: false, // Assuming the role does not have usage privilege by default
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Table Privileges
  // =====================================
  it("fetches table privileges for a role", async () => {
    await client.query("create role test_role with login;");
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($roleName: String!) {
          table(schemaName: "test_schema", name: "test_table") {
            privileges(roleName: $roleName) {
              role {
                name
              }
              select
              insert
              update
              delete
            }
          }
        }
      `,
      { roleName: "test_role" },
      client
    );

    expect(data).toMatchObject({
      table: {
        privileges: {
          role: {
            name: "test_role",
          },
          select: false, // Assuming the role does not have select privilege by default
          insert: false, // Assuming the role does not have insert privilege by default
          update: false, // Assuming the role does not have update privilege by default
          delete: false, // Assuming the role does not have delete privilege by default
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch View Privileges
  // =====================================
  it("fetches view privileges for a role", async () => {
    await client.query("create role test_role with login;");
    await client.query("create schema test_schema;");
    await client.query("create view test_schema.test_view as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($roleName: String!) {
          view(schemaName: "test_schema", name: "test_view") {
            privileges(roleName: $roleName) {
              role {
                name
              }
              select
            }
          }
        }
      `,
      { roleName: "test_role" },
      client
    );

    expect(data).toMatchObject({
      view: {
        privileges: {
          role: {
            name: "test_role",
          },
          select: false, // Assuming the role does not have select privilege by default
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Materialized View Privileges
  // =====================================
  it("fetches materialized view privileges for a role", async () => {
    await client.query("create role test_role with login;");
    await client.query("create schema test_schema;");
    await client.query("create materialized view test_schema.test_matview as select 1 as id;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($roleName: String!) {
          materializedView(schemaName: "test_schema", name: "test_matview") {
            privileges(roleName: $roleName) {
              role {
                name
              }
              select
            }
          }
        }
      `,
      { roleName: "test_role" },
      client
    );

    expect(data).toMatchObject({
      materializedView: {
        privileges: {
          role: {
            name: "test_role",
          },
          select: false, // Assuming the role does not have select privilege by default
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Column Privileges
  // =====================================
  it("fetches column privileges for a role", async () => {
    await client.query("create role test_role with login;");
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key, name text);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($roleName: String!) {
          table(schemaName: "test_schema", name: "test_table") {
            columns {
              nodes {
                name
                privileges(roleName: $roleName) {
                  role {
                    name
                  }
                  select
                  insert
                  update
                }
              }
            }
          }
        }
      `,
      { roleName: "test_role" },
      client
    );

    expect(data).toMatchObject({
      table: {
        columns: {
          nodes: [
            {
              name: "id",
              privileges: {
                role: {
                  name: "test_role",
                },
                select: false, // Assuming the role does not have select privilege by default
                insert: false, // Assuming the role does not have insert privilege by default
                update: false, // Assuming the role does not have update privilege by default
              },
            },
            {
              name: "name",
              privileges: {
                role: {
                  name: "test_role",
                },
                select: false, // Assuming the role does not have select privilege by default
                insert: false, // Assuming the role does not have insert privilege by default
                update: false, // Assuming the role does not have update privilege by default
              },
            },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific Table by Schema Name and Table Name (Non-existent)
  // =====================================
  it("returns null when querying a non-existent table by schema name and table name", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "non_existent") {
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
      table: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific View by Schema Name and Table Name (Non-existent)
  // =====================================
  it("returns null when querying a non-existent view by schema name and view name", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          view(schemaName: "test_schema", name: "non_existent") {
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
      view: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific Materialized View by Schema Name and Matview Name (Non-existent)
  // =====================================
  it("returns null when querying a non-existent materialized view by schema name and mat view name", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          materializedView(schemaName: "test_schema", name: "non_existent") {
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
      materializedView: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific Index by Schema Name and Index Name (Non-existent)
  // =====================================
  it("returns null when querying a non-existent index by schema name and index name", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          index(schemaName: "test_schema", name: "non_existent") {
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
      index: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific Policy by Schema Name and Policy Name (Non-existent)
  // =====================================
  it("returns null when querying a non-existent policy by schema name and policy", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          policy(schemaName: "test_schema", name: "non_existent") {
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
      policy: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific Trigger by Schema Name and Trigger Name (Non-existent)
  // =====================================
  it("returns null when querying a non-existent trigger by schema name and name", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          trigger(schemaName: "test_schema", name: "non_existent") {
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
      trigger: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific Type by Schema Name and Name (Non-existent)
  // =====================================
  it("returns null when querying a non-existent type by schema name and name", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          type(schemaName: "test_schema", name: "non_existent") {
            ... on PgTypeInterface {
              id
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      type: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific Type no args (No match)
  // =====================================
  it("returns null when querying a type without filtering", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          type {
            ... on PgTypeInterface {
              id
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      type: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Specific role no args (No match)
  // =====================================
  it("returns null when querying a role without filtering", async () => {

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          role{
            id
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      role: null,
    });
    expect(errors).toBeUndefined();
  });


  // =====================================
  // TEST: Fetch a Node with nonsense entity type returns null
  // =====================================
  it("returns null when fetching nonsense entity type by Id", async () => {
    const schemaId = buildGlobalId("DoesNotExist", 1);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($id: ID!) {
          node(id: $id) {
            id
          }
        }
      `,
      { id: schemaId },
      client
    );

    expect(data).toMatchObject({
      node: null,
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Fetch Triggers for a Specific Table
  // =====================================
  it("fetches triggers for a specific table", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query(`
      create or replace function test_function()
      returns trigger as $$
      begin
        new.id := new.id + 1;
        return new;
      end;
      $$ language plpgsql;
    `);
    await client.query(`
      create trigger test_trigger
      before insert on test_schema.test_table
      for each row
      execute function test_function();
    `);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            triggers {
              edges {
                node {
                  id
                  name
                  oid
                }
                cursor
              }
              nodes {
                id
                name
                oid
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      table: {
        triggers: {
          edges: [
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "test_trigger",
                oid: expect.any(Number),
              }),
              cursor: expect.any(String),
            }),
          ],
          nodes: [
            expect.objectContaining({
              id: expect.any(String),
              name: "test_trigger",
              oid: expect.any(Number),
            }),
          ],
          pageInfo: {
            hasNextPage: false,
            endCursor: expect.any(String),
          },
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Sort Schemas by Name
  // =====================================
  it("sorts schemas by name", async () => {
    await client.query("create schema schema_a;");
    await client.query("create schema schema_b;");
    await client.query("create schema schema_c;");
    await client.query("drop schema if exists public;");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          database {
            schemas(orderBy: { field: NAME, direction: DESC }) {
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      database: {
        schemas: {
          nodes: [
            { name: "schema_c" },
            { name: "schema_b" },
            { name: "schema_a" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Sort Tables by Name
  // =====================================
  it("sorts tables by name", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.table_c (id serial primary key);");
    await client.query("create table test_schema.table_b (id serial primary key);");
    await client.query("create table test_schema.table_a (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            tables(orderBy: { field: NAME, direction: ASC }) {
              nodes {
                name
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(data).toMatchObject({
      schema: {
        tables: {
          nodes: [
            { name: "table_a" },
            { name: "table_b" },
            { name: "table_c" },
          ],
        },
      },
    });
    expect(errors).toBeUndefined();
  });

  // =====================================
  // TEST: Paginate Tables using first and after
  // =====================================
  it("paginates tables using first and after", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.table_a (id serial primary key);");
    await client.query("create table test_schema.table_b (id serial primary key);");
    await client.query("create table test_schema.table_c (id serial primary key);");

    const { data: firstPageData, errors: firstPageErrors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            tables(first: 2) {
              edges {
                node {
                  id
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(firstPageErrors).toBeUndefined();
    expect(firstPageData).toMatchObject({
      schema: {
        tables: {
          edges: [
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "table_a",
              }),
              cursor: expect.any(String),
            }),
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "table_b",
              }),
              cursor: expect.any(String),
            }),
          ],
          pageInfo: {
            hasNextPage: true,
            endCursor: expect.any(String),
          },
        },
      },
    });

    const endCursor = (firstPageData as { schema: { tables: { pageInfo: { endCursor: string } } } }).schema.tables.pageInfo.endCursor;

    const { data: secondPageData, errors: secondPageErrors } = await executeTestQuery(
      testServer,
      `
        query {
          schema(schemaName: "test_schema") {
            tables(first: 2, after: "${endCursor}") {
              edges {
                node {
                  id
                  name
                }
                cursor
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect(secondPageErrors).toBeUndefined();
    expect(secondPageData).toMatchObject({
      schema: {
        tables: {
          edges: [
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "table_c",
              }),
              cursor: expect.any(String),
            }),
          ],
          pageInfo: {
            hasNextPage: false,
            endCursor: expect.any(String),
          },
        },
      },
    });
  });

  // =====================================
  // TEST: Policy command switch mapping
  // =====================================
  it("fetches a specific policy by oid", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");
    await client.query("create role test_role;");
    await client.query(`create policy test_policy_select on test_schema.test_table for select to test_role using (true);`);
    await client.query(`create policy test_policy_insert on test_schema.test_table for insert to test_role with check (true);`);
    await client.query(`create policy test_policy_update on test_schema.test_table for update to test_role using (true);`);
    await client.query(`create policy test_policy_delete on test_schema.test_table for delete to test_role using (true);`);
    await client.query(`create policy test_policy_all on test_schema.test_table for all to test_role using (true) with check (true);`);

    // Test each policy and its corresponding command
    const policies = [
      { name: 'test_policy_select', expectedCommand: 'SELECT' },
      { name: 'test_policy_insert', expectedCommand: 'INSERT' },
      { name: 'test_policy_update', expectedCommand: 'UPDATE' },
      { name: 'test_policy_delete', expectedCommand: 'DELETE' },
      { name: 'test_policy_all', expectedCommand: 'ALL' }
    ];

    for (const policy of policies) {
      const result = await client.query(`
      select oid
      from pg_catalog.pg_policy
      where polname = $1
      `, [policy.name]);
      const policyOid = result.rows[0].oid;

      const { data, errors } = await executeTestQuery(
      testServer,
      `
        query ($oid: Int!) {
          policy(oid: $oid) {
            command
          }
        }
      `,
      { oid: policyOid },
      client
      );

      expect(data).toMatchObject({
      policy: expect.objectContaining({
        command: policy.expectedCommand,
      }),
      });
      expect(errors).toBeUndefined();
    }
    });

  // =====================================
  // TEST: Foreign Keys
  // =====================================
  it("fetches foreign keys for a table", async () => {
    await client.query("create schema test_schema;");
    await client.query(`
      create table test_schema.parent (
        id serial primary key
      );
    `);
    await client.query(`
      create table test_schema.child (
        id serial primary key,
        parent_id integer references test_schema.parent(id) on delete cascade
      );
    `);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "child") {
            foreignKeys {
              nodes {
                name
                updateAction
                deleteAction
                columnMappings {
                  referencingColumn {
                    name
                  }
                  referencedColumn {
                    name
                  }
                }
                referencedTable {
                  name
                }
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.table?.foreignKeys?.nodes?.[0]).toMatchObject({
      name: expect.any(String),
      updateAction: "NO_ACTION",
      deleteAction: "CASCADE",
      columnMappings: [{
      referencingColumn: { name: "parent_id" },
      referencedColumn: { name: "id" }
      }],
      referencedTable: { name: "parent" }
    });
    expect(errors).toBeUndefined();
  });

  it("fetches tables referencing a table", async () => {
    await client.query("create schema test_schema;");
    await client.query(`
      create table test_schema.parent (
        id serial primary key
      );
    `);
    await client.query(`
      create table test_schema.child (
        id serial primary key,
        parent_id integer references test_schema.parent(id)
      );
    `);

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "parent") {
            referencedBy {
              nodes {
                name
                table {
                  name
                }
              }
            }
          }
        }
      `,
      {},
      client
    );

    expect((data as any)?.table?.referencedBy?.nodes[0]).toMatchObject({
      name: expect.any(String),
      table: { name: "child" }
    });
    expect(errors).toBeUndefined();
  });

});