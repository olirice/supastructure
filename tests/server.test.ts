import { ApolloServer } from "@apollo/server";
import { Pool, PoolClient } from "pg";
import fs from "fs";
import { gql } from "graphql-tag";
import { resolvers } from "../src/resolvers.js";
import { context, ReqContext, DbConfig } from "../src/context.js";
import { dbConfig } from "../src/index.js";
import { buildGlobalId } from "../src/generic.js";

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
  // TEST: Fetch Indexes for a Specific Table
  // =====================================
  it("fetches indexes for a specific table", async () => {
    await client.query("create schema test_schema;");
    await client.query("create table test_schema.test_table (id serial primary key);");

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            indexes {
              edges {
                node {
                  id
                  name
                  accessMethod
                  definition
                }
                cursor
              }
              nodes {
                id
                name
                accessMethod
                definition
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
        indexes: {
          edges: [
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "test_table_pkey",
                accessMethod: "btree",
                definition: expect.any(String),
              }),
              cursor: expect.any(String),
            }),
          ],
          nodes: [
            expect.objectContaining({
              id: expect.any(String),
              name: "test_table_pkey",
              accessMethod: "btree",
              definition: expect.any(String),
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
  // TEST: Fetch Policies for a Specific Table
  // =====================================
  it("fetches policies for a specific table", async () => {
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

    const { data, errors } = await executeTestQuery(
      testServer,
      `
        query {
          table(schemaName: "test_schema", name: "test_table") {
            policies {
              edges {
                node {
                  id
                  name
                  roles
                  command
                  usingExpr
                  withCheck
                }
                cursor
              }
              nodes {
                id
                name
                roles
                command
                usingExpr
                withCheck
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
        policies: {
          edges: [
            expect.objectContaining({
              node: expect.objectContaining({
                id: expect.any(String),
                name: "test_policy",
                roles: ["test_role"],
                command: "SELECT",
                usingExpr: "true",
                withCheck: null,
              }),
              cursor: expect.any(String),
            }),
          ],
          nodes: [
            expect.objectContaining({
              id: expect.any(String),
              name: "test_policy",
              roles: ["test_role"],
              command: "SELECT",
              usingExpr: "true",
              withCheck: null,
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

});



