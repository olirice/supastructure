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

});
