import {
  PgDatabase,
  PgNamespace,
  PgClass,
  PgAttribute,
  PgTrigger,
  PgPolicy,
  PgType,
  PgEnum,
  PgIndex,
  PgRole,
  PgAttributeSchema,
  PgDatabaseSchema,
  PgClassSchema,
  PgNamespaceSchema,
  PgPolicySchema,
  PgTriggerSchema,
  PgEnumSchema,
  PgIndexSchema,
  PgRoleSchema,
  PgTypeSchema,
} from "./types.js";
import pg from "pg";

const { Client } = pg;

export interface ReqContext {
  client: pg.Client | pg.PoolClient;

  pg_database: PgDatabase;
  pg_namespaces: PgNamespace[];
  pg_classes: PgClass[];
  pg_attributes: PgAttribute[];
  pg_triggers: PgTrigger[];
  pg_policies: PgPolicy[];
  pg_types: PgType[];
  pg_enums: PgEnum[];
  pg_index: PgIndex[];
  pg_roles: PgRole[];
}

export interface DbConfig {
  user: string;
  host: string;
  database: string;
  password: string;
  port: number;
}

export async function releaseClient(
  client: pg.Client | pg.PoolClient
): Promise<void> {
  if ("end" in client) {
    // If it's a pg.Client instance, close the connection
    await client.end();
  } else {
    // If it's a pg.PoolClient instance, release it back to the pool
    await client.release();
  }
}

export async function context(
  dbConfig: DbConfig,
  existingClient?: pg.Client | pg.PoolClient
): Promise<ReqContext> {
  // existingClient allows us to pass in a connection mid-transaction to enable parallel testing.
  const client = existingClient || new pg.Client(dbConfig);
  if (!existingClient) {
    await client.connect();
  }
  try {
    // database
    const dbRow = await client.query(`
        select oid, datname
        from pg_catalog.pg_database
        where datname = current_database()
      `);
    const pg_database = PgDatabaseSchema.parse(dbRow.rows[0]);

    // namespaces
    const nsRows = await client.query(`
        select oid, nspname, nspowner
        from pg_catalog.pg_namespace
        where nspname not in ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')
        order by nspname asc
      `);
    const pg_namespaces = nsRows.rows.map((r) => PgNamespaceSchema.parse(r));

    // classes
    const classRows = await client.query(`
        select
          c.oid,
          c.relname,
          c.relnamespace,
          c.relkind,
          c.relispopulated,
          n.nspname
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on c.relnamespace = n.oid
        where n.nspname not in ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')
        order by n.nspname, c.relname
      `);
    const pg_classes = classRows.rows.map((r) => PgClassSchema.parse(r));

    // attributes
    const attrRows = await client.query(`
        select
          a.attrelid,
          a.attname,
          a.atttypid,
          a.attnum,
          a.attnotnull,
          n.nspname
        from pg_catalog.pg_attribute a
        join pg_catalog.pg_class c on a.attrelid = c.oid
        join pg_catalog.pg_namespace n on c.relnamespace = n.oid
        where a.attnum >= 1
          and not a.attisdropped
          and n.nspname not in ('pg_toast','pg_catalog','information_schema','pg_temp')
        order by n.nspname, c.relname, a.attnum
      `);
    const pg_attributes = attrRows.rows.map((r) => PgAttributeSchema.parse(r));

    // triggers
    const trigRows = await client.query(`
        select
          t.oid,
          t.tgname,
          t.tgrelid,
          n.nspname
        from pg_catalog.pg_trigger t
        join pg_catalog.pg_class c on t.tgrelid = c.oid
        join pg_catalog.pg_namespace n on c.relnamespace = n.oid
        where not t.tgisinternal
          and n.nspname not in ('pg_toast','pg_catalog','information_schema','pg_temp')
        order by n.nspname, t.tgname
      `);
    const pg_triggers = trigRows.rows.map((r) => PgTriggerSchema.parse(r));

    // policies
    const policyRows = await client.query(`
      select
        p.oid,
        p.polname,
        p.polrelid,
        p.polcmd,
        coalesce(array_agg(r.rolname::text) filter (where r.rolname is not null), '{}') as polroles,
        pg_get_expr(p.polqual, p.polrelid) as polqual,
        pg_get_expr(p.polwithcheck, p.polrelid) as polwithcheck,
        n.nspname
      from pg_catalog.pg_policy p
      join pg_catalog.pg_class c on c.oid = p.polrelid
      join pg_catalog.pg_namespace n on n.oid = c.relnamespace
      left join pg_catalog.pg_roles r on r.oid = any(p.polroles)
      group by
        p.oid,
        p.polname,
        p.polrelid,
        p.polcmd,
        p.polqual,
        p.polwithcheck,
        n.nspname
      order by n.nspname, p.polname
      `);
    const pg_policies = policyRows.rows.map((r) => PgPolicySchema.parse(r));

    // types
    const typeRows = await client.query(`
      select
        t.oid,
        t.typname,
        t.typtype,
        t.typbasetype,
        t.typelem,
        t.typrelid,
        n.nspname
      from pg_catalog.pg_type t
      join pg_catalog.pg_namespace n on t.typnamespace = n.oid
      order by n.nspname, t.typname
      `);
    const pg_types = typeRows.rows.map((r) => PgTypeSchema.parse(r));

    // enums
    const enumRows = await client.query(`
      select 
        e.enumtypid,
        array_agg(e.enumlabel::text order by e.enumsortorder) as enumlabels,
        n.nspname
      from pg_catalog.pg_enum e
      join pg_catalog.pg_type t on t.oid = e.enumtypid
      join pg_catalog.pg_namespace n on n.oid = t.typnamespace
      group by e.enumtypid, n.nspname, t.typname
      order by n.nspname, t.typname
    `);
    const pg_enums = enumRows.rows.map((r) => PgEnumSchema.parse(r));

    // index
    const indexRows = await client.query(`
      select
        i.indexrelid,
        i.indrelid,
        i.indkey::text,
        pg_get_indexdef(i.indexrelid) as indexdef,
        am.amname as indexam,
        n.nspname
      from pg_catalog.pg_index i
      join pg_catalog.pg_class c on c.oid = i.indexrelid
      join pg_catalog.pg_am am on c.relam = am.oid
      join pg_catalog.pg_namespace n on c.relnamespace = n.oid
      order by pg_get_indexdef(i.indexrelid)
      `);
    const pg_index = indexRows.rows.map((r) => PgIndexSchema.parse(r));

    // roles
    const roleRows = await client.query(`
        select oid, rolname, rolsuper
        from pg_catalog.pg_roles
      `);
    const pg_roles = roleRows.rows.map((r) => PgRoleSchema.parse(r));

    await releaseClient(client);

    return {
      client,
      pg_database,
      pg_namespaces,
      pg_classes,
      pg_attributes,
      pg_triggers,
      pg_policies,
      pg_types,
      pg_enums,
      pg_index,
      pg_roles,
    };
  } catch (err) {
    console.error("error loading data:", err);
    await releaseClient(client);
    throw err;
  }
}
