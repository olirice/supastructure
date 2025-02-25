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
  PgForeignKey,
  PgForeignKeySchema,
} from "./types.js";
import pg from "pg";

const { Client } = pg;

export const queries = {
  // Database queries
  async database(client: pg.Client | pg.PoolClient): Promise<PgDatabase> {
    const dbRow = await client.query(`
      select oid, datname
      from pg_catalog.pg_database
      where datname = current_database()
    `);
    return PgDatabaseSchema.parse(dbRow.rows[0]);
  },

  // Namespace/schema queries
  async namespaces(client: pg.Client | pg.PoolClient): Promise<PgNamespace[]> {
    const nsRows = await client.query(`
      select oid, nspname, nspowner
      from pg_catalog.pg_namespace
      where nspname not in ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')
      order by nspname asc
    `);
    return nsRows.rows.map((r) => PgNamespaceSchema.parse(r));
  },

  async namespaceByOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgNamespace | null> {
    const result = await client.query(`
      select oid, nspname, nspowner
      from pg_catalog.pg_namespace
      where oid = $1
    `, [oid]);
    return result.rows.length ? PgNamespaceSchema.parse(result.rows[0]) : null;
  },

  async namespaceByName(client: pg.Client | pg.PoolClient, name: string): Promise<PgNamespace | null> {
    const result = await client.query(`
      select oid, nspname, nspowner
      from pg_catalog.pg_namespace
      where nspname = $1
    `, [name]);
    return result.rows.length ? PgNamespaceSchema.parse(result.rows[0]) : null;
  },

  // Class/table/view queries
  async classes(client: pg.Client | pg.PoolClient): Promise<PgClass[]> {
    const classRows = await client.query(`
      select
        c.oid,
        c.relname,
        c.relnamespace,
        c.relkind,
        c.relispopulated,
        c.relrowsecurity,
        n.nspname
      from pg_catalog.pg_class c
      join pg_catalog.pg_namespace n on c.relnamespace = n.oid
      where n.nspname not in ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')
      order by n.nspname, c.relname
    `);
    return classRows.rows.map((r) => PgClassSchema.parse(r));
  },

  async classesByNamespace(client: pg.Client | pg.PoolClient, namespaceOid: number): Promise<PgClass[]> {
    const result = await client.query(`
      select
        c.oid,
        c.relname,
        c.relnamespace,
        c.relkind,
        c.relispopulated,
        c.relrowsecurity,
        n.nspname
      from pg_catalog.pg_class c
      join pg_catalog.pg_namespace n on c.relnamespace = n.oid
      where c.relnamespace = $1
      order by c.relname
    `, [namespaceOid]);
    return result.rows.map((r) => PgClassSchema.parse(r));
  },

  async classByOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgClass | null> {
    const result = await client.query(`
      select
        c.oid,
        c.relname,
        c.relnamespace,
        c.relkind,
        c.relispopulated,
        c.relrowsecurity,
        n.nspname
      from pg_catalog.pg_class c
      join pg_catalog.pg_namespace n on c.relnamespace = n.oid
      where c.oid = $1
    `, [oid]);
    return result.rows.length ? PgClassSchema.parse(result.rows[0]) : null;
  },

  async classByNameAndSchema(
    client: pg.Client | pg.PoolClient, 
    schemaName: string, 
    className: string
  ): Promise<PgClass | null> {
    const result = await client.query(`
      select
        c.oid,
        c.relname,
        c.relnamespace,
        c.relkind,
        c.relispopulated,
        c.relrowsecurity,
        n.nspname
      from pg_catalog.pg_class c
      join pg_catalog.pg_namespace n on c.relnamespace = n.oid
      where n.nspname = $1 and c.relname = $2
    `, [schemaName, className]);
    return result.rows.length ? PgClassSchema.parse(result.rows[0]) : null;
  },

  // Attribute/column queries
  async attributes(client: pg.Client | pg.PoolClient): Promise<PgAttribute[]> {
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
    return attrRows.rows.map((r) => PgAttributeSchema.parse(r));
  },

  async attributesByRelation(client: pg.Client | pg.PoolClient, relationOid: number): Promise<PgAttribute[]> {
    const result = await client.query(`
      select
        a.attrelid,
        a.attname,
        a.atttypid,
        a.attnum,
        a.attnotnull
      from pg_catalog.pg_attribute a
      where a.attrelid = $1
        and a.attnum >= 1
        and not a.attisdropped
      order by a.attnum
    `, [relationOid]);
    return result.rows.map((r) => PgAttributeSchema.parse(r));
  },

  // Trigger queries
  async triggers(client: pg.Client | pg.PoolClient): Promise<PgTrigger[]> {
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
    return trigRows.rows.map((r) => PgTriggerSchema.parse(r));
  },

  async triggersByTable(client: pg.Client | pg.PoolClient, tableOid: number): Promise<PgTrigger[]> {
    const result = await client.query(`
      select
        t.oid,
        t.tgname,
        t.tgrelid
      from pg_catalog.pg_trigger t
      where t.tgrelid = $1
        and not t.tgisinternal
      order by t.tgname
    `, [tableOid]);
    return result.rows.map((r) => PgTriggerSchema.parse(r));
  },

  async triggerByOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgTrigger | null> {
    const result = await client.query(`
      select
        t.oid,
        t.tgname,
        t.tgrelid
      from pg_catalog.pg_trigger t
      where t.oid = $1
    `, [oid]);
    return result.rows.length ? PgTriggerSchema.parse(result.rows[0]) : null;
  },

  async triggersByNameAndSchema(
    client: pg.Client | pg.PoolClient,
    schemaName: string,
    triggerName: string
  ): Promise<PgTrigger | null> {
    const result = await client.query(`
      select
        t.oid,
        t.tgname,
        t.tgrelid
      from pg_catalog.pg_trigger t
      join pg_catalog.pg_class c on t.tgrelid = c.oid
      join pg_catalog.pg_namespace n on c.relnamespace = n.oid
      where not t.tgisinternal
        and n.nspname = $1
        and t.tgname = $2
    `, [schemaName, triggerName]);
    return result.rows.length ? PgTriggerSchema.parse(result.rows[0]) : null;
  },

  // Policy queries
  async policies(client: pg.Client | pg.PoolClient): Promise<PgPolicy[]> {
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
    return policyRows.rows.map((r) => PgPolicySchema.parse(r));
  },

  async policiesByTable(client: pg.Client | pg.PoolClient, tableOid: number): Promise<PgPolicy[]> {
    const result = await client.query(`
      select
        p.oid,
        p.polname,
        p.polrelid,
        p.polcmd,
        coalesce(array_agg(r.rolname::text) filter (where r.rolname is not null), '{}') as polroles,
        pg_get_expr(p.polqual, p.polrelid) as polqual,
        pg_get_expr(p.polwithcheck, p.polrelid) as polwithcheck
      from pg_catalog.pg_policy p
      left join pg_catalog.pg_roles r on r.oid = any(p.polroles)
      where p.polrelid = $1
      group by
        p.oid,
        p.polname,
        p.polrelid,
        p.polcmd,
        p.polqual,
        p.polwithcheck
      order by p.polname
    `, [tableOid]);
    return result.rows.map((r) => PgPolicySchema.parse(r));
  },

  // Types queries
  async types(client: pg.Client | pg.PoolClient): Promise<PgType[]> {
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
    return typeRows.rows.map((r) => PgTypeSchema.parse(r));
  },

  async typeByOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgType | null> {
    const result = await client.query(`
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
      where t.oid = $1
    `, [oid]);
    return result.rows.length ? PgTypeSchema.parse(result.rows[0]) : null;
  },

  async typeByNameAndSchema(
    client: pg.Client | pg.PoolClient,
    schemaName: string,
    typeName: string
  ): Promise<PgType | null> {
    const result = await client.query(`
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
      where n.nspname = $1 and t.typname = $2
    `, [schemaName, typeName]);
    return result.rows.length ? PgTypeSchema.parse(result.rows[0]) : null;
  },

  // Enums queries
  async enums(client: pg.Client | pg.PoolClient): Promise<PgEnum[]> {
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
    return enumRows.rows.map((r) => PgEnumSchema.parse(r));
  },

  // Index queries
  async index(client: pg.Client | pg.PoolClient): Promise<PgIndex[]> {
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
    return indexRows.rows.map((r) => PgIndexSchema.parse(r));
  },

  // Roles queries
  async roles(client: pg.Client | pg.PoolClient): Promise<PgRole[]> {
    const roleRows = await client.query(`
      select oid, rolname, rolsuper
      from pg_catalog.pg_roles
    `);
    return roleRows.rows.map((r) => PgRoleSchema.parse(r));
  },

  async roleByOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgRole | null> {
    const result = await client.query(`
      select oid, rolname, rolsuper
      from pg_catalog.pg_roles
      where oid = $1
    `, [oid]);
    return result.rows.length ? PgRoleSchema.parse(result.rows[0]) : null;
  },

  async roleByName(client: pg.Client | pg.PoolClient, name: string): Promise<PgRole | null> {
    const result = await client.query(`
      select oid, rolname, rolsuper
      from pg_catalog.pg_roles
      where rolname = $1
    `, [name]);
    return result.rows.length ? PgRoleSchema.parse(result.rows[0]) : null;
  },

  // Foreign keys queries
  async foreignKeys(client: pg.Client | pg.PoolClient): Promise<PgForeignKey[]> {
    const fkRows = await client.query(`
      select
        c.oid,
        c.conname,
        c.conrelid,
        c.confrelid,
        c.confupdtype,
        c.confdeltype,
        array_agg(a.attnum) as conkey,
        array_agg(cf.attnum) as confkey,
        n.nspname
      from pg_catalog.pg_constraint c
      join pg_catalog.pg_namespace n on n.oid = c.connamespace
      join pg_catalog.pg_attribute a on a.attrelid = c.conrelid
      join pg_catalog.pg_attribute cf on cf.attrelid = c.confrelid
      where c.contype = 'f'
        and a.attnum = any(c.conkey)
        and cf.attnum = any(c.confkey)
        and n.nspname not in ('pg_catalog','information_schema')
      group by c.oid, c.conname, c.conrelid, c.confrelid, c.confupdtype, c.confdeltype, n.nspname
      order by n.nspname, c.conname
    `);
    return fkRows.rows.map((r) => PgForeignKeySchema.parse(r));
  },
};

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
  pg_foreign_keys: PgForeignKey[];
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
  const client = existingClient || new pg.Client(dbConfig);
  if (!existingClient) {
    await client.connect();
  }
  try {
    const pg_database = await queries.database(client);
    const pg_namespaces = await queries.namespaces(client);
    const pg_classes = await queries.classes(client);
    const pg_attributes = await queries.attributes(client);
    const pg_triggers = await queries.triggers(client);
    const pg_policies = await queries.policies(client);
    const pg_types = await queries.types(client);
    const pg_enums = await queries.enums(client);
    const pg_index = await queries.index(client);
    const pg_roles = await queries.roles(client);
    const pg_foreign_keys = await queries.foreignKeys(client);

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
      pg_foreign_keys,
    };
  } catch (err) {
    console.error("error loading data:", err);
    await releaseClient(client);
    throw err;
  }
}
