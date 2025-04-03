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
import DataLoader from "dataloader";
import { createNamespaceLoaders } from "./loaders/pg_namespaces.js";
import { createClassLoaders } from "./loaders/pg_classes.js";
import { attributeQueries, createAttributeLoaders } from "./loaders/pg_attributes.js";
import { createTriggerLoaders, triggerQueries } from "./loaders/pg_triggers.js";

/**
 * Helper functions for parsing database rows into typed objects
 */
function parseTypeRow(row: any): PgType & { typnamespace?: number, nspname?: string } {
  // Apply zod schema validation for the base fields
  const baseType = PgTypeSchema.parse(row);
  
  // Add additional fields that may be needed by resolvers
  return {
    ...baseType,
    typbasetype: row.typbasetype,
    typelem: row.typelem,
    typnamespace: row.typnamespace,
    nspname: row.nspname
  };
}

/**
 * Database query functions for PostgreSQL metadata
 * These functions are used directly or by DataLoaders to fetch data
 */

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

/**
 * Request context interface for GraphQL resolvers
 * Provides access to database client, data loaders, and resolver functions
 */
export interface ReqContext {
  /** PostgreSQL client for direct database access */
  client: pg.Client | pg.PoolClient;
  
  /** 
   * Resolver functions that efficiently load and cache data
   * These use the dataSources cache to avoid redundant queries
   */
  resolveDatabase: () => Promise<PgDatabase>;
  resolveNamespaces: (filter?: (ns: PgNamespace) => boolean) => Promise<PgNamespace[]>;
  resolveClasses: (filter?: (cls: PgClass) => boolean) => Promise<PgClass[]>;
  resolveAttributes: (filter?: (attr: PgAttribute) => boolean) => Promise<PgAttribute[]>;
  resolveTriggers: (filter?: (trigger: PgTrigger) => boolean) => Promise<PgTrigger[]>;
  resolvePolicies: (filter?: (policy: PgPolicy) => boolean) => Promise<PgPolicy[]>;
  resolveTypes: (filter?: (type: PgType) => boolean) => Promise<PgType[]>;
  resolveEnums: (filter?: (enum_: PgEnum) => boolean) => Promise<PgEnum[]>;
  resolveIndexes: (filter?: (index: PgIndex) => boolean) => Promise<PgIndex[]>;
  resolveRoles: (filter?: (role: PgRole) => boolean) => Promise<PgRole[]>;
  resolveForeignKeys: (filter?: (fk: PgForeignKey) => boolean) => Promise<PgForeignKey[]>;
  
  /**
   * DataLoaders for efficient batched SQL queries
   * These reduce the N+1 query problem by batching multiple individual
   * lookups into a single query and caching results
   */
  typeLoader: DataLoader<number, PgType | null>;
  classLoader: DataLoader<number, PgClass | null>;
  classByNameLoader: DataLoader<
    { schema: string; name: string },
    PgClass | null
  >;
  classesByNamespaceLoader: DataLoader<
    { namespaceOid: number; relkind?: string },
    PgClass[]
  >;
  attributesByRelationLoader: DataLoader<number, PgAttribute[] | null>;
  attributesByTableNameLoader: DataLoader<
    { schemaName: string; tableName: string },
    PgAttribute[] | null,
    string
  >;
  triggerLoader: DataLoader<number, PgTrigger | null>;
  triggersByRelationLoader: DataLoader<number, PgTrigger[]>;
  policyLoader: DataLoader<number, PgPolicy[] | null>;
  
  /** 
   * Cached data sources to avoid redundant queries
   * These are populated on-demand by resolver functions
   */
  dataSources: {
    database?: PgDatabase;
    namespaces?: PgNamespace[];
    classes?: PgClass[];
    attributes?: PgAttribute[];
    triggers?: PgTrigger[];
    policies?: PgPolicy[];
    types?: PgType[];
    enums?: PgEnum[];
    indexes?: PgIndex[];
    roles?: PgRole[];
    foreignKeys?: PgForeignKey[];
  };

  /**
   * Namespace loaders
   */
  namespaceLoader: DataLoader<number, PgNamespace | null>;
  namespaceByNameLoader: DataLoader<string, PgNamespace | null>;
}

/**
 * Database configuration interface
 */
export interface DbConfig {
  /** PostgreSQL username */
  user: string;
  /** Database server hostname or IP address */
  host: string;
  /** Database name to connect to */
  database: string;
  /** PostgreSQL password */
  password: string;
  /** PostgreSQL server port */
  port: number;
}

/**
 * Properly release a database client based on its type
 * @param client - The PostgreSQL client to release
 */
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

/**
 * Creates a request context with database client, data loaders, and resolver functions
 * @param dbConfig - Database connection configuration
 * @param existingClient - Optional existing database client to use instead of creating a new one
 * @returns A request context object for GraphQL resolvers
 */
export async function context(
  dbConfig: DbConfig,
  existingClient?: pg.Client | pg.PoolClient
): Promise<ReqContext> {
  const client = existingClient || new pg.Client(dbConfig);
  if (!existingClient) {
    await client.connect();
  }
  
  try {
    // Create data sources object to cache results
    const dataSources: ReqContext['dataSources'] = {};
    
    // Create resolver functions that use efficient data loading with caching
    const resolveDatabase = async () => {
      if (dataSources.database) return dataSources.database;
      dataSources.database = await queries.database(client);
      return dataSources.database;
    };
    
    // Create namespace loaders
    const namespaceLoaders = createNamespaceLoaders(client);
    
    // Create class loaders
    const classLoaders = createClassLoaders(client);
    
    // Create attribute loaders
    const attributeLoaders = createAttributeLoaders(client);
    
    // Create trigger loaders
    const { triggerLoader, triggersByRelationLoader, getAllTriggers } = createTriggerLoaders(client);
    
    // Use namespace loaders to implement resolveNamespaces
    const resolveNamespaces = namespaceLoaders.getAllNamespaces;
    
    // Use class loaders to implement resolveClasses
    const resolveClasses = classLoaders.getAllClasses;
    
    const resolveAttributes = attributeLoaders.getAllAttributes;
    
    const resolveTriggers = async (filter?: (trigger: PgTrigger) => boolean) => {
      return getAllTriggers(filter);
    };
    
    const resolvePolicies = async (filter?: (policy: PgPolicy) => boolean) => {
      if (!dataSources.policies) {
        dataSources.policies = await queries.policies(client);
      }
      return filter ? dataSources.policies.filter(filter) : dataSources.policies;
    };
    
    const resolveTypes = async (filter?: (type: PgType) => boolean) => {
      if (!dataSources.types) {
        dataSources.types = await queries.types(client);
      }
      return filter ? dataSources.types.filter(filter) : dataSources.types;
    };
    
    const resolveEnums = async (filter?: (enum_: PgEnum) => boolean) => {
      if (!dataSources.enums) {
        dataSources.enums = await queries.enums(client);
      }
      return filter ? dataSources.enums.filter(filter) : dataSources.enums;
    };
    
    const resolveIndexes = async (filter?: (index: PgIndex) => boolean) => {
      if (!dataSources.indexes) {
        dataSources.indexes = await queries.index(client);
      }
      return filter ? dataSources.indexes.filter(filter) : dataSources.indexes;
    };
    
    const resolveRoles = async (filter?: (role: PgRole) => boolean) => {
      if (!dataSources.roles) {
        dataSources.roles = await queries.roles(client);
      }
      return filter ? dataSources.roles.filter(filter) : dataSources.roles;
    };
    
    const resolveForeignKeys = async (filter?: (fk: PgForeignKey) => boolean) => {
      if (!dataSources.foreignKeys) {
        dataSources.foreignKeys = await queries.foreignKeys(client);
      }
      return filter ? dataSources.foreignKeys.filter(filter) : dataSources.foreignKeys;
    };
    
    // Create DataLoader for types
    const typeLoader = new DataLoader<number, PgType | null>(async (typeOids) => {
      const uniqueOids = [...new Set(typeOids)];
      
      const result = await client.query(`
        SELECT 
          t.oid, 
          t.typname, 
          t.typnamespace, 
          t.typtype, 
          t.typbasetype,
          t.typelem,
          t.typrelid,
          n.nspname
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
        WHERE t.oid = ANY($1)
      `, [uniqueOids]);
      
      const typeMap = new Map<number, PgType>();
      result.rows.forEach(row => {
        const type = parseTypeRow(row);
        typeMap.set(type.oid, type);
      });
      
      return typeOids.map(oid => typeMap.get(oid) || null);
    });
    
    // Create DataLoader for classes (tables, views, etc.)
    const classLoader = new DataLoader<number, PgClass | null>(async (classOids) => {
      const uniqueOids = [...new Set(classOids)];
      
      const result = await client.query(`
        SELECT
          c.oid,
          c.relname,
          c.relnamespace,
          c.relkind,
          c.relispopulated,
          c.relrowsecurity,
          n.nspname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = ANY($1)
      `, [uniqueOids]);
      
      const classMap = new Map<number, PgClass>();
      result.rows.forEach(row => {
        const cls = PgClassSchema.parse(row);
        classMap.set(cls.oid, cls);
      });
      
      return classOids.map(oid => classMap.get(oid) || null);
    });

    // Create DataLoader for attributes (columns) by relation OID
    const attributeLoader = new DataLoader<number, PgAttribute[] | null>(async (relationOids) => {
      const uniqueOids = [...new Set(relationOids)];
      
      const result = await client.query(`
        SELECT
          a.attrelid,
          a.attname,
          a.atttypid,
          a.attnum,
          a.attnotnull,
          a.atthasdef,
          a.attidentity,
          pg_catalog.pg_get_expr(d.adbin, d.adrelid) as adsrc,
          t.typname,
          t.typnamespace,
          t.typtype,
          t.typrelid,
          n.nspname as typnspname
        FROM pg_catalog.pg_attribute a
        LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
        JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
        JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
        WHERE a.attrelid = ANY($1)
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY a.attnum
      `, [uniqueOids]);
      
      // Group attributes by relation OID
      const attrMap = new Map<number, PgAttribute[]>();
      result.rows.forEach(row => {
        const attr = PgAttributeSchema.parse(row);
        if (!attrMap.has(attr.attrelid)) {
          attrMap.set(attr.attrelid, []);
        }
        attrMap.get(attr.attrelid)!.push(attr);
      });
      
      return relationOids.map(oid => attrMap.get(oid) || null);
    });
    
    // Create DataLoader for policies by relation OID
    const policyLoader = new DataLoader<number, PgPolicy[] | null>(async (tableOids) => {
      const uniqueOids = [...new Set(tableOids)];
      
      const result = await client.query(`
        SELECT
          p.oid,
          p.polrelid,
          p.polname,
          p.polcmd,
          p.polpermissive,
          pg_catalog.pg_get_expr(p.polqual, p.polrelid) as polqual,
          pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid) as polwithcheck,
          array_to_string(p.polroles::name[], ',') as polroles,
          c.relname,
          c.relnamespace,
          n.nspname
        FROM pg_catalog.pg_policy p
        JOIN pg_catalog.pg_class c ON p.polrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE p.polrelid = ANY($1)
      `, [uniqueOids]);
      
      // Group policies by table OID
      const policyMap = new Map<number, PgPolicy[]>();
      result.rows.forEach(row => {
        const policy = PgPolicySchema.parse(row);
        if (!policyMap.has(policy.polrelid)) {
          policyMap.set(policy.polrelid, []);
        }
        policyMap.get(policy.polrelid)!.push(policy);
      });
      
      return tableOids.map(oid => policyMap.get(oid) || null);
    });
    
    return {
      client,
      dataSources,
      resolveDatabase,
      resolveNamespaces,
      resolveClasses,
      resolveAttributes,
      resolveTriggers,
      resolvePolicies,
      resolveTypes,
      resolveEnums,
      resolveIndexes,
      resolveRoles,
      resolveForeignKeys,
      typeLoader,
      classLoader: classLoaders.classLoader,
      classByNameLoader: classLoaders.classByNameLoader,
      classesByNamespaceLoader: classLoaders.classesByNamespaceLoader,
      attributesByRelationLoader: attributeLoaders.attributesByRelationLoader,
      attributesByTableNameLoader: attributeLoaders.attributesByTableNameLoader,
      triggerLoader,
      triggersByRelationLoader,
      policyLoader,
      namespaceLoader: namespaceLoaders.namespaceLoader,
      namespaceByNameLoader: namespaceLoaders.namespaceByNameLoader
    };
  } catch (err) {
    console.error("error loading data:", err);
    await releaseClient(client);
    throw err;
  }
}
