import pg from "pg";
import DataLoader from "dataloader";
import type {
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
  PgForeignKey,
  PgExtension,
} from "./types.js";
import { PgDatabaseSchema } from "./types.js";
import { createLoaders } from "./loaders.js";

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
  resolveExtensions: (filter?: (ext: PgExtension) => boolean) => Promise<PgExtension[]>;

  /**
   * DataLoaders for efficient batched SQL queries
   * These reduce the N+1 query problem by batching multiple individual
   * lookups into a single query and caching results
   */
  typeLoader: DataLoader<number, PgType | null>;
  typeByNameLoader: DataLoader<{ schemaName: string; typeName: string }, PgType | null, string>;
  classLoader: DataLoader<number, PgClass | null>;
  classByNameLoader: DataLoader<{ schema: string; name: string }, PgClass | null>;
  classesByNamespaceLoader: DataLoader<{ namespaceOid: number; relkind?: string }, PgClass[]>;
  attributesByRelationLoader: DataLoader<number, PgAttribute[] | null>;
  attributesByTableNameLoader: DataLoader<
    { schemaName: string; tableName: string },
    PgAttribute[] | null,
    string
  >;
  triggerLoader: DataLoader<number, PgTrigger | null>;
  triggersByRelationLoader: DataLoader<number, PgTrigger[]>;
  policyLoader: DataLoader<number, PgPolicy | null>;
  policiesByRelationLoader: DataLoader<number, PgPolicy[]>;
  enumByTypeIdLoader: DataLoader<number, PgEnum | null>;
  enumByNameLoader: DataLoader<{ schemaName: string; enumName: string }, PgEnum | null, string>;
  indexLoader: DataLoader<number, PgIndex | null>;
  indexesByRelationLoader: DataLoader<number, PgIndex[]>;
  roleLoader: DataLoader<number, PgRole | null>;
  roleByNameLoader: DataLoader<string, PgRole | null>;
  foreignKeyLoader: DataLoader<number, PgForeignKey | null>;
  foreignKeysByRelationLoader: DataLoader<number, PgForeignKey[]>;
  foreignKeysByReferencedRelationLoader: DataLoader<number, PgForeignKey[]>;
  extensionLoader: DataLoader<number, PgExtension | null>;
  extensionByNameLoader: DataLoader<string, PgExtension | null>;
  extensionsBySchemaLoader: DataLoader<number, PgExtension[]>;

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
    extensions?: PgExtension[];
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
export async function releaseClient(client: pg.Client | pg.PoolClient): Promise<void> {
  if ("end" in client) {
    // If it's a pg.Client instance, close the connection
    await client.end();
  } else {
    // If it's a pg.PoolClient instance, release it back to the pool
    await client.release();
  }
}

/**
 * Type guard functions for data sources
 */
function hasDatabase(
  ds: ReqContext["dataSources"]
): ds is ReqContext["dataSources"] & { database: PgDatabase } {
  return !!ds.database;
}

/**
 * Creates a request context with database client, data loaders, and resolver functions
 * @param dbConfig - Database connection configuration
 * @param existingClient - Optional existing database client to use instead of creating a new one
 * @param loaderFactory - Factory function for creating data loaders (for testing)
 * @returns A request context object for GraphQL resolvers
 */
export async function context(
  dbConfig: DbConfig,
  existingClient?: pg.Client | pg.PoolClient,
  loaderFactory = createLoaders
): Promise<ReqContext> {
  const client = existingClient || new pg.Client(dbConfig);
  if (!existingClient) {
    await client.connect();
  }

  try {
    // Create all loaders and resolvers using the factory
    // This also provides a shared dataSources object that the resolvers will use
    const { loaders, resolvers, dataSources } = loaderFactory(client);

    // Resolve database information
    const resolveDatabase = async () => {
      if (hasDatabase(dataSources)) return dataSources.database;

      const dbRow = await client.query(`
        select oid, datname
        from pg_catalog.pg_database
        where datname = current_database()
      `);
      dataSources.database = PgDatabaseSchema.parse(dbRow.rows[0]);
      return dataSources.database;
    };

    // Return the complete context object with proper type assertions
    const contextObj: ReqContext = {
      client,
      dataSources,
      resolveDatabase,
      ...resolvers,
      ...loaders,
    };

    return contextObj;
  } catch (err) {
    console.error("error loading data:", err);
    await releaseClient(client);
    throw err;
  }
}
