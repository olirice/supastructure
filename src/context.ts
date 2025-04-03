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
} from "./types.js";
import { PgDatabaseSchema } from "./types.js";
import { createNamespaceLoaders } from "./loaders/pg_namespaces.js";
import { createClassLoaders } from "./loaders/pg_classes.js";
import { createAttributeLoaders } from "./loaders/pg_attributes.js";
import { createTriggerLoaders } from "./loaders/pg_triggers.js";
import { createPolicyLoaders } from "./loaders/pg_policies.js";
import { createTypeLoaders } from "./loaders/pg_types.js";
import { createEnumLoaders } from "./loaders/pg_enums.js";
import { createIndexLoaders } from "./loaders/pg_indexes.js";
import { createRoleLoaders } from "./loaders/pg_roles.js";
import { createForeignKeyLoaders } from "./loaders/pg_foreign_keys.js";

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
  indexLoader: DataLoader<number, PgIndex | null>;
  indexesByRelationLoader: DataLoader<number, PgIndex[]>;
  roleLoader: DataLoader<number, PgRole | null>;
  roleByNameLoader: DataLoader<string, PgRole | null>;
  foreignKeyLoader: DataLoader<number, PgForeignKey | null>;
  foreignKeysByRelationLoader: DataLoader<number, PgForeignKey[]>;
  foreignKeysByReferencedRelationLoader: DataLoader<number, PgForeignKey[]>;

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
    const dataSources: ReqContext["dataSources"] = {};

    // Resolve database information
    const resolveDatabase = async () => {
      if (dataSources.database) return dataSources.database;
      
      const dbRow = await client.query(`
        select oid, datname
        from pg_catalog.pg_database
        where datname = current_database()
      `);
      dataSources.database = PgDatabaseSchema.parse(dbRow.rows[0]);
      return dataSources.database;
    };

    // Create namespace loaders
    const namespaceLoaders = createNamespaceLoaders(client);

    // Create class loaders
    const classLoaders = createClassLoaders(client);

    // Create attribute loaders
    const attributeLoaders = createAttributeLoaders(client);

    // Create trigger loaders
    const { triggerLoader, triggersByRelationLoader, getAllTriggers } =
      createTriggerLoaders(client);

    // Create policy loaders
    const { policyLoader, policiesByRelationLoader, getAllPolicies } = createPolicyLoaders(client);
    
    // Create type loaders
    const { typeLoader, typeByNameLoader, getAllTypes } = createTypeLoaders(client);
    
    // Create enum loaders
    const { enumByTypeIdLoader, getAllEnums } = createEnumLoaders(client);
    
    // Create index loaders
    const { indexLoader, indexesByRelationLoader, getAllIndexes } = createIndexLoaders(client);
    
    // Create role loaders
    const { roleLoader, roleByNameLoader, getAllRoles } = createRoleLoaders(client);
    
    // Create foreign key loaders
    const { foreignKeyLoader, foreignKeysByRelationLoader, foreignKeysByReferencedRelationLoader, getAllForeignKeys } = 
      createForeignKeyLoaders(client);

    // Use namespace loaders to implement resolveNamespaces
    const resolveNamespaces = namespaceLoaders.getAllNamespaces;

    // Use class loaders to implement resolveClasses
    const resolveClasses = classLoaders.getAllClasses;

    // Use attribute loaders to implement resolveAttributes
    const resolveAttributes = attributeLoaders.getAllAttributes;

    // Use trigger loaders to implement resolveTriggers
    const resolveTriggers = getAllTriggers;

    // Use policy loaders to implement resolvePolicies
    const resolvePolicies = getAllPolicies;
    
    // Use type loaders to implement resolveTypes
    const resolveTypes = getAllTypes;
    
    // Use enum loaders to implement resolveEnums
    const resolveEnums = getAllEnums;
    
    // Use index loaders to implement resolveIndexes
    const resolveIndexes = getAllIndexes;
    
    // Use role loaders to implement resolveRoles
    const resolveRoles = getAllRoles;
    
    // Use foreign key loaders to implement resolveForeignKeys
    const resolveForeignKeys = getAllForeignKeys;

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
      typeByNameLoader,
      classLoader: classLoaders.classLoader,
      classByNameLoader: classLoaders.classByNameLoader,
      classesByNamespaceLoader: classLoaders.classesByNamespaceLoader,
      attributesByRelationLoader: attributeLoaders.attributesByRelationLoader,
      attributesByTableNameLoader: attributeLoaders.attributesByTableNameLoader,
      triggerLoader,
      triggersByRelationLoader,
      policyLoader,
      policiesByRelationLoader,
      enumByTypeIdLoader,
      indexLoader,
      indexesByRelationLoader,
      roleLoader,
      roleByNameLoader,
      foreignKeyLoader,
      foreignKeysByRelationLoader,
      foreignKeysByReferencedRelationLoader,
      namespaceLoader: namespaceLoaders.namespaceLoader,
      namespaceByNameLoader: namespaceLoaders.namespaceByNameLoader,
    };
  } catch (err) {
    console.error("error loading data:", err);
    await releaseClient(client);
    throw err;
  }
}
