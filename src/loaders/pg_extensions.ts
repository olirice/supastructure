import pg from "pg";
import DataLoader from "dataloader";
import { PgExtension } from "../types.js";
import { PgExtensionSchema } from "../types.js";

/**
 * Options for querying extensions from the database
 */
export interface ExtensionQueryOptions {
  /** Filter by specific extension OIDs */
  oids?: number[];
  /** Filter by specific extension names */
  names?: string[];
  /** Filter by schema OIDs */
  schemaOids?: number[];
  /** Only include installed extensions */
  onlyInstalled?: boolean;
}

/**
 * Database query functions for PostgreSQL extensions
 */
export const extensionQueries = {
  /**
   * Query extensions with various filtering options
   */
  async query(
    client: pg.Client | pg.PoolClient,
    options: ExtensionQueryOptions = {}
  ): Promise<PgExtension[]> {
    // Build the WHERE clause based on filter options
    const conditions: string[] = [];
    const params: any[] = [];

    if (options.oids && options.oids.length > 0) {
      conditions.push(`e.oid = ANY($${params.length + 1})`);
      params.push(options.oids);
    }

    if (options.names && options.names.length > 0) {
      conditions.push(`ae.name = ANY($${params.length + 1})`);
      params.push(options.names);
    }

    if (options.schemaOids && options.schemaOids.length > 0) {
      conditions.push(`e.extnamespace = ANY($${params.length + 1})`);
      params.push(options.schemaOids);
    }

    // Only include installed extensions if requested
    if (options.onlyInstalled) {
      conditions.push(`e.oid IS NOT NULL`);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    // Query that joins pg_extension with pg_available_extensions and pg_available_extension_versions
    const result = await client.query(
      `
      select 
        e.oid, 
        coalesce(e.extname, ae.name) AS name, 
        ae.default_version AS "defaultVersion", 
        ae.comment, 
        case when e.oid is null then false else true end as installed,
        e.extversion AS "installedVersion",
        e.extnamespace AS "schemaOid"
      from
        pg_catalog.pg_available_extensions ae
        left join pg_catalog.pg_extension e
          on ae.name = e.extname
      ${whereClause}
      order by ae.name 
      `,
      params
    );

    return result.rows.map((row) => PgExtensionSchema.parse(row));
  },

  /**
   * Get a single extension by OID
   */
  async byOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgExtension | null> {
    const extensions = await this.query(client, { oids: [oid] });
    return extensions.length > 0 ? extensions[0] : null;
  },

  /**
   * Get a single extension by name
   */
  async byName(client: pg.Client | pg.PoolClient, name: string): Promise<PgExtension | null> {
    const extensions = await this.query(client, { names: [name] });
    return extensions.length > 0 ? extensions[0] : null;
  },

  /**
   * Get extensions by schema OID
   */
  async bySchemaOid(
    client: pg.Client | pg.PoolClient,
    schemaOid: number
  ): Promise<PgExtension[]> {
    return this.query(client, { schemaOids: [schemaOid] });
  },
};

/**
 * Create DataLoaders for PostgreSQL extensions
 * @param client - PostgreSQL database client
 * @returns DataLoaders and utility functions for efficient extension queries
 */
export function createExtensionLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading extensions by OID
   */
  const extensionLoader = new DataLoader<number, PgExtension | null>(async (oids) => {
    const extensions = await extensionQueries.query(client, { oids: [...oids] });

    // Create a map for fast lookup by OID
    const extensionMap = new Map<number, PgExtension>();
    extensions.forEach((ext) => {
      if (ext.oid) {
        extensionMap.set(ext.oid, ext);
      }
    });

    // Return extensions in the same order as requested OIDs
    return oids.map((oid) => extensionMap.get(oid) || null);
  });

  /**
   * DataLoader for loading extensions by name
   */
  const extensionByNameLoader = new DataLoader<string, PgExtension | null>(async (names) => {
    const extensions = await extensionQueries.query(client, { names: [...names] });

    // Create a map for fast lookup by name
    const extensionMap = new Map<string, PgExtension>();
    extensions.forEach((ext) => {
      extensionMap.set(ext.name, ext);
    });

    // Return extensions in the same order as requested names
    return names.map((name) => extensionMap.get(name) || null);
  });

  /**
   * DataLoader for loading extensions by schema OID
   */
  const extensionsBySchemaLoader = new DataLoader<number, PgExtension[]>(async (schemaOids) => {
    const extensions = await extensionQueries.query(client, { schemaOids: [...schemaOids] });

    // Group extensions by schema OID
    const extensionsBySchema = new Map<number, PgExtension[]>();
    schemaOids.forEach((oid) => extensionsBySchema.set(oid, []));

    extensions.forEach((ext) => {
      if (ext.schemaOid) {
        const existing = extensionsBySchema.get(ext.schemaOid) || [];
        existing.push(ext);
        extensionsBySchema.set(ext.schemaOid, existing);
      }
    });

    // Return extensions in the same order as requested schema OIDs
    return schemaOids.map((oid) => extensionsBySchema.get(oid) || []);
  });

  /**
   * Function to get all extensions with optional filtering
   */
  const getAllExtensions = async (
    filter?: (ext: PgExtension) => boolean
  ): Promise<PgExtension[]> => {
    const extensions = await extensionQueries.query(client);
    return filter ? extensions.filter(filter) : extensions;
  };

  return {
    extensionLoader,
    extensionByNameLoader,
    extensionsBySchemaLoader,
    getAllExtensions,
  };
} 