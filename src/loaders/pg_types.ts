import type { PgType } from "../types.js";
import { PgTypeSchema } from "../types.js";
import type { Client, PoolClient } from "pg";
import DataLoader from "dataloader";

/**
 * Options for querying types from the database
 */
export interface TypeQueryOptions {
  /** Filter by specific type OIDs */
  oids?: number[];
  /** Filter by specific type names */
  typeNames?: string[];
  /** Filter by schema names */
  schemaNames?: string[];
  /** Filter by type kinds (e.g., 'b' for base, 'c' for composite, etc.) */
  typeKinds?: string[];
  /** Include all types (including system ones) */
  all?: boolean;
}

/**
 * Type query helpers
 */
export const typeQueries = {
  /**
   * Query types with various filtering options
   */
  async query(client: Client | PoolClient, options: TypeQueryOptions = {}): Promise<PgType[]> {
    let query = `
      SELECT
        t.oid,
        t.typname,
        t.typtype,
        t.typbasetype,
        t.typelem,
        t.typrelid,
        t.typnamespace,
        n.nspname
      FROM pg_catalog.pg_type t
      JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE 1=1
    `;

    const params: any[] = [];
    let paramIndex = 1;

    // Filter by type OIDs if provided
    if (options.oids && options.oids.length > 0) {
      query += ` AND t.oid = ANY($${paramIndex})`;
      params.push(options.oids);
      paramIndex++;
    }

    // Filter by type names if provided
    if (options.typeNames && options.typeNames.length > 0) {
      query += ` AND t.typname = ANY($${paramIndex})`;
      params.push(options.typeNames);
      paramIndex++;
    }

    // Filter by schema names if provided
    if (options.schemaNames && options.schemaNames.length > 0) {
      query += ` AND n.nspname = ANY($${paramIndex})`;
      params.push(options.schemaNames);
      paramIndex++;
    }

    // Filter by type kind if provided
    if (options.typeKinds && options.typeKinds.length > 0) {
      query += ` AND t.typtype = ANY($${paramIndex})`;
      params.push(options.typeKinds);
      paramIndex++;
    }

    // Exclude system schemas for "all" queries
    if (!options.all) {
      query += ` AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`;
    }

    query += `
      ORDER BY n.nspname, t.typname
    `;

    const result = await client.query(query, params.length > 0 ? params : []);

    return result.rows.map((row) => PgTypeSchema.parse(row));
  },

  /**
   * Get type by OID
   */
  async byOid(client: Client | PoolClient, oid: number): Promise<PgType | null> {
    return this.query(client, { oids: [oid] }).then((results) => results[0] || null);
  },

  /**
   * Get types by name and schema
   */
  async byNameAndSchema(
    client: Client | PoolClient,
    schemaName: string,
    typeName: string
  ): Promise<PgType | null> {
    return this.query(client, {
      typeNames: [typeName],
      schemaNames: [schemaName],
    }).then((results) => results[0] || null);
  },
};

/**
 * Create DataLoaders for PgType entities
 */
export function createTypeLoaders(client: Client | PoolClient) {
  /**
   * DataLoader for loading types by OID
   */
  const typeLoader = new DataLoader<number, PgType | null>(async (typeOids) => {
    const uniqueOids = [...new Set(typeOids)];

    const types = await typeQueries.query(client, { oids: uniqueOids, all: true });

    // Create a map for quick lookups by OID
    const typeMap = new Map<number, PgType>();
    types.forEach((type) => {
      typeMap.set(type.oid, type);
    });

    return typeOids.map((oid) => typeMap.get(oid) || null);
  });

  /**
   * DataLoader for loading types by name and schema
   */
  const typeByNameLoader = new DataLoader<
    { schemaName: string; typeName: string },
    PgType | null,
    string
  >(
    async (keys) => {
      const schemaNames = [...new Set(keys.map((k) => k.schemaName))];
      const typeNames = [...new Set(keys.map((k) => k.typeName))];

      const types = await typeQueries.query(client, {
        schemaNames,
        typeNames,
        all: true,
      });

      // Create a map for quick lookups by schema and name
      const typeMap = new Map<string, PgType>();
      types.forEach((type) => {
        // Get the nspname from the row data directly
        const nspname = (type as any).nspname;
        if (nspname) {
          const key = `${nspname}.${type.typname}`;
          typeMap.set(key, type);
        }
      });

      return keys.map((key) => typeMap.get(`${key.schemaName}.${key.typeName}`) || null);
    },
    {
      // Unique cache key for each type
      cacheKeyFn: (key) => `${key.schemaName}.${key.typeName}`,
    }
  );

  /**
   * Function to get all types (with optional filtering)
   */
  const getAllTypes = async (filter?: (type: PgType) => boolean): Promise<PgType[]> => {
    const types = await typeQueries.query(client);
    return filter ? types.filter(filter) : types;
  };

  return {
    typeLoader,
    typeByNameLoader,
    getAllTypes,
  };
}
