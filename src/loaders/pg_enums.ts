import type { PgEnum } from "../types.js";
import { PgEnumSchema } from "../types.js";
import type { Client, PoolClient } from "pg";
import DataLoader from "dataloader";

/**
 * Options for querying enums from the database
 */
export interface EnumQueryOptions {
  /** Filter by specific enum type IDs */
  enumTypeIds?: number[];
  /** Filter by schema names */
  schemaNames?: string[];
  /** Filter by enum names */
  enumNames?: string[];
  /** Include all enums (including system ones) */
  all?: boolean;
}

/**
 * Enum query helpers
 */
export const enumQueries = {
  /**
   * Query enums with various filtering options
   */
  async query(client: Client | PoolClient, options: EnumQueryOptions = {}): Promise<PgEnum[]> {
    let query = `
      SELECT 
        e.enumtypid,
        t.typname as enumname,
        n.nspname as schemaname,
        ARRAY_AGG(e.enumlabel ORDER BY e.enumsortorder) as enumlabels
      FROM pg_catalog.pg_enum e
      JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
      JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE 1=1
    `;

    const params: any[] = [];
    let paramIndex = 1;

    // Filter by enum type IDs if provided
    if (options.enumTypeIds && options.enumTypeIds.length > 0) {
      query += ` AND e.enumtypid = ANY($${paramIndex})`;
      params.push(options.enumTypeIds);
      paramIndex++;
    }

    // Filter by schema names if provided
    if (options.schemaNames && options.schemaNames.length > 0) {
      query += ` AND n.nspname = ANY($${paramIndex})`;
      params.push(options.schemaNames);
      paramIndex++;
    }

    // Filter by enum names if provided
    if (options.enumNames && options.enumNames.length > 0) {
      query += ` AND t.typname = ANY($${paramIndex})`;
      params.push(options.enumNames);
      paramIndex++;
    }

    // Exclude system schemas for non-"all" queries
    if (!options.all) {
      query += ` AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`;
    }

    query += `
      GROUP BY e.enumtypid, t.typname, n.nspname
      ORDER BY n.nspname, t.typname
    `;

    const result = await client.query(query, params.length > 0 ? params : []);

    // Ensure enumlabels is always treated as an array
    return result.rows.map((row) => {
      // If enumlabels is a string (could happen with database driver parsing), convert it to array
      if (typeof row.enumlabels === "string") {
        try {
          // Parse the string if it's in a format like '{a,b,c}'
          if (row.enumlabels.startsWith("{") && row.enumlabels.endsWith("}")) {
            // Remove the braces and split by comma
            const parsed = row.enumlabels
              .slice(1, -1)
              .split(",")
              .map((s: string) => s.trim());
            row.enumlabels = parsed;
          } else {
            // Fallback to single element array
            row.enumlabels = [row.enumlabels];
          }
        } catch (e) {
          // In case of parsing error, ensure we have an array
          row.enumlabels = [row.enumlabels];
        }
      } else if (!Array.isArray(row.enumlabels)) {
        // If it's not a string and not an array, make it an empty array
        row.enumlabels = [];
      }

      return PgEnumSchema.parse(row);
    });
  },

  /**
   * Get enum by type ID
   */
  async byTypeId(client: Client | PoolClient, enumTypeId: number): Promise<PgEnum | null> {
    return this.query(client, { enumTypeIds: [enumTypeId] }).then((results) => results[0] || null);
  },
};

/**
 * Create DataLoaders for PgEnum entities
 */
export function createEnumLoaders(client: Client | PoolClient) {
  /**
   * DataLoader for loading enums by type ID
   */
  const enumByTypeIdLoader = new DataLoader<number, PgEnum | null>(async (typeIds) => {
    const uniqueIds = [...new Set(typeIds)];

    const enums = await enumQueries.query(client, { enumTypeIds: uniqueIds, all: true });

    // Create a map for quick lookups by type ID
    const enumMap = new Map<number, PgEnum>();
    enums.forEach((enum_) => {
      enumMap.set(enum_.enumtypid, enum_);
    });

    return typeIds.map((id) => enumMap.get(id) || null);
  });

  /**
   * DataLoader for loading enums by name and schema
   */
  const enumByNameLoader = new DataLoader<
    { schemaName: string; enumName: string },
    PgEnum | null,
    string
  >(
    async (keys) => {
      const schemaNames = [...new Set(keys.map((k) => k.schemaName))];
      const enumNames = [...new Set(keys.map((k) => k.enumName))];

      const enums = await enumQueries.query(client, {
        schemaNames,
        enumNames,
        all: true,
      });

      // Create a map for quick lookups by schema and name
      const enumMap = new Map<string, PgEnum>();
      enums.forEach((enum_) => {
        // Get the schemaname and enumname from the row data directly
        const schemaname = (enum_ as any).schemaname;
        const enumname = (enum_ as any).enumname;
        if (schemaname && enumname) {
          const key = `${schemaname}.${enumname}`;
          enumMap.set(key, enum_);
        }
      });

      return keys.map((key) => enumMap.get(`${key.schemaName}.${key.enumName}`) || null);
    },
    {
      // Unique cache key for each enum
      cacheKeyFn: (key) => `${key.schemaName}.${key.enumName}`,
    }
  );

  /**
   * Function to get all enums (with optional filtering)
   */
  const getAllEnums = async (filter?: (enum_: PgEnum) => boolean): Promise<PgEnum[]> => {
    const enums = await enumQueries.query(client);
    return filter ? enums.filter(filter) : enums;
  };

  return {
    enumByTypeIdLoader,
    enumByNameLoader,
    getAllEnums,
  };
}
