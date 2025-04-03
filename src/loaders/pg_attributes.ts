import DataLoader from "dataloader";
import type { Client, PoolClient } from "pg";
import type { PgAttribute} from "../types.js";
import { PgAttributeSchema } from "../types.js";

/**
 * Options for querying attributes
 */
export interface AttributeQueryOptions {
  /**
   * Filter by relation OIDs (table/view OIDs)
   */
  relationOids?: number[];

  /**
   * Filter by table names
   */
  tableNames?: Array<{ schemaName: string; tableName: string }>;

  /**
   * Filter by column names
   */
  columnNames?: string[];

  /**
   * Skip system schemas
   */
  skipSystemSchemas?: boolean;
}

/**
 * Interface for PgAttribute with schema name for internal use
 */
interface PgAttributeWithSchema extends PgAttribute {
  nspname?: string;
}

/**
 * Functions for querying PgAttribute data
 */
export const attributeQueries = {
  /**
   * Query PgAttribute data with flexible filtering options
   */
  async query(
    client: Client | PoolClient,
    options: AttributeQueryOptions = {}
  ): Promise<PgAttribute[]> {
    const { relationOids, tableNames, columnNames, skipSystemSchemas = true } = options;

    // Build WHERE clauses
    const whereConditions: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    // Add relation OID filter if specified
    if (relationOids?.length) {
      whereConditions.push(`a.attrelid = ANY($${paramIndex})`);
      params.push(relationOids);
      paramIndex++;
    }

    // Add table name filter if specified
    if (tableNames?.length) {
      const conditions: string[] = [];
      tableNames.forEach(({ schemaName, tableName }) => {
        conditions.push(`(n.nspname = $${paramIndex} AND c.relname = $${paramIndex + 1})`);
        params.push(schemaName, tableName);
        paramIndex += 2;
      });
      whereConditions.push(`(${conditions.join(" OR ")})`);
    }

    // Add column name filter if specified
    if (columnNames?.length) {
      whereConditions.push(`a.attname = ANY($${paramIndex})`);
      params.push(columnNames);
      paramIndex++;
    }

    // Filter system schemas
    if (skipSystemSchemas) {
      whereConditions.push(
        `n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`
      );
    }

    // Standard attribute filtering conditions (for user tables)
    whereConditions.push("a.attnum > 0");
    whereConditions.push("NOT a.attisdropped");

    // Build the query
    const whereClause = whereConditions.length ? `WHERE ${whereConditions.join(" AND ")}` : "";

    const sql = `
      SELECT
        a.attrelid,
        a.attname,
        a.atttypid,
        a.attnum,
        a.attnotnull,
        n.nspname
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
      JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
      ${whereClause}
      ORDER BY a.attrelid, a.attnum
    `;

    const result = await client.query(sql, params);

    // Parse and return attributes without schema name property
    return result.rows.map((row) => {
      // Parse with schema name then remove it before returning
      const attrWithSchema = PgAttributeSchema.parse(row) as PgAttributeWithSchema;
      const { nspname, ...attr } = attrWithSchema;
      return attr;
    });
  },

  /**
   * Query for attributes by table name and schema
   */
  async queryByTableName(
    client: Client | PoolClient,
    schemaName: string,
    tableName: string
  ): Promise<PgAttribute[]> {
    const result = await client.query(
      `
      SELECT
        a.attrelid,
        a.attname,
        a.atttypid,
        a.attnum,
        a.attnotnull
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
      JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
      WHERE n.nspname = $1 AND c.relname = $2
        AND a.attnum > 0
        AND NOT a.attisdropped
      ORDER BY a.attnum
    `,
      [schemaName, tableName]
    );

    return result.rows.map((row) => PgAttributeSchema.parse(row));
  },
};

/**
 * Create all PgAttribute DataLoaders
 */
export function createAttributeLoaders(client: Client | PoolClient) {
  /**
   * DataLoader for loading attributes by relation OID
   */
  const attributesByRelationLoader = new DataLoader<number, PgAttribute[] | null>(
    async (relationOids) => {
      const uniqueOids = [...new Set(relationOids)];

      const attributes = await attributeQueries.query(client, {
        relationOids: uniqueOids,
      });

      // Group attributes by relation OID
      const attrMap = new Map<number, PgAttribute[]>();
      attributes.forEach((attr) => {
        if (!attrMap.has(attr.attrelid)) {
          attrMap.set(attr.attrelid, []);
        }
        attrMap.get(attr.attrelid)!.push(attr);
      });

      // Return attributes for each relation OID in the original order
      return relationOids.map((oid) => attrMap.get(oid) || null);
    }
  );

  /**
   * DataLoader for loading attributes by table name and schema
   */
  const attributesByTableNameLoader = new DataLoader<
    { schemaName: string; tableName: string },
    PgAttribute[] | null,
    string
  >(
    async (keys) => {
      // Process each table name individually using the helper query function
      const results = await Promise.all(
        keys.map(({ schemaName, tableName }) =>
          attributeQueries
            .queryByTableName(client, schemaName, tableName)
            .then((attributes) => (attributes.length > 0 ? attributes : null))
        )
      );

      return results;
    },
    {
      // Unique cache key for each table
      cacheKeyFn: (key) => `${key.schemaName}.${key.tableName}`,
    }
  );

  /**
   * Helper function to get all attributes with optional filtering
   */
  const getAllAttributes = async (
    filter?: (attr: PgAttribute) => boolean
  ): Promise<PgAttribute[]> => {
    const attributes = await attributeQueries.query(client, {
      skipSystemSchemas: true,
    });

    return filter ? attributes.filter(filter) : attributes;
  };

  return {
    attributesByRelationLoader,
    attributesByTableNameLoader,
    getAllAttributes,
  };
}
