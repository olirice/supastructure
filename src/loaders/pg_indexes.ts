import pg from "pg";
import DataLoader from "dataloader";
import { PgIndex } from "../types.js";

/**
 * Interface for index query filtering options
 */
export interface IndexQueryOptions {
  /** Filter by OIDs */
  indexOids?: number[];
  /** Filter by relation OIDs */
  relationOids?: number[];
  /** Filter by schema names */
  schemaNames?: string[];
  /** Include system schemas */
  includeSystemSchemas?: boolean;
}

/**
 * Database query functions for PostgreSQL indexes
 */
export const indexQueries = {
  /**
   * Query indexes with various filtering options
   */
  async query(
    client: pg.Client | pg.PoolClient,
    options: IndexQueryOptions = {}
  ): Promise<PgIndex[]> {
    const { indexOids, relationOids, schemaNames, includeSystemSchemas } = options;

    // Build the WHERE clause based on filter options
    const conditions: string[] = [];
    const params: any[] = [];

    if (indexOids && indexOids.length > 0) {
      conditions.push(`x.indexrelid = ANY($${params.length + 1})`);
      params.push(indexOids);
    }

    if (relationOids && relationOids.length > 0) {
      conditions.push(`x.indrelid = ANY($${params.length + 1})`);
      params.push(relationOids);
    }

    if (schemaNames && schemaNames.length > 0) {
      conditions.push(`n.nspname = ANY($${params.length + 1})`);
      params.push(schemaNames);
    }

    // Exclude system schemas unless specifically included
    if (!includeSystemSchemas) {
      conditions.push(
        `n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`
      );
    }

    // Build and execute the SQL query
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const result = await client.query(
      `
      SELECT
        x.indexrelid,
        x.indrelid,
        am.amname as indexam,
        x.indkey::text as indkey,
        pg_get_indexdef(x.indexrelid) as indexdef
      FROM pg_catalog.pg_index x
      JOIN pg_catalog.pg_class ic ON x.indexrelid = ic.oid
      JOIN pg_catalog.pg_class tc ON x.indrelid = tc.oid
      JOIN pg_catalog.pg_am am ON ic.relam = am.oid
      JOIN pg_catalog.pg_namespace n ON tc.relnamespace = n.oid
      ${whereClause}
      ORDER BY n.nspname, ic.relname
      `,
      params
    );

    return result.rows;
  },

  /**
   * Get a single index by OID
   */
  async byOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgIndex | null> {
    const indexes = await this.query(client, { indexOids: [oid] });
    return indexes.length > 0 ? indexes[0] : null;
  },

  /**
   * Get indexes by relation OID
   */
  async byRelationOid(client: pg.Client | pg.PoolClient, relationOid: number): Promise<PgIndex[]> {
    return this.query(client, { relationOids: [relationOid] });
  },
};

/**
 * Create DataLoaders for PostgreSQL indexes
 * @param client - PostgreSQL database client
 * @returns DataLoaders and utility functions for efficient index queries
 */
export function createIndexLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading indexes by OID
   */
  const indexLoader = new DataLoader<number, PgIndex | null>(async (oids) => {
    const indexes = await indexQueries.query(client, { indexOids: [...oids] });

    // Create a map for fast lookup by OID
    const indexMap = new Map<number, PgIndex>();
    indexes.forEach((index) => {
      indexMap.set(index.indexrelid, index);
    });

    // Return indexes in the same order as requested OIDs
    return oids.map((oid) => indexMap.get(oid) || null);
  });

  /**
   * DataLoader for loading indexes by relation OID
   */
  const indexesByRelationLoader = new DataLoader<number, PgIndex[]>(async (relationOids) => {
    const indexes = await indexQueries.query(client, { relationOids: [...relationOids] });

    // Group indexes by relation OID
    const indexesByRelation = new Map<number, PgIndex[]>();
    relationOids.forEach((oid) => indexesByRelation.set(oid, []));

    indexes.forEach((index) => {
      const relationIndexes = indexesByRelation.get(index.indrelid) || [];
      relationIndexes.push(index);
      indexesByRelation.set(index.indrelid, relationIndexes);
    });

    // Return indexes in the same order as requested relation OIDs
    return relationOids.map((oid) => indexesByRelation.get(oid) || []);
  });

  /**
   * Function to get all indexes with optional filtering
   */
  const getAllIndexes = async (filter?: (index: PgIndex) => boolean): Promise<PgIndex[]> => {
    const indexes = await indexQueries.query(client);
    return filter ? indexes.filter(filter) : indexes;
  };

  return {
    indexLoader,
    indexesByRelationLoader,
    getAllIndexes,
  };
}
