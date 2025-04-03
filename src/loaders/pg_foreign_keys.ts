import pg from "pg";
import DataLoader from "dataloader";
import { PgForeignKey } from "../types.js";

/**
 * Interface for foreign key query filtering options
 */
export interface ForeignKeyQueryOptions {
  /** Filter by constraint OIDs */
  constraintOids?: number[];
  /** Filter by relation OIDs (tables with foreign keys) */
  relationOids?: number[];
  /** Filter by referenced relation OIDs (referenced tables) */
  referencedRelationOids?: number[];
  /** Filter by schema names */
  schemaNames?: string[];
  /** Include system schemas */
  includeSystemSchemas?: boolean;
}

/**
 * Database query functions for PostgreSQL foreign keys
 */
export const foreignKeyQueries = {
  /**
   * Query foreign keys with various filtering options
   */
  async query(
    client: pg.Client | pg.PoolClient,
    options: ForeignKeyQueryOptions = {}
  ): Promise<PgForeignKey[]> {
    const { constraintOids, relationOids, referencedRelationOids, schemaNames, includeSystemSchemas } = options;

    // Build the WHERE clause based on filter options
    const conditions: string[] = [];
    const params: any[] = [];

    conditions.push(`c.contype = 'f'`); // Always filter for foreign key constraints

    if (constraintOids && constraintOids.length > 0) {
      conditions.push(`c.oid = ANY($${params.length + 1})`);
      params.push(constraintOids);
    }

    if (relationOids && relationOids.length > 0) {
      conditions.push(`c.conrelid = ANY($${params.length + 1})`);
      params.push(relationOids);
    }

    if (referencedRelationOids && referencedRelationOids.length > 0) {
      conditions.push(`c.confrelid = ANY($${params.length + 1})`);
      params.push(referencedRelationOids);
    }

    if (schemaNames && schemaNames.length > 0) {
      conditions.push(`n.nspname = ANY($${params.length + 1})`);
      params.push(schemaNames);
    }

    // Exclude system schemas unless specifically included
    if (!includeSystemSchemas) {
      conditions.push(`n.nspname NOT IN ('pg_catalog','information_schema')`);
    }

    // Build and execute the SQL query
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const result = await client.query(
      `
      SELECT
        c.oid,
        c.conname,
        c.conrelid,
        c.confrelid,
        c.confupdtype,
        c.confdeltype,
        array_agg(a.attnum) as conkey,
        array_agg(cf.attnum) as confkey
      FROM pg_catalog.pg_constraint c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.connamespace
      JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid
      JOIN pg_catalog.pg_attribute cf ON cf.attrelid = c.confrelid
      ${whereClause}
      AND a.attnum = ANY(c.conkey)
      AND cf.attnum = ANY(c.confkey)
      GROUP BY c.oid, c.conname, c.conrelid, c.confrelid, c.confupdtype, c.confdeltype
      ORDER BY c.conname
      `,
      params
    );

    return result.rows;
  },

  /**
   * Get a single foreign key by OID
   */
  async byOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgForeignKey | null> {
    const fks = await this.query(client, { constraintOids: [oid] });
    return fks.length > 0 ? fks[0] : null;
  },

  /**
   * Get foreign keys by relation OID (source table)
   */
  async byRelationOid(client: pg.Client | pg.PoolClient, relationOid: number): Promise<PgForeignKey[]> {
    return this.query(client, { relationOids: [relationOid] });
  },

  /**
   * Get foreign keys by referenced relation OID (target table)
   */
  async byReferencedRelationOid(client: pg.Client | pg.PoolClient, referencedRelationOid: number): Promise<PgForeignKey[]> {
    return this.query(client, { referencedRelationOids: [referencedRelationOid] });
  }
};

/**
 * Create DataLoaders for PostgreSQL foreign keys
 * @param client - PostgreSQL database client
 * @returns DataLoaders and utility functions for efficient foreign key queries
 */
export function createForeignKeyLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading foreign keys by OID
   */
  const foreignKeyLoader = new DataLoader<number, PgForeignKey | null>(async (oids) => {
    const foreignKeys = await foreignKeyQueries.query(client, { constraintOids: [...oids] });
    
    // Create a map for fast lookup by OID
    const foreignKeyMap = new Map<number, PgForeignKey>();
    foreignKeys.forEach(fk => {
      foreignKeyMap.set(fk.oid, fk);
    });
    
    // Return foreign keys in the same order as requested OIDs
    return oids.map(oid => foreignKeyMap.get(oid) || null);
  });

  /**
   * DataLoader for loading foreign keys by relation OID (source table)
   */
  const foreignKeysByRelationLoader = new DataLoader<number, PgForeignKey[]>(async (relationOids) => {
    const foreignKeys = await foreignKeyQueries.query(client, { relationOids: [...relationOids] });
    
    // Group foreign keys by relation OID
    const foreignKeysByRelation = new Map<number, PgForeignKey[]>();
    relationOids.forEach(oid => foreignKeysByRelation.set(oid, []));
    
    foreignKeys.forEach(fk => {
      const relationForeignKeys = foreignKeysByRelation.get(fk.conrelid) || [];
      relationForeignKeys.push(fk);
      foreignKeysByRelation.set(fk.conrelid, relationForeignKeys);
    });
    
    // Return foreign keys in the same order as requested relation OIDs
    return relationOids.map(oid => foreignKeysByRelation.get(oid) || []);
  });

  /**
   * DataLoader for loading foreign keys by referenced relation OID (target table)
   */
  const foreignKeysByReferencedRelationLoader = new DataLoader<number, PgForeignKey[]>(async (referencedRelationOids) => {
    const foreignKeys = await foreignKeyQueries.query(client, { referencedRelationOids: [...referencedRelationOids] });
    
    // Group foreign keys by referenced relation OID
    const foreignKeysByReferencedRelation = new Map<number, PgForeignKey[]>();
    referencedRelationOids.forEach(oid => foreignKeysByReferencedRelation.set(oid, []));
    
    foreignKeys.forEach(fk => {
      const referencedRelationForeignKeys = foreignKeysByReferencedRelation.get(fk.confrelid) || [];
      referencedRelationForeignKeys.push(fk);
      foreignKeysByReferencedRelation.set(fk.confrelid, referencedRelationForeignKeys);
    });
    
    // Return foreign keys in the same order as requested referenced relation OIDs
    return referencedRelationOids.map(oid => foreignKeysByReferencedRelation.get(oid) || []);
  });

  /**
   * Function to get all foreign keys with optional filtering
   */
  const getAllForeignKeys = async (filter?: (fk: PgForeignKey) => boolean): Promise<PgForeignKey[]> => {
    const foreignKeys = await foreignKeyQueries.query(client);
    return filter ? foreignKeys.filter(filter) : foreignKeys;
  };

  return {
    foreignKeyLoader,
    foreignKeysByRelationLoader,
    foreignKeysByReferencedRelationLoader,
    getAllForeignKeys,
  };
} 