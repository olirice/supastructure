import type { PgTrigger } from "../types.js";
import { PgTriggerSchema } from "../types.js";
import type pg from "pg";
import DataLoader from "dataloader";

/**
 * Interface for trigger query options
 */
export interface TriggerQueryOptions {
  oids?: number[]; // Filter by OIDs
  triggerNames?: string[]; // Filter by trigger names
  triggerRelids?: number[]; // Filter by table OIDs
  schemaNames?: string[]; // Filter by schema names
  all?: boolean; // Get all non-system triggers
}

/**
 * Trigger query functions
 */
export const triggerQueries = {
  /**
   * Query triggers with flexible filtering options
   */
  async query(
    client: pg.Client | pg.PoolClient,
    options: TriggerQueryOptions
  ): Promise<PgTrigger[]> {
    let query = `
      SELECT
        t.oid,
        t.tgname,
        t.tgrelid
      FROM pg_catalog.pg_trigger t
      JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
      JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
      WHERE NOT t.tgisinternal
    `;

    const params: any[] = [];
    let paramIndex = 1;

    // Filter by OIDs if provided
    if (options.oids && options.oids.length > 0) {
      query += ` AND t.oid = ANY($${paramIndex})`;
      params.push(options.oids);
      paramIndex++;
    }

    // Filter by trigger names if provided
    if (options.triggerNames && options.triggerNames.length > 0) {
      query += ` AND t.tgname = ANY($${paramIndex})`;
      params.push(options.triggerNames);
      paramIndex++;
    }

    // Filter by relation OIDs if provided
    if (options.triggerRelids && options.triggerRelids.length > 0) {
      query += ` AND t.tgrelid = ANY($${paramIndex})`;
      params.push(options.triggerRelids);
      paramIndex++;
    }

    // Filter by schema names if provided
    if (options.schemaNames && options.schemaNames.length > 0) {
      query += ` AND n.nspname = ANY($${paramIndex})`;
      params.push(options.schemaNames);
      paramIndex++;
    }

    // Exclude system schemas for "all" queries
    if (options.all) {
      query += ` AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`;
    }

    query += ` ORDER BY t.tgname ASC`;

    const result = await client.query(query, params);

    return result.rows.map((row) => PgTriggerSchema.parse(row));
  },

  /**
   * Get trigger by OID
   */
  async byOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgTrigger | null> {
    return this.query(client, { oids: [oid] }).then((results) => results[0] || null);
  },

  /**
   * Get triggers by relation OID
   */
  async byRelationOid(client: pg.Client | pg.PoolClient, relid: number): Promise<PgTrigger[]> {
    return this.query(client, { triggerRelids: [relid] });
  },

  /**
   * Get trigger by name and schema
   */
  async byNameAndSchema(
    client: pg.Client | pg.PoolClient,
    schemaName: string,
    triggerName: string
  ): Promise<PgTrigger | null> {
    return this.query(client, {
      triggerNames: [triggerName],
      schemaNames: [schemaName],
    }).then((results) => results[0] || null);
  },
};

/**
 * Create trigger-related DataLoaders
 */
export function createTriggerLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading triggers by OID
   */
  const triggerLoader = new DataLoader<number, PgTrigger | null>(async (triggerOids) => {
    const uniqueOids = [...new Set(triggerOids)];

    const triggers = await triggerQueries.query(client, { oids: uniqueOids });

    // Create a map for quick lookups by OID
    const triggerMap = new Map<number, PgTrigger>();
    triggers.forEach((trigger) => {
      triggerMap.set(trigger.oid, trigger);
    });

    return triggerOids.map((oid) => triggerMap.get(oid) || null);
  });

  /**
   * DataLoader for loading triggers by relation OID (table/view)
   */
  const triggersByRelationLoader = new DataLoader<number, PgTrigger[]>(async (relationOids) => {
    const uniqueOids = [...new Set(relationOids)];

    const triggers = await triggerQueries.query(client, {
      triggerRelids: uniqueOids,
    });

    // Group triggers by relation OID
    const triggersByRelation = new Map<number, PgTrigger[]>();
    uniqueOids.forEach((oid) => triggersByRelation.set(oid, []));

    triggers.forEach((trigger) => {
      const list = triggersByRelation.get(trigger.tgrelid) || [];
      list.push(trigger);
      triggersByRelation.set(trigger.tgrelid, list);
    });

    return relationOids.map((oid) => triggersByRelation.get(oid) || []);
  });

  /**
   * Function to get all triggers (with optional filtering)
   */
  const getAllTriggers = async (filter?: (t: PgTrigger) => boolean): Promise<PgTrigger[]> => {
    const triggers = await triggerQueries.query(client, { all: true });

    // Apply filter if provided
    return filter ? triggers.filter(filter) : triggers;
  };

  return {
    triggerLoader,
    triggersByRelationLoader,
    getAllTriggers,
  };
}
