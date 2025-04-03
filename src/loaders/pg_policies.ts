import type { PgPolicy } from "../types.js";
import { PgPolicySchema } from "../types.js";
import type pg from "pg";
import DataLoader from "dataloader";

/**
 * Interface for policy query options
 */
export interface PolicyQueryOptions {
  oids?: number[]; // Filter by OIDs
  policyNames?: string[]; // Filter by policy names
  policyRelids?: number[]; // Filter by table OIDs
  schemaNames?: string[]; // Filter by schema names
  all?: boolean; // Get all non-system policies
}

/**
 * Policy query functions
 */
export const policyQueries = {
  /**
   * Query policies with flexible filtering options
   */
  async query(client: pg.Client | pg.PoolClient, options: PolicyQueryOptions): Promise<PgPolicy[]> {
    let query = `
      SELECT
        p.oid,
        p.polname,
        p.polrelid,
        p.polcmd,
        coalesce(array_agg(r.rolname::text) filter (where r.rolname is not null), '{}') as polroles,
        pg_get_expr(p.polqual, p.polrelid) as polqual,
        pg_get_expr(p.polwithcheck, p.polrelid) as polwithcheck,
        n.nspname
      FROM pg_catalog.pg_policy p
      JOIN pg_catalog.pg_class c ON c.oid = p.polrelid
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_catalog.pg_roles r ON r.oid = ANY(p.polroles)
    `;

    const params: any[] = [];
    let paramIndex = 1;

    // Filter by OIDs if provided
    if (options.oids && options.oids.length > 0) {
      query += ` AND p.oid = ANY($${paramIndex})`;
      params.push(options.oids);
      paramIndex++;
    }

    // Filter by policy names if provided
    if (options.policyNames && options.policyNames.length > 0) {
      query += ` AND p.polname = ANY($${paramIndex})`;
      params.push(options.policyNames);
      paramIndex++;
    }

    // Filter by relation OIDs if provided
    if (options.policyRelids && options.policyRelids.length > 0) {
      query += ` AND p.polrelid = ANY($${paramIndex})`;
      params.push(options.policyRelids);
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

    query += `
      GROUP BY
        p.oid,
        p.polname,
        p.polrelid,
        p.polcmd,
        p.polqual,
        p.polwithcheck,
        n.nspname
      ORDER BY n.nspname, p.polname
    `;

    const result = await client.query(query, params.length > 0 ? params : []);

    return result.rows.map((row) => PgPolicySchema.parse(row));
  },

  /**
   * Get policy by OID
   */
  async byOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgPolicy | null> {
    return this.query(client, { oids: [oid] }).then((results) => results[0] || null);
  },

  /**
   * Get policies by relation OID (table)
   */
  async byRelationOid(client: pg.Client | pg.PoolClient, relid: number): Promise<PgPolicy[]> {
    return this.query(client, { policyRelids: [relid] });
  },

  /**
   * Get policy by name and schema
   */
  async byNameAndSchema(
    client: pg.Client | pg.PoolClient,
    schemaName: string,
    policyName: string
  ): Promise<PgPolicy | null> {
    return this.query(client, {
      policyNames: [policyName],
      schemaNames: [schemaName],
    }).then((results) => results[0] || null);
  },
};

/**
 * Create policy-related DataLoaders
 */
export function createPolicyLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading policies by OID
   */
  const policyLoader = new DataLoader<number, PgPolicy | null>(async (policyOids) => {
    const uniqueOids = [...new Set(policyOids)];

    const policies = await policyQueries.query(client, { oids: uniqueOids });

    // Create a map for quick lookups by OID
    const policyMap = new Map<number, PgPolicy>();
    policies.forEach((policy) => {
      policyMap.set(policy.oid, policy);
    });

    return policyOids.map((oid) => policyMap.get(oid) || null);
  });

  /**
   * DataLoader for loading policies by relation OID (table)
   */
  const policiesByRelationLoader = new DataLoader<number, PgPolicy[]>(async (relationOids) => {
    const uniqueOids = [...new Set(relationOids)];

    const policies = await policyQueries.query(client, {
      policyRelids: uniqueOids,
    });

    // Group policies by relation OID
    const policiesByRelation = new Map<number, PgPolicy[]>();
    uniqueOids.forEach((oid) => policiesByRelation.set(oid, []));

    policies.forEach((policy) => {
      const list = policiesByRelation.get(policy.polrelid) || [];
      list.push(policy);
      policiesByRelation.set(policy.polrelid, list);
    });

    return relationOids.map((oid) => policiesByRelation.get(oid) || []);
  });

  /**
   * Function to get all policies (with optional filtering)
   */
  const getAllPolicies = async (filter?: (p: PgPolicy) => boolean): Promise<PgPolicy[]> => {
    const policies = await policyQueries.query(client, { all: true });

    // Apply filter if provided
    return filter ? policies.filter(filter) : policies;
  };

  return {
    policyLoader,
    policiesByRelationLoader,
    getAllPolicies,
  };
}
