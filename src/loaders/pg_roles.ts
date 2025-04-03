import pg from "pg";
import DataLoader from "dataloader";
import { PgRole } from "../types.js";

/**
 * Interface for role query filtering options
 */
export interface RoleQueryOptions {
  /** Filter by role OIDs */
  roleOids?: number[];
  /** Filter by role names */
  roleNames?: string[];
  /** Only include superusers */
  onlySuperusers?: boolean;
}

/**
 * Database query functions for PostgreSQL roles
 */
export const roleQueries = {
  /**
   * Query roles with various filtering options
   */
  async query(
    client: pg.Client | pg.PoolClient,
    options: RoleQueryOptions = {}
  ): Promise<PgRole[]> {
    const { roleOids, roleNames, onlySuperusers } = options;

    // Build the WHERE clause based on filter options
    const conditions: string[] = [];
    const params: any[] = [];

    if (roleOids && roleOids.length > 0) {
      conditions.push(`r.oid = ANY($${params.length + 1})`);
      params.push(roleOids);
    }

    if (roleNames && roleNames.length > 0) {
      conditions.push(`r.rolname = ANY($${params.length + 1})`);
      params.push(roleNames);
    }

    if (onlySuperusers) {
      conditions.push(`r.rolsuper = true`);
    }

    // Build and execute the SQL query
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const result = await client.query(
      `
      SELECT oid, rolname, rolsuper
      FROM pg_catalog.pg_roles r
      ${whereClause}
      ORDER BY rolname
      `,
      params
    );

    return result.rows;
  },

  /**
   * Get a single role by OID
   */
  async byOid(client: pg.Client | pg.PoolClient, oid: number): Promise<PgRole | null> {
    const roles = await this.query(client, { roleOids: [oid] });
    return roles.length > 0 ? roles[0] : null;
  },

  /**
   * Get a single role by name
   */
  async byName(client: pg.Client | pg.PoolClient, name: string): Promise<PgRole | null> {
    const roles = await this.query(client, { roleNames: [name] });
    return roles.length > 0 ? roles[0] : null;
  }
};

/**
 * Create DataLoaders for PostgreSQL roles
 * @param client - PostgreSQL database client
 * @returns DataLoaders and utility functions for efficient role queries
 */
export function createRoleLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading roles by OID
   */
  const roleLoader = new DataLoader<number, PgRole | null>(async (oids) => {
    const roles = await roleQueries.query(client, { roleOids: [...oids] });
    
    // Create a map for fast lookup by OID
    const roleMap = new Map<number, PgRole>();
    roles.forEach(role => {
      roleMap.set(role.oid, role);
    });
    
    // Return roles in the same order as requested OIDs
    return oids.map(oid => roleMap.get(oid) || null);
  });

  /**
   * DataLoader for loading roles by name
   */
  const roleByNameLoader = new DataLoader<string, PgRole | null>(async (names) => {
    const roles = await roleQueries.query(client, { roleNames: [...names] });
    
    // Create a map for fast lookup by name
    const roleMap = new Map<string, PgRole>();
    roles.forEach(role => {
      roleMap.set(role.rolname, role);
    });
    
    // Return roles in the same order as requested names
    return names.map(name => roleMap.get(name) || null);
  });

  /**
   * Function to get all roles with optional filtering
   */
  const getAllRoles = async (filter?: (role: PgRole) => boolean): Promise<PgRole[]> => {
    const roles = await roleQueries.query(client);
    return filter ? roles.filter(filter) : roles;
  };

  return {
    roleLoader,
    roleByNameLoader,
    getAllRoles,
  };
} 