import { PgNamespace, PgNamespaceSchema } from "../types.js";
import pg from "pg";
import DataLoader from "dataloader";

/**
 * Interface for namespace query options
 */
export interface NamespaceQueryOptions {
  oids?: number[];    // Filter by OIDs
  names?: string[];   // Filter by names
  all?: boolean;      // Get all (excluding system schemas)
}

/**
 * Namespace query functions
 */
export const namespaceQueries = {
  /**
   * Query namespaces with flexible filtering options
   */
  async query(client: pg.Client | pg.PoolClient, options: NamespaceQueryOptions): Promise<PgNamespace[]> {
    let query = `
      SELECT oid, nspname, nspowner
      FROM pg_catalog.pg_namespace
      WHERE 1=1
    `;
    
    const params: any[] = [];
    let paramIndex = 1;
    
    // Filter by OIDs if provided
    if (options.oids && options.oids.length > 0) {
      query += ` AND oid = ANY($${paramIndex})`;
      params.push(options.oids);
      paramIndex++;
    }
    
    // Filter by names if provided
    if (options.names && options.names.length > 0) {
      query += ` AND nspname = ANY($${paramIndex})`;
      params.push(options.names);
      paramIndex++;
    }
    
    // Exclude system schemas for "all" queries
    if (options.all) {
      query += ` AND nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`;
    }
    
    query += ` ORDER BY nspname ASC`;
    
    const result = await client.query(query, params);
    
    return result.rows.map(row => PgNamespaceSchema.parse(row));
  }
};

/**
 * Create namespace-related DataLoaders
 */
export function createNamespaceLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading namespaces by OID
   */
  const namespaceLoader = new DataLoader<number, PgNamespace | null>(async (namespaceOids) => {
    const uniqueOids = [...new Set(namespaceOids)];
    
    const namespaces = await namespaceQueries.query(client, { oids: uniqueOids });
    
    // Create a map for quick lookups by OID
    const namespaceMap = new Map<number, PgNamespace>();
    namespaces.forEach(namespace => {
      namespaceMap.set(namespace.oid, namespace);
    });
    
    return namespaceOids.map(oid => namespaceMap.get(oid) || null);
  });
  
  /**
   * DataLoader for loading namespaces by name
   */
  const namespaceByNameLoader = new DataLoader<string, PgNamespace | null>(async (namespaceNames) => {
    const uniqueNames = [...new Set(namespaceNames)];
    
    const namespaces = await namespaceQueries.query(client, { names: uniqueNames });
    
    // Create a name-to-namespace map
    const nameMap = new Map<string, PgNamespace>();
    namespaces.forEach(namespace => {
      nameMap.set(namespace.nspname, namespace);
    });
    
    return namespaceNames.map(name => nameMap.get(name) || null);
  });
  
  /**
   * Function to get all namespaces (with optional filtering)
   */
  const getAllNamespaces = async (filter?: (ns: PgNamespace) => boolean): Promise<PgNamespace[]> => {
    const namespaces = await namespaceQueries.query(client, { all: true });
    
    // Apply filter if provided
    return filter ? namespaces.filter(filter) : namespaces;
  };
  
  return {
    namespaceLoader,
    namespaceByNameLoader,
    getAllNamespaces
  };
} 