import { PgClass, PgClassSchema } from "../types.js";
import pg from "pg";
import DataLoader from "dataloader";

/**
 * Extended PgClass type that includes nspname
 */
interface PgClassWithSchema extends PgClass {
  nspname: string;
}

/**
 * Interface for class query options
 */
export interface ClassQueryOptions {
  oids?: number[];              // Filter by OIDs
  names?: { schema: string, name: string }[]; // Filter by schema name and class name
  namespaceOids?: number[];     // Filter by namespace OIDs
  relkinds?: string[];          // Filter by relation kinds (r=table, v=view, m=materialized view, etc.)
  all?: boolean;                // Get all (excluding system schemas)
}

/**
 * Class query functions
 */
export const classQueries = {
  /**
   * Query classes with flexible filtering options
   */
  async query(client: pg.Client | pg.PoolClient, options: ClassQueryOptions): Promise<PgClassWithSchema[]> {
    let query = `
      SELECT
        c.oid,
        c.relname,
        c.relnamespace,
        c.relkind,
        c.relispopulated,
        c.relrowsecurity,
        n.nspname
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
      WHERE 1=1
    `;
    
    const params: any[] = [];
    let paramIndex = 1;
    
    // Filter by OIDs if provided
    if (options.oids && options.oids.length > 0) {
      query += ` AND c.oid = ANY($${paramIndex})`;
      params.push(options.oids);
      paramIndex++;
    }
    
    // Filter by namespace OIDs if provided
    if (options.namespaceOids && options.namespaceOids.length > 0) {
      query += ` AND c.relnamespace = ANY($${paramIndex})`;
      params.push(options.namespaceOids);
      paramIndex++;
    }
    
    // Filter by relation kinds if provided
    if (options.relkinds && options.relkinds.length > 0) {
      query += ` AND c.relkind = ANY($${paramIndex})`;
      params.push(options.relkinds);
      paramIndex++;
    }
    
    // Filter by schema name and class name if provided
    if (options.names && options.names.length > 0) {
      const schemaNames = options.names.map(n => n.schema);
      const classNames = options.names.map(n => n.name);
      
      query += ` AND (n.nspname, c.relname) IN (`;
      
      const valuesList = options.names.map((_, idx) => 
        `($${paramIndex + idx * 2}, $${paramIndex + idx * 2 + 1})`
      ).join(', ');
      
      query += valuesList + ')';
      
      // Add all parameter values
      options.names.forEach(n => {
        params.push(n.schema, n.name);
      });
      
      paramIndex += options.names.length * 2;
    }
    
    // Exclude system schemas for "all" queries
    if (options.all) {
      query += ` AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema', 'pg_temp')`;
    }
    
    query += ` ORDER BY n.nspname, c.relname`;
    
    const result = await client.query(query, params);
    
    return result.rows.map(row => {
      const cls = PgClassSchema.parse(row);
      return { ...cls, nspname: row.nspname } as PgClassWithSchema;
    });
  }
};

/**
 * Create class-related DataLoaders
 */
export function createClassLoaders(client: pg.Client | pg.PoolClient) {
  /**
   * DataLoader for loading classes by OID
   */
  const classLoader = new DataLoader<number, PgClass | null>(async (classOids) => {
    const uniqueOids = [...new Set(classOids)];
    
    const classes = await classQueries.query(client, { oids: uniqueOids });
    
    // Create a map for quick lookups by OID
    const classMap = new Map<number, PgClass>();
    classes.forEach(cls => {
      // Store without nspname to match PgClass type
      const { nspname, ...classWithoutNspname } = cls;
      classMap.set(cls.oid, classWithoutNspname);
    });
    
    return classOids.map(oid => classMap.get(oid) || null);
  });
  
  /**
   * DataLoader for loading classes by schema name and relation name
   */
  const classByNameLoader = new DataLoader<
    { schema: string, name: string }, 
    PgClass | null
  >(async (keys) => {
    const uniqueKeys = keys.reduce((acc, key) => {
      const keyStr = `${key.schema}:${key.name}`;
      if (!acc.has(keyStr)) {
        acc.set(keyStr, key);
      }
      return acc;
    }, new Map<string, { schema: string, name: string }>());
    
    const uniqueNames = Array.from(uniqueKeys.values());
    
    const classes = await classQueries.query(client, { names: uniqueNames });
    
    // Create a map for quick lookups by schema:name
    const classMap = new Map<string, PgClass>();
    classes.forEach(cls => {
      const key = `${cls.nspname}:${cls.relname}`;
      // Store without nspname to match PgClass type
      const { nspname, ...classWithoutNspname } = cls;
      classMap.set(key, classWithoutNspname);
    });
    
    return keys.map(key => {
      const lookupKey = `${key.schema}:${key.name}`;
      return classMap.get(lookupKey) || null;
    });
  });
  
  /**
   * DataLoader for loading classes by namespace OID and optional relation kind
   */
  const classesByNamespaceLoader = new DataLoader<
    { namespaceOid: number, relkind?: string },
    PgClass[]
  >(async (keys) => {
    // Group keys by namespaceOid and relkind for efficient querying
    const namespaceGroups = new Map<string, { namespaceOid: number, relkinds: Set<string> }>();
    
    keys.forEach(key => {
      const namespaceKey = `${key.namespaceOid}`;
      if (!namespaceGroups.has(namespaceKey)) {
        namespaceGroups.set(namespaceKey, { 
          namespaceOid: key.namespaceOid, 
          relkinds: new Set()
        });
      }
      
      if (key.relkind) {
        namespaceGroups.get(namespaceKey)!.relkinds.add(key.relkind);
      }
    });
    
    // Build and execute queries for each namespace group
    const allClasses: PgClassWithSchema[] = [];
    
    for (const group of namespaceGroups.values()) {
      const options: ClassQueryOptions = {
        namespaceOids: [group.namespaceOid]
      };
      
      if (group.relkinds.size > 0) {
        options.relkinds = Array.from(group.relkinds);
      }
      
      const classes = await classQueries.query(client, options);
      allClasses.push(...classes);
    }
    
    // Create a map for easy lookup
    const classesMap = new Map<string, PgClass[]>();
    
    allClasses.forEach(cls => {
      // Strip nspname to match PgClass type
      const { nspname, ...classWithoutNspname } = cls;
      
      // Add to namespace-only keys
      const namespaceKey = `${cls.relnamespace}`;
      if (!classesMap.has(namespaceKey)) {
        classesMap.set(namespaceKey, []);
      }
      classesMap.get(namespaceKey)!.push(classWithoutNspname);
      
      // Add to namespace+relkind keys
      const namespaceRelkindKey = `${cls.relnamespace}:${cls.relkind}`;
      if (!classesMap.has(namespaceRelkindKey)) {
        classesMap.set(namespaceRelkindKey, []);
      }
      classesMap.get(namespaceRelkindKey)!.push(classWithoutNspname);
    });
    
    // Map back to original request order
    return keys.map(key => {
      const lookupKey = key.relkind 
        ? `${key.namespaceOid}:${key.relkind}`
        : `${key.namespaceOid}`;
      
      return classesMap.get(lookupKey) || [];
    });
  });
  
  /**
   * Function to get all classes (with optional filtering)
   */
  const getAllClasses = async (filter?: (cls: PgClass) => boolean): Promise<PgClass[]> => {
    const classes = await classQueries.query(client, { all: true });
    
    // Strip nspname to match PgClass type
    const classesWithoutNspname = classes.map(cls => {
      const { nspname, ...classWithoutNspname } = cls;
      return classWithoutNspname;
    });
    
    // Apply filter if provided
    return filter ? classesWithoutNspname.filter(filter) : classesWithoutNspname;
  };
  
  return {
    classLoader,
    classByNameLoader,
    classesByNamespaceLoader,
    getAllClasses
  };
} 