import { resolvers } from "../src/resolvers.js";
import { ReqContext } from "../src/context.js";
import { PgType, PgNamespace, PgClass, PgAttribute } from "../src/types.js";
import DataLoader from "dataloader";

function createTestContext(overrides: Partial<ReqContext> = {}): ReqContext {
  const database = { oid: 1, datname: "test_db" };
  const namespaces: PgNamespace[] = [];
  const classes: any[] = [];
  const attributes: any[] = [];
  const policies: any[] = [];
  const roles: any[] = [];
  const triggers: any[] = [];
  const types: any[] = [];
  const enums: any[] = [];
  const indexes: any[] = [];
  const foreignKeys: any[] = [];
  
  const dataSources = {
    database,
    classes,
    attributes,
    policies,
    roles,
    triggers,
    types,
    enums,
    indexes,
    foreignKeys,
  };
  
  // Create mock DataLoaders
  const typeLoader = new DataLoader<number, PgType | null>(async (keys) => {
    return keys.map(key => {
      const type = types.find(t => t.oid === key);
      return type || null;
    });
  });
  
  const namespaceLoader = new DataLoader<number, PgNamespace | null>(async (keys) => {
    return keys.map(key => {
      const namespace = namespaces.find(n => n.oid === key);
      return namespace || null;
    });
  });
  
  const namespaceByNameLoader = new DataLoader<string, PgNamespace | null>(async (keys) => {
    return keys.map(key => {
      const namespace = namespaces.find(n => n.nspname === key);
      return namespace || null;
    });
  });
  
  const classLoader = new DataLoader<number, PgClass | null>(async (keys) => {
    return keys.map(key => {
      const cls = classes.find(c => c.oid === key);
      return cls || null;
    });
  });
  
  // Add classByNameLoader
  const classByNameLoader = new DataLoader<
    { schema: string, name: string }, 
    PgClass | null
  >(async (keys) => {
    return keys.map(key => {
      const namespace = namespaces.find(n => n.nspname === key.schema);
      if (!namespace) return null;
      
      const cls = classes.find(c => 
        c.relnamespace === namespace.oid && 
        c.relname === key.name
      );
      return cls || null;
    });
  });
  
  // Add classesByNamespaceLoader
  const classesByNamespaceLoader = new DataLoader<
    { namespaceOid: number, relkind?: string },
    PgClass[]
  >(async (keys) => {
    return keys.map(key => {
      const filteredClasses = classes.filter(c => {
        if (c.relnamespace !== key.namespaceOid) return false;
        if (key.relkind && c.relkind !== key.relkind) return false;
        return true;
      });
      return filteredClasses;
    });
  });
  
  const attributeLoader = new DataLoader<number, PgAttribute[] | null>(async (keys) => {
    return keys.map(key => {
      const attrs = attributes.filter(a => a.attrelid === key);
      return attrs.length > 0 ? attrs : null;
    });
  });
  
  const triggerLoader = new DataLoader<number, any[] | null>(async (keys) => {
    return keys.map(key => {
      const tableTriggers = triggers.filter(t => t.tgrelid === key);
      return tableTriggers.length > 0 ? tableTriggers : null;
    });
  });
  
  const policyLoader = new DataLoader<number, any[] | null>(async (keys) => {
    return keys.map(key => {
      const tablePolicies = policies.filter(p => p.polrelid === key);
      return tablePolicies.length > 0 ? tablePolicies : null;
    });
  });
  
  return {
    resolveDatabase: jest.fn().mockResolvedValue(database),
    resolveNamespaces: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? namespaces.filter(filter) : namespaces)),
    resolveClasses: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? classes.filter(filter) : classes)),
    resolveAttributes: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? attributes.filter(filter) : attributes)),
    resolvePolicies: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? policies.filter(filter) : policies)),
    resolveRoles: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? roles.filter(filter) : roles)),
    resolveTriggers: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? triggers.filter(filter) : triggers)),
    resolveTypes: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? types.filter(filter) : types)),
    resolveEnums: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? enums.filter(filter) : enums)),
    resolveIndexes: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? indexes.filter(filter) : indexes)),
    resolveForeignKeys: jest.fn().mockImplementation((filter?: any) => 
      Promise.resolve(filter ? foreignKeys.filter(filter) : foreignKeys)),
    typeLoader,
    namespaceLoader,
    namespaceByNameLoader,
    classLoader,
    classByNameLoader,
    classesByNamespaceLoader,
    attributeLoader,
    triggerLoader,
    policyLoader,
    dataSources,
    client: {
      query: jest.fn().mockResolvedValue({ rows: [] }),
      release: jest.fn(),
      connect: jest.fn(),
      copyFrom: jest.fn(),
      copyTo: jest.fn(),
      end: jest.fn(),
      on: jest.fn(),
      off: jest.fn(),
    } as any,
    ...overrides,
  };
}

describe("Resolvers with null branches", () => {
  test("Table resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext();
    const namespaces = [{ oid: 1, nspname: "public" }];
    (ctx.resolveNamespaces as any).mockResolvedValue(namespaces);
    ctx.namespaceLoader.prime(1, namespaces[0]);
    ctx.namespaceByNameLoader.prime("public", namespaces[0]);
    
    ctx.dataSources.classes = [{ oid: 123, relname: "test_table", relnamespace: 2, relkind: "r", relrowsecurity: false }];

    const result = await resolvers.Table.schema(
      { oid: 123, relname: "test_table", relnamespace: 2, relkind: "r", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });

  test("View resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext();
    const namespaces = [{ oid: 1, nspname: "public" }];
    (ctx.resolveNamespaces as any).mockResolvedValue(namespaces);
    ctx.namespaceLoader.prime(1, namespaces[0]);
    ctx.namespaceByNameLoader.prime("public", namespaces[0]);
    
    ctx.dataSources.classes = [{ oid: 123, relname: "test_view", relnamespace: 2, relkind: "v", relrowsecurity: false }];

    const result = await resolvers.View.schema(
      { oid: 123, relname: "test_view", relnamespace: 2, relkind: "v", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });

  test("MaterializedView resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext();
    const namespaces = [{ oid: 1, nspname: "public" }];
    (ctx.resolveNamespaces as any).mockResolvedValue(namespaces);
    ctx.namespaceLoader.prime(1, namespaces[0]);
    ctx.namespaceByNameLoader.prime("public", namespaces[0]);
    
    ctx.dataSources.classes = [{ oid: 123, relname: "test_matview", relnamespace: 2, relkind: "m", relrowsecurity: false }];

    const result = await resolvers.MaterializedView.schema(
      { oid: 123, relname: "test_matview", relnamespace: 2, relkind: "m", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });

  test("Index resolver returns null for non-existent schema", async () => {
    const ctx = createTestContext();
    const namespaces = [{ oid: 1, nspname: "public" }];
    (ctx.resolveNamespaces as any).mockResolvedValue(namespaces);
    ctx.namespaceLoader.prime(1, namespaces[0]);
    ctx.namespaceByNameLoader.prime("public", namespaces[0]);
    
    ctx.dataSources.classes = [{ oid: 123, relname: "test_index", relnamespace: 2, relkind: "i", relrowsecurity: false }];

    const result = await resolvers.Index.schema(
      { oid: 123, relname: "test_index", relnamespace: 2, relkind: "i", relrowsecurity: false },
      {},
      ctx
    );

    expect(result).toBeNull();
  });
});
