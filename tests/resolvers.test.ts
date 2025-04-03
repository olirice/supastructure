import { resolvers } from "../src/resolvers.js";
import type { ReqContext } from "../src/context.js";
import type {
  PgType,
  PgNamespace,
  PgClass,
  PgAttribute,
  PgTrigger,
  PgPolicy,
  PgRole,
  PgEnum,
  PgIndex,
  PgForeignKey,
} from "../src/types.js";
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
    return keys.map((key) => {
      const type = types.find((t) => t.oid === key);
      return type || null;
    });
  });

  const namespaceLoader = new DataLoader<number, PgNamespace | null>(async (keys) => {
    return keys.map((key) => {
      const namespace = namespaces.find((n) => n.oid === key);
      return namespace || null;
    });
  });

  const namespaceByNameLoader = new DataLoader<string, PgNamespace | null>(async (keys) => {
    return keys.map((key) => {
      const namespace = namespaces.find((n) => n.nspname === key);
      return namespace || null;
    });
  });

  const classLoader = new DataLoader<number, PgClass | null>(async (keys) => {
    return keys.map((key) => {
      const cls = classes.find((c) => c.oid === key);
      return cls || null;
    });
  });

  // Add classByNameLoader
  const classByNameLoader = new DataLoader<{ schema: string; name: string }, PgClass | null>(
    async (keys) => {
      return keys.map((key) => {
        const namespace = namespaces.find((n) => n.nspname === key.schema);
        if (!namespace) return null;

        const cls = classes.find((c) => c.relnamespace === namespace.oid && c.relname === key.name);
        return cls || null;
      });
    }
  );

  // Add classesByNamespaceLoader
  const classesByNamespaceLoader = new DataLoader<
    { namespaceOid: number; relkind?: string },
    PgClass[]
  >(async (keys) => {
    return keys.map((key) => {
      const filteredClasses = classes.filter((c) => {
        if (c.relnamespace !== key.namespaceOid) return false;
        if (key.relkind && c.relkind !== key.relkind) return false;
        return true;
      });
      return filteredClasses;
    });
  });

  // Create attributeLoaders
  const attributesByRelationLoader = new DataLoader<number, PgAttribute[] | null>(async (keys) => {
    return keys.map((key) => {
      const attrs = attributes.filter((a) => a.attrelid === key);
      return attrs.length > 0 ? attrs : null;
    });
  });

  const attributesByTableNameLoader = new DataLoader<
    { schemaName: string; tableName: string },
    PgAttribute[] | null,
    string
  >(
    async (keys) => {
      return keys.map((key) => {
        // Find the class first
        const cls = classes.find((c) => {
          const ns = namespaces.find((n) => n.oid === c.relnamespace);
          return ns && ns.nspname === key.schemaName && c.relname === key.tableName;
        });

        if (!cls) return null;

        const attrs = attributes.filter((a) => a.attrelid === cls.oid);
        return attrs.length > 0 ? attrs : null;
      });
    },
    {
      cacheKeyFn: (key) => `${key.schemaName}.${key.tableName}`,
    }
  );

  const triggerLoader = new DataLoader<number, PgTrigger | null>(async (keys) => {
    return keys.map((key) => {
      const tableTriggers = triggers.filter((t) => t.tgrelid === key);
      return tableTriggers.length > 0 ? tableTriggers[0] : null;
    });
  });

  const triggersByRelationLoader = new DataLoader<number, PgTrigger[]>(async (keys) => {
    return keys.map((key) => {
      const tableTriggers = triggers.filter((t) => t.tgrelid === key);
      return tableTriggers.length > 0 ? tableTriggers : [];
    });
  });

  // PolicyLoader for loading by OID
  const policyLoader = new DataLoader<number, PgPolicy | null>(async (keys) => {
    return keys.map((key) => {
      const policy = policies.find((p) => p.oid === key);
      return policy || null;
    });
  });

  // PoliciesByRelationLoader for loading by table OID
  const policiesByRelationLoader = new DataLoader<number, PgPolicy[]>(async (keys) => {
    return keys.map((key) => {
      const tablePolicies = policies.filter((p) => p.polrelid === key);
      return tablePolicies.length > 0 ? tablePolicies : [];
    });
  });

  // Create typeByNameLoader
  const typeByNameLoader = new DataLoader<
    { schemaName: string; typeName: string },
    PgType | null,
    string
  >(
    async (keys) => {
      return keys.map((key) => {
        const namespace = namespaces.find((n) => n.nspname === key.schemaName);
        if (!namespace) return null;
        const type = types.find(
          (t) => t.typname === key.typeName && t.typnamespace === namespace.oid
        );
        return type || null;
      });
    },
    {
      cacheKeyFn: (key) => `${key.schemaName}.${key.typeName}`,
    }
  );

  // Create enumByTypeIdLoader
  const enumByTypeIdLoader = new DataLoader<number, PgEnum | null>(async (keys) => {
    return keys.map((key) => {
      const enum_ = enums.find((e) => e.enumtypid === key);
      return enum_ || null;
    });
  });

  // Create enumByNameLoader
  const enumByNameLoader = new DataLoader<
    { schemaName: string; enumName: string },
    PgEnum | null,
    string
  >(
    async (keys) => {
      return keys.map(() => null); // Mock implementation
    },
    {
      cacheKeyFn: (key) => `${key.schemaName}.${key.enumName}`,
    }
  );

  // Create indexLoader
  const indexLoader = new DataLoader<number, PgIndex | null>(async (keys) => {
    return keys.map((key) => {
      const index = indexes.find((i) => i.indexrelid === key);
      return index || null;
    });
  });

  // Create indexesByRelationLoader
  const indexesByRelationLoader = new DataLoader<number, PgIndex[]>(async (keys) => {
    return keys.map((key) => {
      const relationIndexes = indexes.filter((i) => i.indrelid === key);
      return relationIndexes;
    });
  });

  // Create roleLoader
  const roleLoader = new DataLoader<number, PgRole | null>(async (keys) => {
    return keys.map((key) => {
      const role = roles.find((r) => r.oid === key);
      return role || null;
    });
  });

  // Create roleByNameLoader
  const roleByNameLoader = new DataLoader<string, PgRole | null>(async (keys) => {
    return keys.map((key) => {
      const role = roles.find((r) => r.rolname === key);
      return role || null;
    });
  });

  // Create foreignKeyLoader
  const foreignKeyLoader = new DataLoader<number, PgForeignKey | null>(async (keys) => {
    return keys.map((key) => {
      const fk = foreignKeys.find((f) => f.oid === key);
      return fk || null;
    });
  });

  // Create foreignKeysByRelationLoader
  const foreignKeysByRelationLoader = new DataLoader<number, PgForeignKey[]>(async (keys) => {
    return keys.map((key) => {
      const relationFks = foreignKeys.filter((f) => f.conrelid === key);
      return relationFks;
    });
  });

  // Create foreignKeysByReferencedRelationLoader
  const foreignKeysByReferencedRelationLoader = new DataLoader<number, PgForeignKey[]>(
    async (keys) => {
      return keys.map((key) => {
        const referencedFks = foreignKeys.filter((f) => f.confrelid === key);
        return referencedFks;
      });
    }
  );

  return {
    resolveDatabase: jest.fn().mockResolvedValue(database),
    resolveNamespaces: jest
      .fn()
      .mockImplementation((filter?: any) =>
        Promise.resolve(filter ? namespaces.filter(filter) : namespaces)
      ),
    resolveClasses: jest
      .fn()
      .mockImplementation((filter?: any) =>
        Promise.resolve(filter ? classes.filter(filter) : classes)
      ),
    resolveAttributes: jest
      .fn()
      .mockImplementation((filter?: any) =>
        Promise.resolve(filter ? attributes.filter(filter) : attributes)
      ),
    resolvePolicies: jest
      .fn()
      .mockImplementation((filter?: any) =>
        Promise.resolve(filter ? policies.filter(filter) : policies)
      ),
    resolveRoles: jest
      .fn()
      .mockImplementation((filter?: any) => Promise.resolve(filter ? roles.filter(filter) : roles)),
    resolveTriggers: jest
      .fn()
      .mockImplementation((filter?: any) =>
        Promise.resolve(filter ? triggers.filter(filter) : triggers)
      ),
    resolveTypes: jest
      .fn()
      .mockImplementation((filter?: any) => Promise.resolve(filter ? types.filter(filter) : types)),
    resolveEnums: jest
      .fn()
      .mockImplementation((filter?: any) => Promise.resolve(filter ? enums.filter(filter) : enums)),
    resolveIndexes: jest
      .fn()
      .mockImplementation((filter?: any) =>
        Promise.resolve(filter ? indexes.filter(filter) : indexes)
      ),
    resolveForeignKeys: jest
      .fn()
      .mockImplementation((filter?: any) =>
        Promise.resolve(filter ? foreignKeys.filter(filter) : foreignKeys)
      ),
    typeLoader,
    typeByNameLoader,
    namespaceLoader,
    namespaceByNameLoader,
    classLoader,
    classByNameLoader,
    classesByNamespaceLoader,
    attributesByRelationLoader,
    attributesByTableNameLoader,
    triggerLoader,
    triggersByRelationLoader,
    policyLoader,
    policiesByRelationLoader,
    enumByTypeIdLoader,
    enumByNameLoader,
    indexLoader,
    indexesByRelationLoader,
    roleLoader,
    roleByNameLoader,
    foreignKeyLoader,
    foreignKeysByRelationLoader,
    foreignKeysByReferencedRelationLoader,
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

    ctx.dataSources.classes = [
      {
        oid: 123,
        relname: "test_table",
        relnamespace: 2,
        relkind: "r",
        relrowsecurity: false,
      },
    ];

    const result = await resolvers.Table.schema(
      {
        oid: 123,
        relname: "test_table",
        relnamespace: 2,
        relkind: "r",
        relrowsecurity: false,
      },
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

    ctx.dataSources.classes = [
      {
        oid: 123,
        relname: "test_view",
        relnamespace: 2,
        relkind: "v",
        relrowsecurity: false,
      },
    ];

    const result = await resolvers.View.schema(
      {
        oid: 123,
        relname: "test_view",
        relnamespace: 2,
        relkind: "v",
        relrowsecurity: false,
      },
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

    ctx.dataSources.classes = [
      {
        oid: 123,
        relname: "test_matview",
        relnamespace: 2,
        relkind: "m",
        relrowsecurity: false,
      },
    ];

    const result = await resolvers.MaterializedView.schema(
      {
        oid: 123,
        relname: "test_matview",
        relnamespace: 2,
        relkind: "m",
        relrowsecurity: false,
      },
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

    ctx.dataSources.classes = [
      {
        oid: 123,
        relname: "test_index",
        relnamespace: 2,
        relkind: "i",
        relrowsecurity: false,
      },
    ];

    const result = await resolvers.Index.schema(
      {
        oid: 123,
        relname: "test_index",
        relnamespace: 2,
        relkind: "i",
        relrowsecurity: false,
      },
      {},
      ctx
    );

    expect(result).toBeNull();
  });
});
