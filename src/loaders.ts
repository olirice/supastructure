import { Client, PoolClient } from "pg";
import DataLoader from "dataloader";
import { createNamespaceLoaders } from "./loaders/pg_namespaces.js";
import { createClassLoaders } from "./loaders/pg_classes.js";
import { createAttributeLoaders } from "./loaders/pg_attributes.js";
import { createTriggerLoaders } from "./loaders/pg_triggers.js";
import { createPolicyLoaders } from "./loaders/pg_policies.js";
import { createTypeLoaders } from "./loaders/pg_types.js";
import { createEnumLoaders } from "./loaders/pg_enums.js";
import { createIndexLoaders } from "./loaders/pg_indexes.js";
import { createRoleLoaders } from "./loaders/pg_roles.js";
import { createForeignKeyLoaders } from "./loaders/pg_foreign_keys.js";
import type {
  PgDatabase,
  PgNamespace,
  PgClass,
  PgAttribute,
  PgTrigger,
  PgPolicy,
  PgType,
  PgEnum,
  PgIndex,
  PgRole,
  PgForeignKey,
} from "./types.js";

interface DataSources {
  database?: PgDatabase;
  namespaces?: PgNamespace[];
  classes?: PgClass[];
  attributes?: PgAttribute[];
  triggers?: PgTrigger[];
  policies?: PgPolicy[];
  types?: PgType[];
  enums?: PgEnum[];
  indexes?: PgIndex[];
  roles?: PgRole[];
  foreignKeys?: PgForeignKey[];
}

// Type guard functions for data sources
function hasNamespaces(ds: DataSources): ds is DataSources & { namespaces: PgNamespace[] } {
  return !!ds.namespaces;
}

function hasClasses(ds: DataSources): ds is DataSources & { classes: PgClass[] } {
  return !!ds.classes;
}

function hasAttributes(ds: DataSources): ds is DataSources & { attributes: PgAttribute[] } {
  return !!ds.attributes;
}

function hasTriggers(ds: DataSources): ds is DataSources & { triggers: PgTrigger[] } {
  return !!ds.triggers;
}

function hasPolicies(ds: DataSources): ds is DataSources & { policies: PgPolicy[] } {
  return !!ds.policies;
}

function hasTypes(ds: DataSources): ds is DataSources & { types: PgType[] } {
  return !!ds.types;
}

function hasEnums(ds: DataSources): ds is DataSources & { enums: PgEnum[] } {
  return !!ds.enums;
}

function hasIndexes(ds: DataSources): ds is DataSources & { indexes: PgIndex[] } {
  return !!ds.indexes;
}

function hasRoles(ds: DataSources): ds is DataSources & { roles: PgRole[] } {
  return !!ds.roles;
}

function hasForeignKeys(ds: DataSources): ds is DataSources & { foreignKeys: PgForeignKey[] } {
  return !!ds.foreignKeys;
}

/**
 * Creates all DataLoaders for PostgreSQL database entities.
 * Centralizes loader creation logic in one place for better organization.
 *
 * @param client - PostgreSQL client or pool client for database access
 * @returns Object containing all DataLoaders, resolver functions, and shared dataSources
 */
export function createLoaders(client: Client | PoolClient): {
  loaders: {
    namespaceLoader: DataLoader<number, PgNamespace | null>;
    namespaceByNameLoader: DataLoader<string, PgNamespace | null>;
    classLoader: DataLoader<number, PgClass | null>;
    classByNameLoader: DataLoader<{ schema: string; name: string }, PgClass | null>;
    classesByNamespaceLoader: DataLoader<{ namespaceOid: number; relkind?: string }, PgClass[]>;
    attributesByRelationLoader: DataLoader<number, PgAttribute[] | null>;
    attributesByTableNameLoader: DataLoader<
      { schemaName: string; tableName: string },
      PgAttribute[] | null,
      string
    >;
    triggerLoader: DataLoader<number, PgTrigger | null>;
    triggersByRelationLoader: DataLoader<number, PgTrigger[]>;
    policyLoader: DataLoader<number, PgPolicy | null>;
    policiesByRelationLoader: DataLoader<number, PgPolicy[]>;
    typeLoader: DataLoader<number, PgType | null>;
    typeByNameLoader: DataLoader<{ schemaName: string; typeName: string }, PgType | null, string>;
    enumByTypeIdLoader: DataLoader<number, PgEnum | null>;
    enumByNameLoader: DataLoader<{ schemaName: string; enumName: string }, PgEnum | null, string>;
    indexLoader: DataLoader<number, PgIndex | null>;
    indexesByRelationLoader: DataLoader<number, PgIndex[]>;
    roleLoader: DataLoader<number, PgRole | null>;
    roleByNameLoader: DataLoader<string, PgRole | null>;
    foreignKeyLoader: DataLoader<number, PgForeignKey | null>;
    foreignKeysByRelationLoader: DataLoader<number, PgForeignKey[]>;
    foreignKeysByReferencedRelationLoader: DataLoader<number, PgForeignKey[]>;
  };
  resolvers: {
    resolveNamespaces: (filter?: (ns: PgNamespace) => boolean) => Promise<PgNamespace[]>;
    resolveClasses: (filter?: (cls: PgClass) => boolean) => Promise<PgClass[]>;
    resolveAttributes: (filter?: (attr: PgAttribute) => boolean) => Promise<PgAttribute[]>;
    resolveTriggers: (filter?: (trigger: PgTrigger) => boolean) => Promise<PgTrigger[]>;
    resolvePolicies: (filter?: (policy: PgPolicy) => boolean) => Promise<PgPolicy[]>;
    resolveTypes: (filter?: (type: PgType) => boolean) => Promise<PgType[]>;
    resolveEnums: (filter?: (enum_: PgEnum) => boolean) => Promise<PgEnum[]>;
    resolveIndexes: (filter?: (index: PgIndex) => boolean) => Promise<PgIndex[]>;
    resolveRoles: (filter?: (role: PgRole) => boolean) => Promise<PgRole[]>;
    resolveForeignKeys: (filter?: (fk: PgForeignKey) => boolean) => Promise<PgForeignKey[]>;
  };
  dataSources: DataSources;
} {
  // Create data sources container that will be shared with the context
  const dataSources: DataSources = {};

  // Create namespace loaders
  const namespaceLoaders = createNamespaceLoaders(client);

  // Create class loaders
  const classLoaders = createClassLoaders(client);

  // Create attribute loaders
  const attributeLoaders = createAttributeLoaders(client);

  // Create trigger loaders
  const triggerLoaders = createTriggerLoaders(client);

  // Create policy loaders
  const policyLoaders = createPolicyLoaders(client);

  // Create type loaders
  const typeLoaders = createTypeLoaders(client);

  // Create enum loaders
  const enumLoaders = createEnumLoaders(client);

  // Create index loaders
  const indexLoaders = createIndexLoaders(client);

  // Create role loaders
  const roleLoaders = createRoleLoaders(client);

  // Create foreign key loaders
  const foreignKeyLoaders = createForeignKeyLoaders(client);

  // Create resolver functions with type-safe data source access
  const resolvers = {
    // Namespaces resolver with caching
    resolveNamespaces: async (filter?: (ns: PgNamespace) => boolean) => {
      if (hasNamespaces(dataSources) && !filter) {
        return dataSources.namespaces;
      }

      const namespaces = await namespaceLoaders.getAllNamespaces();
      if (!filter) {
        dataSources.namespaces = namespaces;
      }

      return filter ? namespaces.filter(filter) : namespaces;
    },

    // Classes resolver with caching
    resolveClasses: async (filter?: (cls: PgClass) => boolean) => {
      if (hasClasses(dataSources) && !filter) {
        return dataSources.classes;
      }

      const classes = await classLoaders.getAllClasses();
      if (!filter) {
        dataSources.classes = classes;
      }

      return filter ? classes.filter(filter) : classes;
    },

    // Attributes resolver with caching
    resolveAttributes: async (filter?: (attr: PgAttribute) => boolean) => {
      if (hasAttributes(dataSources) && !filter) {
        return dataSources.attributes;
      }

      const attributes = await attributeLoaders.getAllAttributes();
      if (!filter) {
        dataSources.attributes = attributes;
      }

      return filter ? attributes.filter(filter) : attributes;
    },

    // Triggers resolver with caching
    resolveTriggers: async (filter?: (trigger: PgTrigger) => boolean) => {
      if (hasTriggers(dataSources) && !filter) {
        return dataSources.triggers;
      }

      const triggers = await triggerLoaders.getAllTriggers();
      if (!filter) {
        dataSources.triggers = triggers;
      }

      return filter ? triggers.filter(filter) : triggers;
    },

    // Policies resolver with caching
    resolvePolicies: async (filter?: (policy: PgPolicy) => boolean) => {
      if (hasPolicies(dataSources) && !filter) {
        return dataSources.policies;
      }

      const policies = await policyLoaders.getAllPolicies();
      if (!filter) {
        dataSources.policies = policies;
      }

      return filter ? policies.filter(filter) : policies;
    },

    // Types resolver with caching
    resolveTypes: async (filter?: (type: PgType) => boolean) => {
      if (hasTypes(dataSources) && !filter) {
        return dataSources.types;
      }

      const types = await typeLoaders.getAllTypes();
      if (!filter) {
        dataSources.types = types;
      }

      return filter ? types.filter(filter) : types;
    },

    // Enums resolver with caching
    resolveEnums: async (filter?: (enum_: PgEnum) => boolean) => {
      if (hasEnums(dataSources) && !filter) {
        return dataSources.enums;
      }

      const enums = await enumLoaders.getAllEnums();
      if (!filter) {
        dataSources.enums = enums;
      }

      return filter ? enums.filter(filter) : enums;
    },

    // Indexes resolver with caching
    resolveIndexes: async (filter?: (index: PgIndex) => boolean) => {
      if (hasIndexes(dataSources) && !filter) {
        return dataSources.indexes;
      }

      const indexes = await indexLoaders.getAllIndexes();
      if (!filter) {
        dataSources.indexes = indexes;
      }

      return filter ? indexes.filter(filter) : indexes;
    },

    // Roles resolver with caching
    resolveRoles: async (filter?: (role: PgRole) => boolean) => {
      if (hasRoles(dataSources) && !filter) {
        return dataSources.roles;
      }

      const roles = await roleLoaders.getAllRoles();
      if (!filter) {
        dataSources.roles = roles;
      }

      return filter ? roles.filter(filter) : roles;
    },

    // Foreign keys resolver with caching
    resolveForeignKeys: async (filter?: (fk: PgForeignKey) => boolean) => {
      if (hasForeignKeys(dataSources) && !filter) {
        return dataSources.foreignKeys;
      }

      const foreignKeys = await foreignKeyLoaders.getAllForeignKeys();
      if (!filter) {
        dataSources.foreignKeys = foreignKeys;
      }

      return filter ? foreignKeys.filter(filter) : foreignKeys;
    },
  };

  // Collect all loaders in a flat structure
  const loaders = {
    // Namespace loaders
    namespaceLoader: namespaceLoaders.namespaceLoader,
    namespaceByNameLoader: namespaceLoaders.namespaceByNameLoader,

    // Class loaders
    classLoader: classLoaders.classLoader,
    classByNameLoader: classLoaders.classByNameLoader,
    classesByNamespaceLoader: classLoaders.classesByNamespaceLoader,

    // Attribute loaders
    attributesByRelationLoader: attributeLoaders.attributesByRelationLoader,
    attributesByTableNameLoader: attributeLoaders.attributesByTableNameLoader,

    // Trigger loaders
    triggerLoader: triggerLoaders.triggerLoader,
    triggersByRelationLoader: triggerLoaders.triggersByRelationLoader,

    // Policy loaders
    policyLoader: policyLoaders.policyLoader,
    policiesByRelationLoader: policyLoaders.policiesByRelationLoader,

    // Type loaders
    typeLoader: typeLoaders.typeLoader,
    typeByNameLoader: typeLoaders.typeByNameLoader,

    // Enum loaders
    enumByTypeIdLoader: enumLoaders.enumByTypeIdLoader,
    enumByNameLoader: enumLoaders.enumByNameLoader,

    // Index loaders
    indexLoader: indexLoaders.indexLoader,
    indexesByRelationLoader: indexLoaders.indexesByRelationLoader,

    // Role loaders
    roleLoader: roleLoaders.roleLoader,
    roleByNameLoader: roleLoaders.roleByNameLoader,

    // Foreign key loaders
    foreignKeyLoader: foreignKeyLoaders.foreignKeyLoader,
    foreignKeysByRelationLoader: foreignKeyLoaders.foreignKeysByRelationLoader,
    foreignKeysByReferencedRelationLoader: foreignKeyLoaders.foreignKeysByReferencedRelationLoader,
  };

  return { loaders, resolvers, dataSources };
}
