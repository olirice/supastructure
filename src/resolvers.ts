import {
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
  PgForeignKeySchema,
} from "./types.js";
import { ReqContext, queries } from "./context.js";
import {
  decodeId,
  singleResultOrError,
  sortItems,
  buildGlobalId,
  paginate,
  limitPageSize,
} from "./generic.js";
export const resolvers = {
  Query: {
    database: async (
      _p: unknown,
      _a: unknown,
      ctx: ReqContext
    ): Promise<PgDatabase | null> => {
      return await ctx.resolveDatabase();
    },

    // single-entity queries for schema, table, etc.
    schema: async (
      _p: unknown,
      args: { schemaName?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgNamespace | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Schema") {
        return await queries.namespaceByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return await queries.namespaceByOid(ctx.client, args.oid);
      }
      if (args.schemaName) {
        return await queries.namespaceByName(ctx.client, args.schemaName);
      }
      return null;
    },

    table: async (
      _p: unknown,
      args: { schemaName?: string; name?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgClass | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Table") {
        const result = await queries.classByOid(ctx.client, fromId.oid);
        return result?.relkind === 'r' ? result : null;
      }
      if (args.oid) {
        const result = await queries.classByOid(ctx.client, args.oid);
        return result?.relkind === 'r' ? result : null;
      }
      if (args.schemaName && args.name) {
        const result = await queries.classByNameAndSchema(ctx.client, args.schemaName, args.name);
        return result?.relkind === 'r' ? result : null;
      }
      return null;
    },

    view: async (
      _p: unknown,
      args: { schemaName?: string; name?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgClass | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "View") {
        const result = await queries.classByOid(ctx.client, fromId.oid);
        return result?.relkind === 'v' ? result : null;
      }
      if (args.oid) {
        const result = await queries.classByOid(ctx.client, args.oid);
        return result?.relkind === 'v' ? result : null;
      }
      if (args.schemaName && args.name) {
        const result = await queries.classByNameAndSchema(ctx.client, args.schemaName, args.name);
        return result?.relkind === 'v' ? result : null;
      }
      return null;
    },

    materializedView: async (
      _p: unknown,
      args: { schemaName?: string; name?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgClass | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "MaterializedView") {
        const result = await queries.classByOid(ctx.client, fromId.oid);
        return result?.relkind === 'm' ? result : null;
      }
      if (args.oid) {
        const result = await queries.classByOid(ctx.client, args.oid);
        return result?.relkind === 'm' ? result : null;
      }
      if (args.schemaName && args.name) {
        const result = await queries.classByNameAndSchema(ctx.client, args.schemaName, args.name);
        return result?.relkind === 'm' ? result : null;
      }
      return null;
    },

    index: async (
      _p: unknown,
      args: { schemaName?: string; name?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgClass | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Index") {
        const result = await queries.classByOid(ctx.client, fromId.oid);
        return result?.relkind === 'i' ? result : null;
      }
      if (args.oid) {
        const result = await queries.classByOid(ctx.client, args.oid);
        return result?.relkind === 'i' ? result : null;
      }
      if (args.schemaName && args.name) {
        const result = await queries.classByNameAndSchema(ctx.client, args.schemaName, args.name);
        return result?.relkind === 'i' ? result : null;
      }
      return null;
    },

    trigger: async (
      _p: unknown,
      args: { id?: string; oid?: number, schemaName?: string; name?: string },
      ctx: ReqContext
    ): Promise<PgTrigger | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Trigger") {
        return await queries.triggerByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return await queries.triggerByOid(ctx.client, args.oid);
      }
      if (args.schemaName && args.name) {
        return await queries.triggersByNameAndSchema(ctx.client, args.schemaName, args.name);
      }
      return null;
    },

    policy: async (_p: unknown, args: any, ctx: ReqContext): Promise<PgPolicy | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Policy") {
        const policies = await ctx.resolvePolicies(p => p.oid === fromId.oid);
        return singleResultOrError(policies, "Policy");
      }
      if (args.oid) {
        const policies = await ctx.resolvePolicies(p => p.oid === args.oid);
        return singleResultOrError(policies, "Policy");
      }
      if (args.schemaName && args.name) {
        const namespaces = await ctx.resolveNamespaces(s => s.nspname === args.schemaName);
        const ns = namespaces.length > 0 ? namespaces[0] : null;
        if (!ns) return null;
        
        const classes = await ctx.resolveClasses();
        const policies = await ctx.resolvePolicies();
        
        const matched = policies.filter(po => {
          const c = classes.find(cl => cl.oid === po.polrelid);
          return c && c.relnamespace === ns.oid && po.polname === args.name;
        });
        
        return singleResultOrError(matched, "Policy");
      }
      return null;
    },

    type: async (
      _p: unknown,
      args: { schemaName?: string; name?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgType | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "PgType") {
        return await queries.typeByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return await queries.typeByOid(ctx.client, args.oid);
      }
      if (args.schemaName && args.name) {
        return await queries.typeByNameAndSchema(ctx.client, args.schemaName, args.name);
      }
      return null;
    },

    role: async (
      _p: unknown,
      args: { name?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgRole | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Role") {
        return await queries.roleByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return await queries.roleByOid(ctx.client, args.oid);
      }
      if (args.name) {
        return await queries.roleByName(ctx.client, args.name);
      }
      return null;
    },

    node: async (_p: unknown, args: { id: string }, ctx: ReqContext): Promise<any> => {
      const info = decodeId(args.id);
      switch (info.typeName) {
        case "Database": {
          const database = await ctx.resolveDatabase();
          return database.oid === info.oid ? database : null;
        }
        case "Schema": {
          // Use DataLoader to batch and cache namespace lookups by OID
          return ctx.namespaceLoader.load(info.oid);
        }
        case "Table": {
          // Use DataLoader to batch and cache class lookups by OID
          const cls = await ctx.classLoader.load(info.oid);
          return cls && cls.relkind === "r" ? cls : null;
        }
        case "View": {
          // Use DataLoader to batch and cache class lookups by OID
          const cls = await ctx.classLoader.load(info.oid);
          return cls && cls.relkind === "v" ? cls : null;
        }
        case "MaterializedView": {
          // Use DataLoader to batch and cache class lookups by OID
          const cls = await ctx.classLoader.load(info.oid);
          return cls && cls.relkind === "m" ? cls : null;
        }
        case "Index": {
          // Use DataLoader to batch and cache class lookups by OID
          const cls = await ctx.classLoader.load(info.oid);
          return cls && cls.relkind === "i" ? cls : null;
        }
        case "Trigger": {
          const triggers = await ctx.resolveTriggers(t => t.oid === info.oid);
          return triggers.length > 0 ? triggers[0] : null;
        }
        case "Policy": {
          const policies = await ctx.resolvePolicies(p => p.oid === info.oid);
          return policies.length > 0 ? policies[0] : null;
        }
        case "PgType": {
          // Use DataLoader to batch and cache type lookups by OID
          return ctx.typeLoader.load(info.oid);
        }
        case "Column": {
          // For columns, we need to get all attributes for a relation
          // and then find the specific one we're looking for
          // This is a bit different since we don't have a direct loader by column OID
          const attributes = await ctx.resolveAttributes(a => a.attrelid === info.oid);
          return attributes.length > 0 ? attributes[0] : null;
        }
        case "Role": {
          const roles = await ctx.resolveRoles(r => r.oid === info.oid);
          return roles.length > 0 ? roles[0] : null;
        }
        default:
          return null;
      }
    },
  },

  ////////////////////////////////////////
  // Connection resolvers (rename top-level arrays to "nodes")
  ////////////////////////////////////////
  SchemaConnection: {
    edges: (p: { edges: Array<{ node: PgNamespace }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgNamespace }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgNamespace }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  TableConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  ColumnConnection: {
    edges: (p: { edges: Array<{ node: PgAttribute }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgAttribute }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgAttribute }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  ViewConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  MaterializedViewConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  IndexConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  TriggerConnection: {
    edges: (p: { edges: Array<{ node: PgTrigger }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgTrigger }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgTrigger }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  PolicyConnection: {
    edges: (p: { edges: Array<{ node: PgPolicy }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgPolicy }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgPolicy }>; first: number }) =>
      p.edges.map((e) => e.node),
  },


  ////////////////////////////////////////
  // Field resolvers: Database, Schema, Table, etc.
  ////////////////////////////////////////

  Database: {
    id: (p: PgDatabase) => buildGlobalId("Database", p.oid),
    oid: (p: PgDatabase) => p.oid,
    name: (p: PgDatabase) => p.datname,
    schemas: async (p: PgDatabase, args: any, ctx: ReqContext): Promise<any> => {
      let items = await ctx.resolveNamespaces();
      if (args.orderBy?.field) {
        if (args.orderBy.field === "NAME") {
          sortItems(items, (x) => x.nspname, args.orderBy.direction);
        } else {
          sortItems(items, (x) => x.oid, args.orderBy.direction);
        }
      }
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    privileges: async (p: PgDatabase, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(`
        select pg_catalog.has_database_privilege($1, $2, 'connect') AS connect
      `, [args.roleName, p.datname]);

      const roles = await ctx.resolveRoles(r => r.rolname === args.roleName);
      return {
        role: roles.length > 0 ? roles[0] : null,
        connect: result.rows[0].connect,
      };
    },
  },

  Schema: {
    id: (p: PgNamespace) => buildGlobalId("Schema", p.oid),
    oid: (p: PgNamespace) => p.oid,
    name: (p: PgNamespace) => p.nspname,

    tables: async (p: PgNamespace, args: any, ctx: ReqContext): Promise<any> => {
      let items = await ctx.resolveClasses(c => c.relnamespace === p.oid && c.relkind === "r");
      if (args.orderBy?.field) {
        if (args.orderBy.field === "NAME") {
          sortItems(items, (x) => x.relname, args.orderBy.direction);
        } else {
          sortItems(items, (x) => x.oid, args.orderBy.direction);
        }
      }
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    views: async (p: PgNamespace, args: any, ctx: ReqContext): Promise<any> => {
      const items = await ctx.resolveClasses(c => c.relnamespace === p.oid && c.relkind === "v");
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    materializedViews: async (p: PgNamespace, args: any, ctx: ReqContext): Promise<any> => {
      const items = await ctx.resolveClasses(c => c.relnamespace === p.oid && c.relkind === "m");
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    privileges: async (p: PgNamespace, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(`
        select pg_catalog.has_schema_privilege($1, $2, 'USAGE') AS usage
      `, [args.roleName, p.nspname]);

      const roles = await ctx.resolveRoles(r => r.rolname === args.roleName);
      return {
        role: roles.length > 0 ? roles[0] : null,
        usage: result.rows[0].usage,
      };
    },
  },

  Table: {
    id: (p: PgClass) => buildGlobalId("Table", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    rowLevelSecurityEnabled: (p: PgClass) => p.relrowsecurity || false,
    schema: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache namespace lookups by OID
      return ctx.namespaceLoader.load(p.relnamespace);
    },
    columns: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache attribute lookups by relation OID
      const cols = await ctx.attributeLoader.load(p.oid) || [];
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
    indexes: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      const indexes = await ctx.resolveIndexes(ix => ix.indrelid === p.oid);
      
      // Use Promise.all with DataLoader to batch and cache class lookups by OID
      const indexClasses = await Promise.all(
        indexes.map(ix => ctx.classLoader.load(ix.indexrelid))
      );
      
      // Filter out null values and non-index classes
      const matched = indexClasses
        .filter(c => c && c.relkind === "i");
      
      return paginate(matched, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String((c as PgClass).oid),
      });
    },
    policies: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache policy lookups by table OID
      const policies = await ctx.policyLoader.load(p.oid) || [];
      return paginate(policies, {
        first: args.first,
        after: args.after,
        cursorForNode: (x) => String(x.oid),
      });
    },
    triggers: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache trigger lookups by table OID
      const triggers = await ctx.triggerLoader.load(p.oid) || [];
      return paginate(triggers, {
        first: args.first,
        after: args.after,
        cursorForNode: (x) => String(x.oid),
      });
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(`
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select,
               pg_catalog.has_table_privilege($1, $2::oid, 'INSERT') AS insert,
               pg_catalog.has_table_privilege($1, $2::oid, 'UPDATE') AS update,
               pg_catalog.has_table_privilege($1, $2::oid, 'DELETE') AS delete
      `, [args.roleName, p.oid]);

      const roles = await ctx.resolveRoles(r => r.rolname === args.roleName);
      return {
        role: roles.length > 0 ? roles[0] : null,
        select: result.rows[0].select,
        insert: result.rows[0].insert,
        update: result.rows[0].update,
        delete: result.rows[0].delete,
      };
    },
    foreignKeys: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      const items = await ctx.resolveForeignKeys(fk => fk.conrelid === p.oid);
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    referencedBy: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      const items = await ctx.resolveForeignKeys(fk => fk.confrelid === p.oid);
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
  },

  Column: {
    id: (p: PgAttribute) => buildGlobalId("Column", p.attrelid),
    name: (p: PgAttribute) => p.attname,
    attnum: (p: PgAttribute) => p.attnum,
    atttypid: (p: PgAttribute) => p.atttypid,
    table: async (p: PgAttribute, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache class lookups by OID
      return ctx.classLoader.load(p.attrelid);
    },
    type: async (p: PgAttribute, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache type lookups by OID
      return ctx.typeLoader.load(p.atttypid);
    },
    privileges: async (p: PgAttribute, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(`
        select pg_catalog.has_column_privilege($1, $2::oid, $3, 'SELECT') AS select,
               pg_catalog.has_column_privilege($1, $2::oid, $3, 'INSERT') AS insert,
               pg_catalog.has_column_privilege($1, $2::oid, $3, 'UPDATE') AS update
      `, [args.roleName, p.attrelid, p.attname]);

      const roles = await ctx.resolveRoles(r => r.rolname === args.roleName);
      return {
        role: roles.length > 0 ? roles[0] : null,
        select: result.rows[0].select,
        insert: result.rows[0].insert,
        update: result.rows[0].update,
      };
    },
  },

  View: {
    id: (p: PgClass) => buildGlobalId("View", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    schema: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache namespace lookups by OID
      return ctx.namespaceLoader.load(p.relnamespace);
    },
    columns: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache attribute lookups by relation OID
      const cols = await ctx.attributeLoader.load(p.oid) || [];
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(`
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select
      `, [args.roleName, p.oid]);

      const roles = await ctx.resolveRoles(r => r.rolname === args.roleName);
      return {
        role: roles.length > 0 ? roles[0] : null,
        select: result.rows[0].select,
      };
    },
  },

  MaterializedView: {
    id: (p: PgClass) => buildGlobalId("MaterializedView", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    schema: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache namespace lookups by OID
      return ctx.namespaceLoader.load(p.relnamespace);
    },
    isPopulated: (p: PgClass) =>
      typeof p.relispopulated === "boolean" ? p.relispopulated : false,
    columns: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache attribute lookups by relation OID
      const cols = await ctx.attributeLoader.load(p.oid) || [];
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(`
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select
      `, [args.roleName, p.oid]);

      const roles = await ctx.resolveRoles(r => r.rolname === args.roleName);
      return {
        role: roles.length > 0 ? roles[0] : null,
        select: result.rows[0].select,
      };
    },
  },

  Index: {
    id: (p: PgClass) => buildGlobalId("Index", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    schema: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache namespace lookups by OID
      return ctx.namespaceLoader.load(p.relnamespace);
    },
    table: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      const indexes = await ctx.resolveIndexes(x => x.indexrelid === p.oid);
      if (indexes.length === 0) return null;
      
      const ix = indexes[0];
      // Use DataLoader to batch and cache class lookups by OID
      return ctx.classLoader.load(ix.indrelid);
    },
    accessMethod: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      const indexes = await ctx.resolveIndexes(x => x.indexrelid === p.oid);
      return indexes.length > 0 ? indexes[0].indexam : "unknown";
    },
    definition: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      const indexes = await ctx.resolveIndexes(x => x.indexrelid === p.oid);
      return indexes.length > 0 ? indexes[0].indexdef : null;
    },
  },

  Trigger: {
    id: (p: PgTrigger) => buildGlobalId("Trigger", p.oid),
    oid: (p: PgTrigger) => p.oid,
    name: (p: PgTrigger) => p.tgname,
    table: async (p: PgTrigger, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache class lookups by OID
      return ctx.classLoader.load(p.tgrelid);
    },
  },

  Policy: {
    id: (p: PgPolicy) => buildGlobalId("Policy", p.oid),
    oid: (p: PgPolicy) => p.oid,
    name: (p: PgPolicy) => p.polname,
    table: async (p: PgPolicy, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache class lookups by OID
      return ctx.classLoader.load(p.polrelid);
    },
    command: (p: PgPolicy) => {
      switch (p.polcmd) {
        case "r":
          return "SELECT";
        case "a":
          return "INSERT";
        case "w":
          return "UPDATE";
        case "d":
          return "DELETE";
        case "*":
          return "ALL";
        default:
          return p.polcmd;
      }
    },
    roles: (p: PgPolicy) => p.polroles || [],
    usingExpr: (p: PgPolicy) => p.polqual || null,
    withCheck: (p: PgPolicy) => p.polwithcheck || null,
  },
  Role: {
    id: (p: PgRole) => buildGlobalId("Role", p.oid),
    oid: (p: PgRole) => p.oid,
    name: (p: PgRole) => p.rolname,
  },

  PgType: {
    __resolveType(obj: PgType) {
      return resolvePgType(obj);
    },
  },

  DomainType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "DOMAIN",
    baseType: async (p: PgType, _a: unknown, ctx: ReqContext): Promise<any> => {
      if (p.typbasetype && p.typbasetype !== 0) {
        // Use DataLoader to batch and cache type lookups by OID
        return ctx.typeLoader.load(p.typbasetype);
      }
      return null;
    },
  },
  EnumType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "ENUM",
    enumVariants: async (p: PgType, _a: unknown, ctx: ReqContext): Promise<any> => {
      const enums = await ctx.resolveEnums(e => e.enumtypid === p.oid);
      return enums.length > 0 ? enums[0].enumlabels : [];
    },
  },
  CompositeType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "COMPOSITE",
    fields: async (p: PgType, _a: unknown, ctx: ReqContext): Promise<any> => {
      if (!p.typrelid) return [];
      
      // Use DataLoader to batch and cache attribute lookups by relation OID
      const attrs = await ctx.attributeLoader.load(p.typrelid) || [];
      
      return Promise.all(attrs.map(async (a) => {
        // Use DataLoader to batch and cache type lookups by OID
        const type = await ctx.typeLoader.load(a.atttypid);
        return {
          name: a.attname,
          type: type,
          notNull: a.attnotnull,
        };
      }));
    },
  },
  ArrayType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "ARRAY",
    elementType: async (p: PgType, _a: unknown, ctx: ReqContext): Promise<any> => {
      if (p.typelem && p.typelem !== 0) {
        // Use DataLoader to batch and cache type lookups by OID
        return ctx.typeLoader.load(p.typelem);
      }
      return null;
    },
  },
  ScalarType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "SCALAR",
  },
  UnknownType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "UNKNOWN",
  },

  ////////////////////////////////////////
  // Node interface
  ////////////////////////////////////////
  Node: {
    __resolveType(obj: any) {
      if (obj.datname) return "Database";
      if (obj.nspname) return "Schema";
      if (obj.relname && obj.relkind === "r") return "Table";
      if (obj.relname && obj.relkind === "v") return "View";
      if (obj.relname && obj.relkind === "m") return "MaterializedView";
      if (obj.relname && obj.relkind === "i") return "Index";
      if (obj.tgname) return "Trigger";
      if (obj.polname) return "Policy";
      if (obj.typname !== undefined) {
        return resolvePgType(obj);
      }
      if (obj.attrelid !== undefined && obj.attname !== undefined) {
        return "Column";
      }
      if (obj.rolname !== undefined) {
        return "Role";
      }
      return null;
    },
  },

  ForeignKey: {
    id: (p: PgForeignKey) => buildGlobalId("ForeignKey", p.oid),
    oid: (p: PgForeignKey) => p.oid,
    name: (p: PgForeignKey) => p.conname,
    table: async (p: PgForeignKey, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache class lookups by OID
      return ctx.classLoader.load(p.conrelid);
    },
    referencedTable: async (p: PgForeignKey, _a: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache class lookups by OID
      return ctx.classLoader.load(p.confrelid);
    },
    updateAction: (p: PgForeignKey) => resolveForeignKeyAction(p.confupdtype),
    deleteAction: (p: PgForeignKey) => resolveForeignKeyAction(p.confdeltype),
    columnMappings: async (p: PgForeignKey, _a: any, ctx: ReqContext): Promise<any> => {
      // Load all attributes for the referencing and referenced tables
      const [referencingAttrs, referencedAttrs] = await Promise.all([
        ctx.attributeLoader.load(p.conrelid),
        ctx.attributeLoader.load(p.confrelid)
      ]);
      
      if (!referencingAttrs || !referencedAttrs) return [];
      
      return p.conkey.map((attnum: number, idx: number) => ({
        referencingColumn: referencingAttrs.find(a => a.attnum === attnum) || null,
        referencedColumn: referencedAttrs.find(a => a.attnum === p.confkey[idx]) || null,
      }));
    },
  },

  ForeignKeyConnection: {
    edges: (p: { edges: Array<{ node: PgForeignKey }>; first: number; pageInfo: any }) =>
      p.edges,
    pageInfo: (p: { edges: Array<{ node: PgForeignKey }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgForeignKey }>; first: number }) =>
      p.edges.map((e) => e.node),
  },
}

function resolvePgType(obj: PgType): string {
  if (obj.typtype === 'd') return 'DomainType';
  if (obj.typtype === 'e') return 'EnumType'; 
  if (obj.typtype === 'c') return 'CompositeType';
  if (obj.typtype === 'b') {
    if (obj.typelem && obj.typelem !== 0) return 'ArrayType';
    return 'ScalarType';
  }
  return 'UnknownType';
}

function resolveForeignKeyAction(action: string): string {
  switch (action) {
    case 'a': return 'NO_ACTION';
    case 'r': return 'RESTRICT';
    case 'c': return 'CASCADE';
    case 'n': return 'SET_NULL';
    case 'd': return 'SET_DEFAULT';
    default: return 'NO_ACTION';
  }
}
