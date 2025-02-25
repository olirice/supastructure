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
    database: (
      _p: unknown,
      _a: unknown,
      ctx: ReqContext
    ): PgDatabase | null => {
      return ctx.pg_database;
    },

    // single-entity queries for schema, table, etc.
    schema: async (
      _p: unknown,
      args: { schemaName?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): Promise<PgNamespace | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Schema") {
        return queries.namespaceByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return queries.namespaceByOid(ctx.client, args.oid);
      }
      if (args.schemaName) {
        return queries.namespaceByName(ctx.client, args.schemaName);
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
        return queries.triggerByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return queries.triggerByOid(ctx.client, args.oid);
      }
      if (args.schemaName && args.name) {
        return queries.triggersByNameAndSchema(ctx.client, args.schemaName, args.name);
      }
      return null;
    },

    policy: (_p: unknown, args: any, ctx: ReqContext): PgPolicy | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Policy") {
        const matched = ctx.pg_policies.filter((p) => p.oid === fromId.oid);
        return singleResultOrError(matched, "Policy");
      }
      if (args.oid) {
        const matched = ctx.pg_policies.filter((p) => p.oid === args.oid);
        return singleResultOrError(matched, "Policy");
      }
      if (args.schemaName && args.name) {
        const ns = ctx.pg_namespaces.find((s) => s.nspname === args.schemaName);
        if (!ns) return null;
        const matched = ctx.pg_policies.filter((po) => {
          const c = ctx.pg_classes.find((cl) => cl.oid === po.polrelid);
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
        return queries.typeByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return queries.typeByOid(ctx.client, args.oid);
      }
      if (args.schemaName && args.name) {
        return queries.typeByNameAndSchema(ctx.client, args.schemaName, args.name);
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
        return queries.roleByOid(ctx.client, fromId.oid);
      }
      if (args.oid) {
        return queries.roleByOid(ctx.client, args.oid);
      }
      if (args.name) {
        return queries.roleByName(ctx.client, args.name);
      }
      return null;
    },

    node: (_p: unknown, args: { id: string }, ctx: ReqContext) => {
      const info = decodeId(args.id);
      switch (info.typeName) {
        case "Database":
          return ctx.pg_database.oid === info.oid ? ctx.pg_database : null;
        case "Schema":
          return ctx.pg_namespaces.find((s) => s.oid === info.oid) || null;
        case "Table":
          return (
            ctx.pg_classes.find(
              (c) => c.oid === info.oid && c.relkind === "r"
            ) || null
          );
        case "View":
          return (
            ctx.pg_classes.find(
              (c) => c.oid === info.oid && c.relkind === "v"
            ) || null
          );
        case "MaterializedView":
          return (
            ctx.pg_classes.find(
              (c) => c.oid === info.oid && c.relkind === "m"
            ) || null
          );
        case "Index":
          return (
            ctx.pg_classes.find(
              (c) => c.oid === info.oid && c.relkind === "i"
            ) || null
          );
        case "Trigger":
          return ctx.pg_triggers.find((t) => t.oid === info.oid) || null;
        case "Policy":
          return ctx.pg_policies.find((p) => p.oid === info.oid) || null;
        case "PgType":
          return ctx.pg_types.find((t) => t.oid === info.oid) || null;
        case "Column":
          return ctx.pg_attributes.find((t) => t.attrelid === info.oid) || null;
        case "Role":
          return ctx.pg_roles.find((r) => r.oid === info.oid) || null;
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
    schemas: (p: PgDatabase, args: any, ctx: ReqContext) => {
      let items = ctx.pg_namespaces;
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
    privileges: async (p: PgDatabase, args: { roleName: string }, ctx: ReqContext) => {
      const result = await ctx.client.query(`
        select pg_catalog.has_database_privilege($1, $2, 'connect') AS connect
      `, [args.roleName, p.datname]);

      return {
        role: ctx.pg_roles.find((r: PgRole) => r.rolname === args.roleName) || null,
        connect: result.rows[0].connect,
      };
    },
  },

  Schema: {
    id: (p: PgNamespace) => buildGlobalId("Schema", p.oid),
    oid: (p: PgNamespace) => p.oid,
    name: (p: PgNamespace) => p.nspname,

    tables: (p: PgNamespace, args: any, ctx: ReqContext) => {
      let items = ctx.pg_classes.filter(
        (c) => c.relnamespace === p.oid && c.relkind === "r"
      );
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
    views: (p: PgNamespace, args: any, ctx: ReqContext) => {
      const items = ctx.pg_classes.filter(
        (c) => c.relnamespace === p.oid && c.relkind === "v"
      );
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    materializedViews: (p: PgNamespace, args: any, ctx: ReqContext) => {
      const items = ctx.pg_classes.filter(
        (c) => c.relnamespace === p.oid && c.relkind === "m"
      );
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    privileges: async (p: PgNamespace, args: { roleName: string }, ctx: ReqContext) => {
      const result = await ctx.client.query(`
        select pg_catalog.has_schema_privilege($1, $2, 'USAGE') AS usage
      `, [args.roleName, p.nspname]);

      return {
        role: ctx.pg_roles.find((r) => r.rolname === args.roleName) || null,
        usage: result.rows[0].usage,
      };
    },
  },

  Table: {
    id: (p: PgClass) => buildGlobalId("Table", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    rowLevelSecurityEnabled: (p: PgClass) => p.relrowsecurity || false,
    schema: (p: PgClass, _a: any, ctx: ReqContext) =>
      ctx.pg_namespaces.find((n) => n.oid === p.relnamespace) || null,
    columns: (p: PgClass, args: any, ctx: ReqContext) => {
      const cols = ctx.pg_attributes
        .filter((col) => col.attrelid === p.oid);
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
    indexes: (p: PgClass, args: any, ctx: ReqContext) => {
      const allIx = ctx.pg_index.filter((ix) => ix.indrelid === p.oid);
      const matched = allIx
        .map((ix) =>
          ctx.pg_classes.find(
            (c) => c.oid === ix.indexrelid && c.relkind === "i"
          )
        )
        .filter(Boolean);
      return paginate(matched, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String((c as PgClass).oid),
      });
    },
    policies: (p: PgClass, args: any, ctx: ReqContext) => {
      const matched = ctx.pg_policies.filter((pol) => pol.polrelid === p.oid);
      return paginate(matched, {
        first: args.first,
        after: args.after,
        cursorForNode: (x) => String(x.oid),
      });
    },
    triggers: (p: PgClass, args: any, ctx: ReqContext) => {
      const matched = ctx.pg_triggers.filter((tr) => tr.tgrelid === p.oid);
      return paginate(matched, {
        first: args.first,
        after: args.after,
        cursorForNode: (x) => String(x.oid),
      });
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext) => {
      const result = await ctx.client.query(`
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select,
               pg_catalog.has_table_privilege($1, $2::oid, 'INSERT') AS insert,
               pg_catalog.has_table_privilege($1, $2::oid, 'UPDATE') AS update,
               pg_catalog.has_table_privilege($1, $2::oid, 'DELETE') AS delete
      `, [args.roleName, p.oid]);

      return {
        role: ctx.pg_roles.find((r) => r.rolname === args.roleName) || null,
        select: result.rows[0].select,
        insert: result.rows[0].insert,
        update: result.rows[0].update,
        delete: result.rows[0].delete,
      };
    },
    foreignKeys: (p: PgClass, args: any, ctx: ReqContext) => {
      const items = ctx.pg_foreign_keys.filter(
        (fk) => fk.conrelid === p.oid
      );
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    referencedBy: (p: PgClass, args: any, ctx: ReqContext) => {
      const items = ctx.pg_foreign_keys.filter(
        (fk) => fk.confrelid === p.oid
      );
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
    table: (p: PgAttribute, _a: any, ctx: ReqContext) =>
      ctx.pg_classes.find((c) => c.oid === p.attrelid) || null,
    type: (p: PgAttribute, _a: any, ctx: ReqContext) =>
      ctx.pg_types.find((t) => t.oid === p.atttypid) || null,
    privileges: async (p: PgAttribute, args: { roleName: string }, ctx: ReqContext) => {
      const result = await ctx.client.query(`
        select pg_catalog.has_column_privilege($1, $2::oid, $3, 'SELECT') AS select,
               pg_catalog.has_column_privilege($1, $2::oid, $3, 'INSERT') AS insert,
               pg_catalog.has_column_privilege($1, $2::oid, $3, 'UPDATE') AS update
      `, [args.roleName, p.attrelid, p.attname]);

      return {
        role: ctx.pg_roles.find((r) => r.rolname === args.roleName) || null,
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
    schema: (p: PgClass, _a: any, ctx: ReqContext) =>
      ctx.pg_namespaces.find((n) => n.oid === p.relnamespace) || null,
    columns: (p: PgClass, args: any, ctx: ReqContext) => {
      const cols = ctx.pg_attributes
        .filter((col) => col.attrelid === p.oid);
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext) => {
      const result = await ctx.client.query(`
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select
      `, [args.roleName, p.oid]);

      return {
        role: ctx.pg_roles.find((r) => r.rolname === args.roleName) || null,
        select: result.rows[0].select,
      };
    },
  },

  MaterializedView: {
    id: (p: PgClass) => buildGlobalId("MaterializedView", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    schema: (p: PgClass, _a: any, ctx: ReqContext) =>
      ctx.pg_namespaces.find((n) => n.oid === p.relnamespace) || null,
    isPopulated: (p: PgClass) =>
      typeof p.relispopulated === "boolean" ? p.relispopulated : false,
    columns: (p: PgClass, args: any, ctx: ReqContext) => {
      const cols = ctx.pg_attributes
        .filter((col) => col.attrelid === p.oid);
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext) => {
      const result = await ctx.client.query(`
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select
      `, [args.roleName, p.oid]);

      return {
        role: ctx.pg_roles.find((r) => r.rolname === args.roleName) || null,
        select: result.rows[0].select,
      };
    },
  },

  Index: {
    id: (p: PgClass) => buildGlobalId("Index", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    schema: (p: PgClass, _a: any, ctx: ReqContext) =>
      ctx.pg_namespaces.find((n) => n.oid === p.relnamespace) || null,
    table: (p: PgClass, _a: any, ctx: ReqContext) => {
      const ix = ctx.pg_index.find((x) => x.indexrelid === p.oid);
      if (!ix) return null;
      return ctx.pg_classes.find((c) => c.oid === ix.indrelid) || null;
    },
    accessMethod: (p: PgClass, _a: any, ctx: ReqContext) => {
      const ix = ctx.pg_index.find((x) => x.indexrelid === p.oid);
      return ix ? ix.indexam : "unknown";
    },
    definition: (p: PgClass, _a: any, ctx: ReqContext) => {
      const ix = ctx.pg_index.find((x) => x.indexrelid === p.oid);
      return ix?.indexdef || null;
    },
  },

  Trigger: {
    id: (p: PgTrigger) => buildGlobalId("Trigger", p.oid),
    oid: (p: PgTrigger) => p.oid,
    name: (p: PgTrigger) => p.tgname,
    table: (p: PgTrigger, _a: any, ctx: ReqContext) =>
      ctx.pg_classes.find((c) => c.oid === p.tgrelid) || null,
  },

  Policy: {
    id: (p: PgPolicy) => buildGlobalId("Policy", p.oid),
    oid: (p: PgPolicy) => p.oid,
    name: (p: PgPolicy) => p.polname,
    table: (p: PgPolicy, _a: any, ctx: ReqContext) =>
      ctx.pg_classes.find((c) => c.oid === p.polrelid) || null,
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
    baseType: (p: PgType, _a: unknown, ctx: ReqContext) => {
      if (p.typbasetype && p.typbasetype !== 0) {
        return ctx.pg_types.find((t) => t.oid === p.typbasetype) || null;
      }
      return null;
    },
  },
  EnumType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "ENUM",
    enumVariants: (p: PgType, _a: unknown, ctx: ReqContext) => {
      const relevant = ctx.pg_enums.find((e) => e.enumtypid === p.oid);
      return relevant ? relevant.enumlabels : [];
    },
  },
  CompositeType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "COMPOSITE",
    fields: (p: PgType, _a: unknown, ctx: ReqContext) => {
      if (!p.typrelid) return [];
      const attrs = ctx.pg_attributes.filter((a) => a.attrelid === p.typrelid);
      return attrs.map((a) => {
        return {
          name: a.attname,
          type: ctx.pg_types.find((tt) => tt.oid === a.atttypid) || null,
          notNull: a.attnotnull,
        };
      });
    },
  },
  ArrayType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "ARRAY",
    elementType: (p: PgType, _a: unknown, ctx: ReqContext) => {
      if (p.typelem && p.typelem !== 0) {
        return ctx.pg_types.find((t) => t.oid === p.typelem) || null;
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
    table: (p: PgForeignKey, _a: any, ctx: ReqContext) =>
      ctx.pg_classes.find((c) => c.oid === p.conrelid) || null,
    referencedTable: (p: PgForeignKey, _a: any, ctx: ReqContext) =>
      ctx.pg_classes.find((c) => c.oid === p.confrelid) || null,
    updateAction: (p: PgForeignKey) => resolveForeignKeyAction(p.confupdtype),
    deleteAction: (p: PgForeignKey) => resolveForeignKeyAction(p.confdeltype),
    columnMappings: (p: PgForeignKey, _a: any, ctx: ReqContext) => {
      return p.conkey.map((attnum: number, idx: number) => ({
        referencingColumn: ctx.pg_attributes.find(
          (a) => a.attrelid === p.conrelid && a.attnum === attnum
        ) || null,
        referencedColumn: ctx.pg_attributes.find(
          (a) => a.attrelid === p.confrelid && a.attnum === p.confkey[idx]
        ) || null,
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