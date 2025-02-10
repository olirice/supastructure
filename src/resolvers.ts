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
} from "./types.js";
import { ReqContext } from "./context.js";
import {
  decodeId,
  singleResultOrError,
  sortItems,
  buildGlobalId,
  paginate,
  findTypeNamespaceOid,
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
    schema: (
      _p: unknown,
      args: { schemaName?: string; id?: string; oid?: number },
      ctx: ReqContext
    ): PgNamespace | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Schema") {
        const matched = ctx.pg_namespaces.filter((s) => s.oid === fromId.oid);
        return singleResultOrError(matched, "Schema");
      }
      if (args.oid) {
        const matched = ctx.pg_namespaces.filter((s) => s.oid === args.oid);
        return singleResultOrError(matched, "Schema");
      }
      if (args.schemaName) {
        const matched = ctx.pg_namespaces.filter(
          (s) => s.nspname === args.schemaName
        );
        return singleResultOrError(matched, "Schema");
      }
      return null;
    },

    table: (_p: unknown, args: any, ctx: ReqContext): PgClass | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Table") {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === fromId.oid && c.relkind === "r"
        );
        return singleResultOrError(matched, "Table");
      }
      if (args.oid) {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === args.oid && c.relkind === "r"
        );
        return singleResultOrError(matched, "Table");
      }
      if (args.schemaName && args.name) {
        const ns = ctx.pg_namespaces.find((s) => s.nspname === args.schemaName);
        if (!ns) return null;
        const matched = ctx.pg_classes.filter(
          (c) =>
            c.relkind === "r" &&
            c.relname === args.name &&
            c.relnamespace === ns.oid
        );
        return singleResultOrError(matched, "Table");
      }
      return null;
    },

    view: (_p: unknown, args: any, ctx: ReqContext): PgClass | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "View") {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === fromId.oid && c.relkind === "v"
        );
        return singleResultOrError(matched, "View");
      }
      if (args.oid) {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === args.oid && c.relkind === "v"
        );
        return singleResultOrError(matched, "View");
      }
      if (args.schemaName && args.name) {
        const ns = ctx.pg_namespaces.find((s) => s.nspname === args.schemaName);
        if (!ns) return null;
        const matched = ctx.pg_classes.filter(
          (c) =>
            c.relkind === "v" &&
            c.relname === args.name &&
            c.relnamespace === ns.oid
        );
        return singleResultOrError(matched, "View");
      }
      return null;
    },

    materializedView: (
      _p: unknown,
      args: any,
      ctx: ReqContext
    ): PgClass | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "MaterializedView") {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === fromId.oid && c.relkind === "m"
        );
        return singleResultOrError(matched, "MaterializedView");
      }
      if (args.oid) {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === args.oid && c.relkind === "m"
        );
        return singleResultOrError(matched, "MaterializedView");
      }
      if (args.schemaName && args.name) {
        const ns = ctx.pg_namespaces.find((s) => s.nspname === args.schemaName);
        if (!ns) return null;
        const matched = ctx.pg_classes.filter(
          (c) =>
            c.relkind === "m" &&
            c.relname === args.name &&
            c.relnamespace === ns.oid
        );
        return singleResultOrError(matched, "MaterializedView");
      }
      return null;
    },

    index: (_p: unknown, args: any, ctx: ReqContext): PgClass | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Index") {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === fromId.oid && c.relkind === "i"
        );
        return singleResultOrError(matched, "Index");
      }
      if (args.oid) {
        const matched = ctx.pg_classes.filter(
          (c) => c.oid === args.oid && c.relkind === "i"
        );
        return singleResultOrError(matched, "Index");
      }
      if (args.schemaName && args.name) {
        const ns = ctx.pg_namespaces.find((s) => s.nspname === args.schemaName);
        if (!ns) return null;
        const matched = ctx.pg_classes.filter(
          (c) =>
            c.relkind === "i" &&
            c.relname === args.name &&
            c.relnamespace === ns.oid
        );
        return singleResultOrError(matched, "Index");
      }
      return null;
    },

    trigger: (_p: unknown, args: any, ctx: ReqContext): PgTrigger | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Trigger") {
        const matched = ctx.pg_triggers.filter((t) => t.oid === fromId.oid);
        return singleResultOrError(matched, "Trigger");
      }
      if (args.oid) {
        const matched = ctx.pg_triggers.filter((t) => t.oid === args.oid);
        return singleResultOrError(matched, "Trigger");
      }
      if (args.schemaName && args.name) {
        const ns = ctx.pg_namespaces.find((s) => s.nspname === args.schemaName);
        if (!ns) return null;
        const matched = ctx.pg_triggers.filter((tr) => {
          const c = ctx.pg_classes.find((cl) => cl.oid === tr.tgrelid);
          return c && c.relnamespace === ns.oid && tr.tgname === args.name;
        });
        return singleResultOrError(matched, "Trigger");
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

    type: (_p: unknown, args: any, ctx: ReqContext): PgType | null => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "PgType") {
        const matched = ctx.pg_types.filter((t) => t.oid === fromId.oid);
        return singleResultOrError(matched, "PgType");
      }
      if (args.oid) {
        const matched = ctx.pg_types.filter((t) => t.oid === args.oid);
        return singleResultOrError(matched, "PgType");
      }
      if (args.schemaName && args.name) {
        const ns = ctx.pg_namespaces.find((n) => n.nspname === args.schemaName);
        if (!ns) return null;
        // check composite
        const maybeClass = ctx.pg_classes.find(
          (c) => c.relname === args.name && c.relnamespace === ns.oid
        );
        if (maybeClass) {
          const matched = ctx.pg_types.filter(
            (ty) => ty.typrelid === maybeClass.oid
          );
          return singleResultOrError(matched, "PgType");
        }
        const matched = ctx.pg_types.filter((ty) => ty.typname === args.name);
        return singleResultOrError(matched, "PgType");
      }
      return null;
    },

    role: (_p: unknown, args: any, ctx: ReqContext): PgRole | null => {
      // example single-result role lookup
      // if ID is specified:
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Role") {
        const match = ctx.pg_roles.filter((r) => r.oid === fromId.oid);
        return singleResultOrError(match, "Role");
      }
      if (args.oid) {
        const match = ctx.pg_roles.filter((r) => r.oid === args.oid);
        return singleResultOrError(match, "Role");
      }
      if (args.name) {
        const match = ctx.pg_roles.filter((r) => r.rolname === args.name);
        return singleResultOrError(match, "Role");
      }
      return null;
    },

    node: (_p: unknown, args: { id: string }, ctx: ReqContext) => {
      const info = decodeId(args.id);
      if (!info) return null;
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
          return ctx.pg_attributes.find((a) => a.attrelid === info.oid) || null;
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

  RoleConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },

  SchemaConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  TableConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  ColumnConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  ViewConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  MaterializedViewConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  IndexConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  TriggerConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  PolicyConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  PgTypeConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },

  ////////////////////////////////////////
  // Privilege connections
  ////////////////////////////////////////

  SchemaPrivilegeConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
  },
  TablePrivilegeConnection: {
    edges: (p: any) => p.edges,
    pageInfo: (p: any) => p.pageInfo,
    nodes: (p: any) => p.edges.map((e: any) => e.node),
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
    indexes: (p: PgNamespace, args: any, ctx: ReqContext) => {
      const items = ctx.pg_classes.filter(
        (c) => c.relnamespace === p.oid && c.relkind === "i"
      );
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    triggers: (p: PgNamespace, args: any, ctx: ReqContext) => {
      const items = ctx.pg_triggers.filter((t) => {
        const c = ctx.pg_classes.find((cl) => cl.oid === t.tgrelid);
        return c && c.relnamespace === p.oid;
      });
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    policies: (p: PgNamespace, args: any, ctx: ReqContext) => {
      const items = ctx.pg_policies.filter((pol) => {
        const c = ctx.pg_classes.find((cl) => cl.oid === pol.polrelid);
        return c && c.relnamespace === p.oid;
      });
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    types: (p: PgNamespace, args: any, ctx: ReqContext) => {
      const schemaTypes = ctx.pg_types.filter(
        (t) => findTypeNamespaceOid(t, ctx) === p.oid
      );
      return paginate(schemaTypes, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
  },

  Table: {
    id: (p: PgClass) => buildGlobalId("Table", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    relkind: (p: PgClass) => p.relkind,
    schema: (p: PgClass, _a: any, ctx: ReqContext) =>
      ctx.pg_namespaces.find((n) => n.oid === p.relnamespace) || null,
    columns: (p: PgClass, args: any, ctx: ReqContext) => {
      const cols = ctx.pg_attributes
        .filter((col) => col.attrelid === p.oid)
        .map((col) => ({
          attrelid: col.attrelid,
          name: col.attname,
          attnum: col.attnum,
          atttypid: col.atttypid,
        }));
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
  },

  Column: {
    id: (p: { attrelid: number }) => buildGlobalId("Column", p.attrelid),
    table: (p: { attrelid: number }, _a: any, ctx: ReqContext) =>
      ctx.pg_classes.find((c) => c.oid === p.attrelid) || null,
    type: (p: { atttypid: number }, _a: any, ctx: ReqContext) =>
      ctx.pg_types.find((t) => t.oid === p.atttypid) || null,
  },

  View: {
    id: (p: PgClass) => buildGlobalId("View", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    relkind: (p: PgClass) => p.relkind,
    schema: (p: PgClass, _a: any, ctx: ReqContext) =>
      ctx.pg_namespaces.find((n) => n.oid === p.relnamespace) || null,
    columns: (p: PgClass, args: any, ctx: ReqContext) => {
      const cols = ctx.pg_attributes
        .filter((col) => col.attrelid === p.oid)
        .map((col) => ({
          attrelid: col.attrelid,
          name: col.attname,
          attnum: col.attnum,
          atttypid: col.atttypid,
        }));
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
  },

  MaterializedView: {
    id: (p: PgClass) => buildGlobalId("MaterializedView", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    relkind: (p: PgClass) => p.relkind,
    schema: (p: PgClass, _a: any, ctx: ReqContext) =>
      ctx.pg_namespaces.find((n) => n.oid === p.relnamespace) || null,
    populated: (p: PgClass) =>
      typeof p.relispopulated === "boolean" ? p.relispopulated : false,
    columns: (p: PgClass, args: any, ctx: ReqContext) => {
      const cols = ctx.pg_attributes
        .filter((col) => col.attrelid === p.oid)
        .map((col) => ({
          attrelid: col.attrelid,
          name: col.attname,
          attnum: col.attnum,
          atttypid: col.atttypid,
        }));
      return paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String(c.attrelid),
      });
    },
  },

  Index: {
    id: (p: PgClass) => buildGlobalId("Index", p.oid),
    oid: (p: PgClass) => p.oid,
    name: (p: PgClass) => p.relname,
    relkind: (p: PgClass) => p.relkind,
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

  PgType: {
    __resolveType(obj: PgType) {
      if (obj.typtype === "d") return "DomainType";
      if (obj.typtype === "e") return "EnumType";
      if (obj.typtype === "c") return "CompositeType";
      if (obj.typtype === "b") {
        if (obj.typelem && obj.typelem !== 0) return "ArrayType";
        return "ScalarType";
      }
      return "UnknownType";
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
      const relevant = ctx.pg_enums.filter((e) => e.enumtypid === p.oid);
      return relevant.map((e) => e.enumlabel);
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
        return "PgType";
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
};
