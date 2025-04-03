import type {
  PgDatabase,
  PgNamespace,
  PgClass,
  PgAttribute,
  PgTrigger,
  PgPolicy,
  PgType,
  PgRole,
  PgForeignKey,
} from "./types.js";
import { PgEnum, PgIndex, PgForeignKeySchema } from "./types.js";
import type { ReqContext } from "./context.js";
import { queries } from "./context.js";
import {
  decodeId,
  singleResultOrError,
  sortItems,
  buildGlobalId,
  paginate,
  limitPageSize,
} from "./generic.js";
import util from "util";
import { z } from "zod";
import { PgTypeSchema } from "./types.js";

// Define PaginationArgs interface
interface PaginationArgs {
  first?: number;
  after?: string;
}

export const resolvers = {
  Query: {
    database: async (_p: unknown, _a: unknown, ctx: ReqContext): Promise<PgDatabase | null> => {
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
        return ctx.namespaceLoader.load(fromId.oid);
      }
      if (args.oid) {
        return ctx.namespaceLoader.load(args.oid);
      }
      if (args.schemaName) {
        return ctx.namespaceByNameLoader.load(args.schemaName);
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
        const result = await ctx.classLoader.load(fromId.oid);
        return result?.relkind === "r" ? result : null;
      }
      if (args.oid) {
        const result = await ctx.classLoader.load(args.oid);
        return result?.relkind === "r" ? result : null;
      }
      if (args.schemaName && args.name) {
        // First get the namespace by name
        const namespace = await ctx.namespaceByNameLoader.load(args.schemaName);
        if (!namespace) return null;

        // Then find the class by name and namespace
        const classes = await ctx.resolveClasses();
        const match = classes.find(
          (c) => c.relnamespace === namespace.oid && c.relname === args.name && c.relkind === "r"
        );
        return match || null;
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
        const result = await ctx.classLoader.load(fromId.oid);
        return result?.relkind === "v" ? result : null;
      }
      if (args.oid) {
        const result = await ctx.classLoader.load(args.oid);
        return result?.relkind === "v" ? result : null;
      }
      if (args.schemaName && args.name) {
        // First get the namespace by name
        const namespace = await ctx.namespaceByNameLoader.load(args.schemaName);
        if (!namespace) return null;

        // Then find the class by name and namespace
        const classes = await ctx.resolveClasses();
        const match = classes.find(
          (c) => c.relnamespace === namespace.oid && c.relname === args.name && c.relkind === "v"
        );
        return match || null;
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
        const result = await ctx.classLoader.load(fromId.oid);
        return result?.relkind === "m" ? result : null;
      }
      if (args.oid) {
        const result = await ctx.classLoader.load(args.oid);
        return result?.relkind === "m" ? result : null;
      }
      if (args.schemaName && args.name) {
        // First get the namespace by name
        const namespace = await ctx.namespaceByNameLoader.load(args.schemaName);
        if (!namespace) return null;

        // Then find the class by name and namespace
        const classes = await ctx.resolveClasses();
        const match = classes.find(
          (c) => c.relnamespace === namespace.oid && c.relname === args.name && c.relkind === "m"
        );
        return match || null;
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
        const result = await ctx.classLoader.load(fromId.oid);
        return result?.relkind === "i" ? result : null;
      }
      if (args.oid) {
        const result = await ctx.classLoader.load(args.oid);
        return result?.relkind === "i" ? result : null;
      }
      if (args.schemaName && args.name) {
        // First get the namespace by name
        const namespace = await ctx.namespaceByNameLoader.load(args.schemaName);
        if (!namespace) return null;

        // Then find the class by name and namespace
        const classes = await ctx.resolveClasses();
        const match = classes.find(
          (c) => c.relnamespace === namespace.oid && c.relname === args.name && c.relkind === "i"
        );
        return match || null;
      }
      return null;
    },

    trigger: async (
      _p: unknown,
      args: { id?: string; oid?: number; schemaName?: string; name?: string },
      ctx: ReqContext
    ): Promise<PgTrigger | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Trigger") {
        // Search for trigger by oid
        const triggers = await ctx.resolveTriggers((t) => t.oid === fromId.oid);
        return triggers.length > 0 ? triggers[0] : null;
      }
      if (args.oid) {
        // Search for trigger by oid
        const triggers = await ctx.resolveTriggers((t) => t.oid === args.oid);
        return triggers.length > 0 ? triggers[0] : null;
      }
      if (args.schemaName && args.name) {
        // First get the namespace by name
        const namespace = await ctx.namespaceByNameLoader.load(args.schemaName);
        if (!namespace) return null;

        // Then find all triggers by name in this schema
        const triggers = await ctx.resolveTriggers();
        const classes = await ctx.resolveClasses();

        // Find the trigger that belongs to a table in the specified schema
        const match = triggers.find((t) => {
          const cls = classes.find((c) => c.oid === t.tgrelid);
          return cls && cls.relnamespace === namespace.oid && t.tgname === args.name;
        });

        return match || null;
      }
      return null;
    },

    policy: async (_p: unknown, args: any, ctx: ReqContext): Promise<PgPolicy | null> => {
      const fromId = args.id ? decodeId(args.id) : null;
      if (fromId && fromId.typeName === "Policy") {
        return ctx.policyLoader.load(fromId.oid);
      }
      if (args.oid) {
        return ctx.policyLoader.load(args.oid);
      }
      if (args.schemaName && args.name) {
        const ns = await ctx.namespaceByNameLoader.load(args.schemaName);
        if (!ns) return null;

        // For looking up by name, we still need to use the resolvePolicies method
        // since we don't have a dedicated loader for name lookups
        const classes = await ctx.resolveClasses();
        const policies = await ctx.resolvePolicies();

        const matched = policies.filter((po) => {
          const c = classes.find((cl) => cl.oid === po.polrelid);
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
        return ctx.typeLoader.load(fromId.oid);
      }
      if (args.oid) {
        return ctx.typeLoader.load(args.oid);
      }
      if (args.schemaName && args.name) {
        // Execute a query to find a type by name and schema
        const result = await ctx.client.query(
          `
          SELECT 
            t.oid, 
            t.typname, 
            t.typtype, 
            t.typbasetype, 
            t.typelem, 
            t.typrelid,
            t.typnamespace,
            n.nspname
          FROM pg_catalog.pg_type t
          JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
          WHERE n.nspname = $1 AND t.typname = $2
        `,
          [args.schemaName, args.name]
        );

        return result.rows.length ? parseDbType(result.rows[0]) : null;
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
      console.log("Node query for:", info);

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
          return ctx.triggerLoader.load(info.oid);
        }
        case "Policy": {
          return ctx.policyLoader.load(info.oid);
        }
        case "PgType": {
          console.log("Loading PgType node with oid:", info.oid);

          // Instead of using the DataLoader here, do a direct query to ensure we get full type info
          // The failure seems to be with the GraphQL Node interface resolution
          const result = await ctx.client.query(
            `
            SELECT 
              t.oid, 
              t.typname, 
              t.typtype, 
              t.typbasetype, 
              t.typelem, 
              t.typrelid,
              t.typnamespace,
              n.nspname
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            WHERE t.oid = $1
          `,
            [info.oid]
          );

          if (result.rows.length === 0) {
            console.log("No PgType found with oid:", info.oid);
            return null;
          }

          // Parse the type
          const type = parseDbType(result.rows[0]);
          console.log("Loaded PgType:", type);

          // Build the appropriate object based on the type kind
          const kind = resolvePgType(type);
          console.log("Resolved kind:", kind);

          // Return the type object directly - we already have all fields needed
          // This will be assigned the right __typename through the Node type resolver
          return {
            ...type,
            __typename: kind,
            id: buildGlobalId("PgType", type.oid),
            kind: kind.replace("Type", "").toUpperCase(),
          };
        }
        case "Column": {
          // For columns, we need to get all attributes for a relation
          // and then find the specific one we're looking for
          // This is a bit different since we don't have a direct loader by column OID
          const attributes = await ctx.resolveAttributes((a) => a.attrelid === info.oid);
          return attributes.length > 0 ? attributes[0] : null;
        }
        case "Role": {
          const roles = await ctx.resolveRoles((r) => r.oid === info.oid);
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
    edges: (p: { edges: Array<{ node: PgNamespace }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgNamespace }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgNamespace }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  TableConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) => p.edges.map((e) => e.node),
  },

  ColumnConnection: {
    edges: (p: { edges: Array<{ node: PgAttribute }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgAttribute }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgAttribute }>; first: number }) =>
      p.edges.map((e) => e.node),
  },

  ViewConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) => p.edges.map((e) => e.node),
  },

  MaterializedViewConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) => p.edges.map((e) => e.node),
  },

  IndexConnection: {
    edges: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgClass }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgClass }>; first: number }) => p.edges.map((e) => e.node),
  },

  TriggerConnection: {
    edges: (p: { edges: Array<{ node: PgTrigger }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgTrigger }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgTrigger }>; first: number }) => p.edges.map((e) => e.node),
  },

  PolicyConnection: {
    edges: (p: { edges: Array<{ node: PgPolicy }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgPolicy }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgPolicy }>; first: number }) => p.edges.map((e) => e.node),
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
    privileges: async (
      p: PgDatabase,
      args: { roleName: string },
      ctx: ReqContext
    ): Promise<any> => {
      const result = await ctx.client.query(
        `
        select pg_catalog.has_database_privilege($1, $2, 'connect') AS connect
      `,
        [args.roleName, p.datname]
      );

      const roles = await ctx.resolveRoles((r) => r.rolname === args.roleName);
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
      // Use the classesByNamespaceLoader to efficiently load tables
      let items = await ctx.classesByNamespaceLoader.load({
        namespaceOid: p.oid,
        relkind: "r",
      });

      // Apply sorting if needed
      if (args.orderBy?.field) {
        if (args.orderBy.field === "NAME") {
          sortItems(items, (x) => x.relname, args.orderBy.direction);
        } else {
          sortItems(items, (x) => x.oid, args.orderBy.direction);
        }
      }

      // Apply pagination
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    views: async (p: PgNamespace, args: any, ctx: ReqContext): Promise<any> => {
      // Use the classesByNamespaceLoader to efficiently load views
      const items = await ctx.classesByNamespaceLoader.load({
        namespaceOid: p.oid,
        relkind: "v",
      });

      // Apply pagination
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    materializedViews: async (p: PgNamespace, args: any, ctx: ReqContext): Promise<any> => {
      // Use the classesByNamespaceLoader to efficiently load materialized views
      const items = await ctx.classesByNamespaceLoader.load({
        namespaceOid: p.oid,
        relkind: "m",
      });

      // Apply pagination
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    privileges: async (
      p: PgNamespace,
      args: { roleName: string },
      ctx: ReqContext
    ): Promise<any> => {
      const result = await ctx.client.query(
        `
        select pg_catalog.has_schema_privilege($1, $2, 'USAGE') AS usage
      `,
        [args.roleName, p.nspname]
      );

      const roles = await ctx.resolveRoles((r) => r.rolname === args.roleName);
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
    columns: async (p: PgClass, args: PaginationArgs, ctx: ReqContext): Promise<any> => {
      const cols = (await ctx.attributesByRelationLoader.load(p.oid)) || [];
      const paginationResult = paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (node) => String(node.attnum),
      });
      return {
        edges: paginationResult.edges,
        pageInfo: paginationResult.pageInfo,
      };
    },
    indexes: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      const indexes = await ctx.resolveIndexes((ix) => ix.indrelid === p.oid);

      // Use Promise.all with DataLoader to batch and cache class lookups by OID
      const indexClasses = await Promise.all(
        indexes.map((ix) => ctx.classLoader.load(ix.indexrelid))
      );

      // Filter out null values and non-index classes
      const matched = indexClasses.filter((c) => c && c.relkind === "i");

      return paginate(matched, {
        first: args.first,
        after: args.after,
        cursorForNode: (c) => String((c as PgClass).oid),
      });
    },
    policies: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache policy lookups by table OID
      const policies = await ctx.policiesByRelationLoader.load(p.oid);
      return paginate(policies, {
        first: args.first,
        after: args.after,
        cursorForNode: (x) => String(x.oid),
      });
    },
    triggers: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      // Use DataLoader to batch and cache trigger lookups by table OID
      const triggers = await ctx.triggersByRelationLoader.load(p.oid);
      return paginate(triggers, {
        first: args.first,
        after: args.after,
        cursorForNode: (x) => String(x.oid),
      });
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(
        `
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select,
               pg_catalog.has_table_privilege($1, $2::oid, 'INSERT') AS insert,
               pg_catalog.has_table_privilege($1, $2::oid, 'UPDATE') AS update,
               pg_catalog.has_table_privilege($1, $2::oid, 'DELETE') AS delete
      `,
        [args.roleName, p.oid]
      );

      const roles = await ctx.resolveRoles((r) => r.rolname === args.roleName);
      return {
        role: roles.length > 0 ? roles[0] : null,
        select: result.rows[0].select,
        insert: result.rows[0].insert,
        update: result.rows[0].update,
        delete: result.rows[0].delete,
      };
    },
    foreignKeys: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      const items = await ctx.resolveForeignKeys((fk) => fk.conrelid === p.oid);
      return paginate(items, {
        first: args.first,
        after: args.after,
        cursorForNode: (n) => String(n.oid),
      });
    },
    referencedBy: async (p: PgClass, args: any, ctx: ReqContext): Promise<any> => {
      const items = await ctx.resolveForeignKeys((fk) => fk.confrelid === p.oid);
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
    privileges: async (
      p: PgAttribute,
      args: { roleName: string },
      ctx: ReqContext
    ): Promise<any> => {
      const result = await ctx.client.query(
        `
        select pg_catalog.has_column_privilege($1, $2::oid, $3, 'SELECT') AS select,
               pg_catalog.has_column_privilege($1, $2::oid, $3, 'INSERT') AS insert,
               pg_catalog.has_column_privilege($1, $2::oid, $3, 'UPDATE') AS update
      `,
        [args.roleName, p.attrelid, p.attname]
      );

      const roles = await ctx.resolveRoles((r) => r.rolname === args.roleName);
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
    columns: async (p: PgClass, args: PaginationArgs, ctx: ReqContext): Promise<any> => {
      const cols = (await ctx.attributesByRelationLoader.load(p.oid)) || [];
      const paginationResult = paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (node) => String(node.attnum),
      });
      return {
        edges: paginationResult.edges,
        pageInfo: paginationResult.pageInfo,
      };
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(
        `
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select
      `,
        [args.roleName, p.oid]
      );

      const roles = await ctx.resolveRoles((r) => r.rolname === args.roleName);
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
    isPopulated: (p: PgClass) => (typeof p.relispopulated === "boolean" ? p.relispopulated : false),
    columns: async (p: PgClass, args: PaginationArgs, ctx: ReqContext): Promise<any> => {
      const cols = (await ctx.attributesByRelationLoader.load(p.oid)) || [];
      const paginationResult = paginate(cols, {
        first: args.first,
        after: args.after,
        cursorForNode: (node) => String(node.attnum),
      });
      return {
        edges: paginationResult.edges,
        pageInfo: paginationResult.pageInfo,
      };
    },
    privileges: async (p: PgClass, args: { roleName: string }, ctx: ReqContext): Promise<any> => {
      const result = await ctx.client.query(
        `
        select pg_catalog.has_table_privilege($1, $2::oid, 'SELECT') AS select
      `,
        [args.roleName, p.oid]
      );

      const roles = await ctx.resolveRoles((r) => r.rolname === args.roleName);
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
      const indexes = await ctx.resolveIndexes((x) => x.indexrelid === p.oid);
      if (indexes.length === 0) return null;

      const ix = indexes[0];
      // Use DataLoader to batch and cache class lookups by OID
      return ctx.classLoader.load(ix.indrelid);
    },
    accessMethod: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      const indexes = await ctx.resolveIndexes((x) => x.indexrelid === p.oid);
      return indexes.length > 0 ? indexes[0].indexam : "unknown";
    },
    definition: async (p: PgClass, _a: any, ctx: ReqContext): Promise<any> => {
      const indexes = await ctx.resolveIndexes((x) => x.indexrelid === p.oid);
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
    roles: (p: PgPolicy) => {
      // Handle the case where polroles might be a string (comma-separated values)
      const roles = p.polroles as string[] | string | undefined;

      if (typeof roles === "string") {
        return roles.split(",").filter((r: string) => r.trim() !== "");
      }
      return roles || [];
    },
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
      console.log("PgType __resolveType called with:", obj);
      return resolvePgType(obj);
    },
  },

  PgTypeInterface: {
    __resolveType(obj: any) {
      console.log("PgTypeInterface __resolveType called with:", obj);
      // If we already have a __typename, use it directly
      if (obj.__typename) {
        return obj.__typename;
      }
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
      const enums = await ctx.resolveEnums((e) => e.enumtypid === p.oid);
      return enums.length > 0 ? enums[0].enumlabels : [];
    },
  },

  CompositeType: {
    id: (p: PgType) => buildGlobalId("PgType", p.oid),
    oid: (p: PgType) => p.oid,
    name: (p: PgType) => p.typname,
    kind: () => "COMPOSITE",
    fields: async (p: PgType, _args: any, ctx: ReqContext) => {
      if (!p.typrelid) {
        console.warn(`Missing typrelid for composite type ${p.typname}`);
        return [];
      }

      // Use DataLoader to batch and cache attribute lookups by relation OID
      const attrs = (await ctx.attributesByRelationLoader.load(p.typrelid)) || [];

      return Promise.all(
        attrs.map(async (a) => {
          const type = await ctx.typeLoader.load(a.atttypid);
          return {
            name: a.attname,
            type: type || null,
            notNull: a.attnotnull,
          };
        })
      );
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

  Node: {
    __resolveType(obj: any) {
      console.log("Node __resolveType called with:", obj);

      // If we already have a __typename (used for PgTypes in the node resolver)
      if (obj.__typename) {
        return obj.__typename;
      }

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
    columnMappings: async (p: PgForeignKey, _a: any, ctx: ReqContext) => {
      const [relationCols, foreignCols] = await Promise.all([
        ctx.attributesByRelationLoader.load(p.conrelid),
        ctx.attributesByRelationLoader.load(p.confrelid),
      ]);

      if (!relationCols || !foreignCols) {
        return [];
      }

      // Build a map of column numbers to columns
      const relColMap: Map<number, PgAttribute> = new Map();
      relationCols.forEach((c) => relColMap.set(c.attnum, c));

      const forColMap: Map<number, PgAttribute> = new Map();
      foreignCols.forEach((c) => forColMap.set(c.attnum, c));

      // Create FKColumn objects
      const result = [];
      for (let i = 0; i < p.conkey.length; i++) {
        const relCol = relColMap.get(p.conkey[i]);
        const forCol = forColMap.get(p.confkey[i]);

        if (relCol && forCol) {
          result.push({
            referencingColumn: relCol,
            referencedColumn: forCol,
          });
        }
      }

      return result;
    },
  },

  ForeignKeyConnection: {
    edges: (p: { edges: Array<{ node: PgForeignKey }>; first: number; pageInfo: any }) => p.edges,
    pageInfo: (p: { edges: Array<{ node: PgForeignKey }>; first: number; pageInfo: any }) => ({
      ...p.pageInfo,
    }),
    nodes: (p: { edges: Array<{ node: PgForeignKey }>; first: number }) =>
      p.edges.map((e) => e.node),
  },
};

function resolvePgType(obj: PgType): string {
  console.log("Resolving PgType:", obj);

  const typtype = obj.typtype || ""; // Ensure typtype is a string

  if (typtype === "d") return "DomainType";
  if (typtype === "e") return "EnumType";
  if (typtype === "c") return "CompositeType";
  if (typtype === "b") {
    // For array types, check if typelem exists and is not zero
    if (obj.typelem && obj.typelem !== 0) return "ArrayType";
    return "ScalarType";
  }

  // Default fallback
  return "UnknownType";
}

function resolveForeignKeyAction(action: string): string {
  switch (action) {
    case "a":
      return "NO_ACTION";
    case "r":
      return "RESTRICT";
    case "c":
      return "CASCADE";
    case "n":
      return "SET_NULL";
    case "d":
      return "SET_DEFAULT";
    default:
      return "NO_ACTION";
  }
}

export const pgNamespaceResolvers = {
  // Field resolvers for PgNamespace (schema) objects
  schema: (parent: PgNamespace) => parent.nspname,

  tables: async (parent: PgNamespace, _args: any, ctx: ReqContext): Promise<PgClass[]> => {
    const classes = await ctx.resolveClasses();
    return classes.filter(
      (c) => c.relnamespace === parent.oid && (c.relkind === "r" || c.relkind === "p")
    );
  },

  // ... other existing resolvers ...
};

export const queryResolvers = {
  // ... existing resolvers ...

  // GraphQL Query: schema(name: String!): PgNamespace
  schema: async (
    _parent: any,
    args: { name: string },
    ctx: ReqContext
  ): Promise<PgNamespace | null> => {
    return ctx.namespaceByNameLoader.load(args.name);
  },

  // GraphQL Query: schemas: [PgNamespace!]!
  schemas: async (_parent: any, _args: any, ctx: ReqContext): Promise<PgNamespace[]> => {
    return ctx.resolveNamespaces();
  },

  // ... other existing resolvers ...
};

// ... other resolvers ...

export const tableResolvers = {
  // ... existing resolvers ...

  schema: async (parent: PgClass, _args: any, ctx: ReqContext): Promise<PgNamespace | null> => {
    return ctx.namespaceLoader.load(parent.relnamespace);
  },

  // ... other existing resolvers ...
};

// ... other existing resolvers ...

function parseDbType(row: any): PgType & { typnamespace?: number; nspname?: string } {
  // Apply zod schema validation and transformation
  const baseType = PgTypeSchema.parse(row);

  // Add additional fields if they exist in the row but not in the schema
  return {
    ...baseType,
    typbasetype: row.typbasetype,
    typelem: row.typelem,
    typnamespace: row.typnamespace,
    nspname: row.nspname,
  };
}
