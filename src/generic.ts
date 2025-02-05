import { ReqContext } from "./context.js";
import { PgType } from "./types.js";

export function sortItems<T>(
  items: T[],
  fieldGetter: (item: T) => string | number,
  direction: "ASC" | "DESC" | undefined
): T[] {
  return items.sort((a, b) => {
    const fa = fieldGetter(a);
    const fb = fieldGetter(b);
    if (typeof fa === "string" && typeof fb === "string") {
      return direction === "DESC" ? fb.localeCompare(fa) : fa.localeCompare(fb);
    }
    return direction === "DESC"
      ? (fb as number) - (fa as number)
      : (fa as number) - (fb as number);
  });
}

export interface PaginateOptions<T> {
  first?: number;
  after?: string;
  cursorForNode: (node: T) => string;
}

export interface PaginatedResult<T> {
  edges: Array<{ node: T; cursor: string }>;
  pageInfo: {
    hasNextPage: boolean;
    endCursor: string | null;
  };
}

export function paginate<T>(
  items: T[],
  { first = 10, after, cursorForNode }: PaginateOptions<T>
): PaginatedResult<T> {
  let sliceStart = 0;
  if (after) {
    const afterVal = parseInt(Buffer.from(after, "base64").toString(), 10);
    const idx = items.findIndex((i) => cursorForNode(i) === String(afterVal));
    sliceStart = idx >= 0 ? idx + 1 : 0;
  }
  const sliceEnd = sliceStart + first;
  const sliced = items.slice(sliceStart, sliceEnd);
  const hasNextPage = sliceEnd < items.length;
  const edges = sliced.map((node) => {
    const c = cursorForNode(node);
    return {
      node,
      cursor: Buffer.from(c).toString("base64"),
    };
  });
  return {
    edges,
    pageInfo: {
      hasNextPage,
      endCursor: edges.length ? edges[edges.length - 1].cursor : null,
    },
  };
}

export function decodeId(id: string): { typeName: string; oid: number } | null {
  try {
    const decoded = Buffer.from(id, "base64").toString();
    const [typeName, oidStr] = decoded.split(":");
    const oid = parseInt(oidStr, 10);
    return { typeName, oid };
  } catch {
    return null;
  }
}

export function buildGlobalId(typeName: string, oid: number): string {
  return Buffer.from(`${typeName}:${oid}`).toString("base64");
}

export function singleResultOrError<T>(
  items: T[],
  entityName: string
): T | null {
  if (items.length > 1) {
    throw new Error(
      `Multiple ${entityName} results found. Provide more specific filters.`
    );
  }
  return items.length ? items[0] : null;
}

export function findTypeNamespaceOid(
  typ: PgType,
  ctx: ReqContext
): number | null {
  if (typ.typrelid && typ.typrelid !== 0) {
    const cls = ctx.pg_classes.find((c) => c.oid === typ.typrelid);
    return cls ? cls.relnamespace : null;
  }
  return null;
}
