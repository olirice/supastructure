import { z } from "zod";

export const PgDatabaseSchema = z.object({
  oid: z.number().int(),
  datname: z.string(),
});
export type PgDatabase = z.infer<typeof PgDatabaseSchema>;

export const PgNamespaceSchema = z.object({
  oid: z.number().int(),
  nspname: z.string(),
  nspowner: z.number().int().optional(),
});
export type PgNamespace = z.infer<typeof PgNamespaceSchema>;

export const PgClassSchema = z.object({
  oid: z.number().int(),
  relname: z.string(),
  relnamespace: z.number().int(),
  relkind: z.string().length(1),
  relispopulated: z.boolean().optional(), // for mat views
});
export type PgClass = z.infer<typeof PgClassSchema>;

export const PgAttributeSchema = z.object({
  attrelid: z.number().int(),
  attname: z.string(),
  atttypid: z.number().int(),
  attnum: z.number().int(),
  attnotnull: z.boolean(),
});
export type PgAttribute = z.infer<typeof PgAttributeSchema>;

export const PgTriggerSchema = z.object({
  oid: z.number().int(),
  tgname: z.string(),
  tgrelid: z.number().int(),
});
export type PgTrigger = z.infer<typeof PgTriggerSchema>;

export const PgPolicySchema = z.object({
  oid: z.number().int(),
  polname: z.string(),
  polrelid: z.number().int(),
  polcmd: z.string().optional(),
  polroles: z.array(z.string()).optional(),
  polqual: z.string().nullable().optional(),
  polwithcheck: z.string().nullable().optional(),
});
export type PgPolicy = z.infer<typeof PgPolicySchema>;

export const PgTypeSchema = z.object({
  oid: z.number().int(),
  typname: z.string(),
  typtype: z.string().optional(),
  typbasetype: z.number().int().optional(),
  typelem: z.number().int().optional(),
  typrelid: z.number().int().optional(),
});
export type PgType = z.infer<typeof PgTypeSchema>;

export const PgEnumSchema = z.object({
  enumtypid: z.number().int(),
  enumlabels: z.array(z.string()), // Changed from enumlabel to enumlabels array
});
export type PgEnum = z.infer<typeof PgEnumSchema>;

export const PgIndexSchema = z.object({
  indexrelid: z.number().int(),
  indrelid: z.number().int(),
  indkey: z.string().optional(),
  indexdef: z.string().optional(),
  indexam: z.string(), // combined from join with pg_am
});
export type PgIndex = z.infer<typeof PgIndexSchema>;

// We'll also add a Role type from pg_roles
// so we can link privileges to a real role object
export const PgRoleSchema = z.object({
  oid: z.number().int(),
  rolname: z.string(),
  rolsuper: z.boolean().optional(),
});
export type PgRole = z.infer<typeof PgRoleSchema>;
