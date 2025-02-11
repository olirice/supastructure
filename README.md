# supastructure


**supastructure** is an experimental GraphQL API server and library. It connects to [Supabase](https://supabase.com/) projects, enabling fine-grained user defined queries of database structure. This project aims to assist AI developer platforms, such as [Bolt.new](https://bolt.new/) and [Lovable.dev](https://lovable.dev/), in extracting fine-grained context from Supabase projects to enhance LLM-generated responses to user prompts.

**Note:**:
- supastructure is not supported by Supabase in any capacity
- the primary goal of this is learning

## Features

- **GraphQL API Server:** GraphQL API server for querying Supabase project structures
- **Embedded GraphQL Explorer:** Access the GraphQL explorer at `http://localhost:4000` for interactive queries
- **Library Usage:** Embed it directly in your node application

## Installation
Ensure you have Node.js and npm installed. Then, follow these steps:

1. Clone the Repository:
```shell
git clone https://github.com/olirice/supastructure.git
cd supastructure
```

2. Install Dependencies
```shell
npm install
```

## Usage

### As an API Server

Start the API Server with
```shell
npm start
```

Then visit `http://localhost:4000` and query your project.

![GraphQL Sandbox](assets/sandbox.png)


## Testing

To run the test suite, use:

```shell
npm test
```

For continuous testing during development

```shell
npm run test:watch
```

## GraphQL Schema
This schema is undergoing rapid change and may be out of date in the README. For the official source of truth see ![src/schema.graphql](src/schema.graphql)

Known things that need to change:
- Privileges aren't buttoned up yet
- Need to add a bunch of other entities
  - Foreign Servers
  - Foreign Tables
  - Extensions
    - Should extensions have associated schemas/functions/views/etc from pg_depends?

```graphql
# src/schema.graphql

########################################
# Node + Shared
########################################
interface Node {
  id: ID!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: String
}

enum SortDirection {
  ASC
  DESC
}

########################################
# Queries
########################################
type Query {
  database: Database!

  # single-entity queries
  schema(oid: Int, schemaName: String, name: String, id: ID): Schema
  table(oid: Int, schemaName: String, name: String, id: ID): Table
  view(oid: Int, schemaName: String, name: String, id: ID): View
  materializedView(
    oid: Int
    schemaName: String
    name: String
    id: ID
  ): MaterializedView
  index(oid: Int, schemaName: String, name: String, id: ID): Index
  trigger(oid: Int, schemaName: String, name: String, id: ID): Trigger
  policy(oid: Int, schemaName: String, name: String, id: ID): Policy
  type(oid: Int, schemaName: String, name: String, id: ID): PgType

  # role
  role(oid: Int, name: String, id: ID): Role

  node(id: ID!): Node
}

########################################
# Role
########################################
type RoleConnection {
  edges: [RoleEdge!]!
  pageInfo: PageInfo!
  nodes: [Role!]!
}

type RoleEdge {
  node: Role!
  cursor: String!
}

type Role implements Node {
  id: ID!
  oid: Int!
  name: String!
  isSuperuser: Boolean
}

########################################
# Database
########################################
type Database implements Node {
  id: ID!
  oid: Int!
  name: String!

  schemas(
    first: Int
    after: String
    filter: SchemaFilter
    orderBy: SchemaOrderBy
  ): SchemaConnection!
}

input SchemaFilter {
  name: String
  oid: Int
}

enum SchemaOrderByField {
  NAME
  OID
}

input SchemaOrderBy {
  field: SchemaOrderByField
  direction: SortDirection
}

# for privileges
type DatabasePrivilege {
  role: Role!
  connect: Boolean
}

type DatabasePrivilegeConnection {
  edges: [DatabasePrivilegeEdge!]!
  pageInfo: PageInfo!
  nodes: [DatabasePrivilege!]!
}

type DatabasePrivilegeEdge {
  node: DatabasePrivilege!
  cursor: String!
}

########################################
# Schema + Privileges
########################################
type SchemaConnection {
  edges: [SchemaEdge!]!
  pageInfo: PageInfo!
  nodes: [Schema!]!
}

type SchemaEdge {
  node: Schema!
  cursor: String!
}

# dedicated privilege for a schema
# e.g. usage, create
# also references a role

type SchemaPrivilege {
  role: Role!
  usage: Boolean
  create: Boolean
}

type SchemaPrivilegeConnection {
  edges: [SchemaPrivilegeEdge!]!
  pageInfo: PageInfo!
  nodes: [SchemaPrivilege!]!
}

type SchemaPrivilegeEdge {
  node: SchemaPrivilege!
  cursor: String!
}

type Schema implements Node {
  id: ID!
  oid: Int!
  name: String!

  tables(
    first: Int
    after: String
    filter: TableFilter
    orderBy: TableOrderBy
  ): TableConnection!
  views(first: Int, after: String): ViewConnection!
  materializedViews(first: Int, after: String): MaterializedViewConnection!
  indexes(first: Int, after: String): IndexConnection!
  triggers(first: Int, after: String): TriggerConnection!
  policies(first: Int, after: String): PolicyConnection!
  types(first: Int, after: String): PgTypeConnection!

  # privileges
  activePrivileges: SchemaPrivilegeConnection!
  defaultPrivileges: SchemaPrivilegeConnection!
}

########################################
# Table + Privileges
########################################
input TableFilter {
  name: String
  oid: Int
}

enum TableOrderByField {
  NAME
  OID
}

input TableOrderBy {
  field: TableOrderByField
  direction: SortDirection
}

type TableConnection {
  edges: [TableEdge!]!
  pageInfo: PageInfo!
  nodes: [Table!]!
}

type TableEdge {
  node: Table!
  cursor: String!
}

# table-specific privileges
# e.g. select, insert, update, delete

type TablePrivilege {
  role: Role!
  select: Boolean
  insert: Boolean
  update: Boolean
  delete: Boolean
}

type TablePrivilegeConnection {
  edges: [TablePrivilegeEdge!]!
  pageInfo: PageInfo!
  nodes: [TablePrivilege!]!
}

type TablePrivilegeEdge {
  node: TablePrivilege!
  cursor: String!
}

type Table implements Node {
  id: ID!
  oid: Int!
  name: String!
  relkind: String!
  schema: Schema!

  columns: ColumnConnection!
  indexes: IndexConnection!
  policies: PolicyConnection!

  # privileges
  activePrivileges: TablePrivilegeConnection!
  defaultPrivileges: TablePrivilegeConnection!
}

########################################
# Columns
########################################
type ColumnConnection {
  edges: [ColumnEdge!]!
  pageInfo: PageInfo!
  nodes: [Column!]!
}

type ColumnEdge {
  node: Column!
  cursor: String!
}

type Column implements Node {
  id: ID!
  name: String!
  attnum: Int!
  atttypid: Int!
  table: Table!
  type: PgType!
}

########################################
# View + Privileges
########################################
type ViewConnection {
  edges: [ViewEdge!]!
  pageInfo: PageInfo!
  nodes: [View!]!
}

type ViewEdge {
  node: View!
  cursor: String!
}

# We'll reuse table-like privileges for view
# or define a separate if you prefer, but let's reuse TablePrivilege.

type View implements Node {
  id: ID!
  oid: Int!
  name: String!
  relkind: String!
  schema: Schema!

  # privileges
  activePrivileges: TablePrivilegeConnection!
  defaultPrivileges: TablePrivilegeConnection!
}

########################################
# MaterializedView + Privileges
########################################
type MaterializedViewConnection {
  edges: [MaterializedViewEdge!]!
  pageInfo: PageInfo!
  nodes: [MaterializedView!]!
}

type MaterializedViewEdge {
  node: MaterializedView!
  cursor: String!
}

type MaterializedView implements Node {
  id: ID!
  oid: Int!
  name: String!
  relkind: String!
  schema: Schema!
  populated: Boolean!

  # privileges
  activePrivileges: TablePrivilegeConnection!
  defaultPrivileges: TablePrivilegeConnection!
}

########################################
# Index
########################################
type IndexConnection {
  edges: [IndexEdge!]!
  pageInfo: PageInfo!
  nodes: [Index!]!
}

type IndexEdge {
  node: Index!
  cursor: String!
}

type Index implements Node {
  id: ID!
  oid: Int!
  name: String!
  relkind: String!
  schema: Schema!
  table: Table!
  accessMethod: String!
  definition: String
}

########################################
# Trigger
########################################
type TriggerConnection {
  edges: [TriggerEdge!]!
  pageInfo: PageInfo!
  nodes: [Trigger!]!
}

type TriggerEdge {
  node: Trigger!
  cursor: String!
}

type Trigger implements Node {
  id: ID!
  oid: Int!
  name: String!
  table: Table!
}

########################################
# Policy
########################################
type PolicyConnection {
  edges: [PolicyEdge!]!
  pageInfo: PageInfo!
  nodes: [Policy!]!
}

type PolicyEdge {
  node: Policy!
  cursor: String!
}

type Policy implements Node {
  id: ID!
  oid: Int!
  name: String!
  table: Table!
  command: String
  roles: [String!]
  usingExpr: String
  withCheck: String
}

########################################
# PgType + union
########################################

interface PgTypeInterface implements Node {
  id: ID!
  oid: Int!
  name: String!
  kind: TypeKind!
}

enum TypeKind {
  DOMAIN
  SCALAR
  ARRAY
  COMPOSITE
  ENUM
  UNKNOWN
}

type PgTypeConnection {
  edges: [PgTypeEdge!]!
  pageInfo: PageInfo!
  nodes: [PgType!]!
}

type PgTypeEdge {
  node: PgType!
  cursor: String!
}

union PgType =
    DomainType
  | ScalarType
  | EnumType
  | ArrayType
  | CompositeType
  | UnknownType

# domain
type DomainType implements PgTypeInterface & Node {
  id: ID!
  oid: Int!
  name: String!
  kind: TypeKind!
  baseType: PgType
}

# scalar
type ScalarType implements PgTypeInterface & Node {
  id: ID!
  oid: Int!
  name: String!
  kind: TypeKind!
}

# enum
type EnumType implements PgTypeInterface & Node {
  id: ID!
  oid: Int!
  name: String!
  kind: TypeKind!
  enumVariants: [String!]!
}

# array
type ArrayType implements PgTypeInterface & Node {
  id: ID!
  oid: Int!
  name: String!
  kind: TypeKind!
  elementType: PgType
}

type CompositeField {
  name: String!
  type: PgType!
  notNull: Boolean
}

# composite
type CompositeType implements PgTypeInterface & Node {
  id: ID!
  oid: Int!
  name: String!
  kind: TypeKind!
  fields: [CompositeField!]!
}

# fallback / unknown
type UnknownType implements PgTypeInterface & Node {
  id: ID!
  oid: Int!
  name: String!
  kind: TypeKind!
}
```

## How to Contribute

Please open an issue to discuss the problem you'd like to solve before opening any PRs
