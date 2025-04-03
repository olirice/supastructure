import { config } from "dotenv";
import { ApolloServer } from "@apollo/server";
import { startStandaloneServer } from "@apollo/server/standalone";
import { readFileSync } from "fs";
import { gql } from "graphql-tag";
import { resolvers } from "./resolvers.js";
import type { DbConfig, ReqContext } from "./context.js";
import { context, releaseClient } from "./context.js";
import depthLimit from "graphql-depth-limit";
import { createComplexityLimitRule } from "graphql-validation-complexity";

// Load environment variables
config();

export const dbConfig: DbConfig = {
  user: process.env.PG_USER || "postgres",
  host: process.env.PG_HOST || "localhost",
  database: process.env.PG_DATABASE || "postgres",
  password: process.env.PG_PASSWORD || "password",
  port: process.env.PG_PORT ? parseInt(process.env.PG_PORT, 10) : 5435,
};

const typeDefs = gql(readFileSync("src/schema.graphql", "utf8"));

// Create complexity limit rule with specified costs
const complexityLimitRule = createComplexityLimitRule(1500, {
  // Set costs according to requirements
  scalarCost: 1,    // Each scalar field costs 1 point
  objectCost: 2,    // Each object field costs 2 points
  listFactor: 10,   // List fields multiply cost by 10 × number of items
});

const server = new ApolloServer<ReqContext>({
  typeDefs,
  resolvers,
  validationRules: [
    // Limit query depth to 9 levels to prevent excessive nesting
    depthLimit(9),
    // Apply complexity limit to prevent expensive queries
    complexityLimitRule
  ],
  plugins: [
    {
      async requestDidStart() {
        return {
          async willSendResponse(requestContext) {
            const { contextValue } = requestContext;
            if (contextValue && contextValue.client) {
              await releaseClient(contextValue.client);
            }
          },
        };
      },
    },
  ],
});

// Don't run the production server when testing
if (process.env.NODE_ENV !== "test") {
  (async () => {
    const { url } = await startStandaloneServer(server, {
      listen: { port: 4000 },
      context: () => context(dbConfig),
    });

    // console.log(`server ready at: ${url}`);
  })();
}
