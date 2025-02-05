import { ApolloServer } from "@apollo/server";
import { startStandaloneServer } from "@apollo/server/standalone";
import { readFileSync } from "fs";
import { gql } from "graphql-tag";
import { resolvers } from "./resolvers.js";
import { ReqContext, context } from "./context.js";

const typeDefs = gql(readFileSync("src/schema.graphql", "utf8"));

const server = new ApolloServer<ReqContext>({ typeDefs, resolvers });

(async () => {
  const { url } = await startStandaloneServer(server, {
    listen: { port: 4000 },
    context,
  });

  console.log(`server ready at: ${url}`);
})();
