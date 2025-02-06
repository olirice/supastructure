import { config } from 'dotenv';
import { ApolloServer } from '@apollo/server';
import { startStandaloneServer } from '@apollo/server/standalone';
import { readFileSync } from 'fs';
import { gql } from 'graphql-tag';
import { resolvers } from './resolvers.js';
import { context, DbConfig, ReqContext } from './context.js';

// Load environment variables
config();

const dbConfig: DbConfig = {
  user: process.env.PG_USER || 'postgres',
  host: process.env.PG_HOST || 'localhost',
  database: process.env.PG_DATABASE || 'postgres',
  password: process.env.PG_PASSWORD || '',
  port: process.env.PG_PORT ? parseInt(process.env.PG_PORT, 10) : 5435,
};

const typeDefs = gql(readFileSync('src/schema.graphql', 'utf8'));

const server = new ApolloServer<ReqContext>({ typeDefs, resolvers });

(async () => {
  const { url } = await startStandaloneServer(server, {
    listen: { port: 4000 },
    context: () => context(dbConfig),
  });

  console.log(`server ready at: ${url}`);
})();
