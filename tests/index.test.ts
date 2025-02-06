import { ApolloServer } from '@apollo/server';
import { readFileSync } from 'fs';
import { gql } from 'graphql-tag';
import { resolvers } from '../src/resolvers.js';
import { context, DbConfig, ReqContext } from '../src/context.js';

const dbConfig: DbConfig = {
  user: 'postgres',
  host: 'localhost', 
  database: 'postgres',
  password: '',
  port: 5435
};

const typeDefs = gql(readFileSync('src/schema.graphql', 'utf8'));

describe('GraphQL Server', () => {
  let testServer: ApolloServer<ReqContext>;

  beforeAll(() => {
    testServer = new ApolloServer({
      typeDefs,
      resolvers
    });
  });

  it('executes a valid query', async () => {
    const response = await testServer.executeOperation({
      query: `
        query {
          hello
        }
      `
    }, {
      contextValue: await context(dbConfig)
    });

    expect(response.body.kind).toBe('single');
    console.log(response.body);
    //expect(response.errors).toBeUndefined();
    //expect(response.data).toBeDefined();
  });
});
