import { Client } from "pg";
import { createExtensionLoaders, extensionQueries } from "../../src/loaders/pg_extensions.js";

// Mock pg Client
const mockClient = {
  query: jest.fn(),
} as unknown as Client;

describe("Extension loaders", () => {
  beforeEach(() => {
    jest.clearAllMocks();

    // Default mock implementation for query
    (mockClient.query as jest.Mock).mockImplementation((query, params) => {
      const allExtensions = [
        {
          oid: 1,
          name: "pg_stat_statements",
          defaultVersion: "1.9",
          comment: "track planning and execution statistics of all SQL statements executed",
          relocatable: true,
          installed: true,
          installedVersion: "1.9",
          schemaOid: 11
        },
        {
          oid: 2,
          name: "pgcrypto",
          defaultVersion: "1.3",
          comment: "cryptographic functions",
          relocatable: false,
          installed: true,
          installedVersion: "1.3",
          schemaOid: 11
        },
        {
          oid: null,
          name: "pg_partman",
          defaultVersion: "4.5.1",
          comment: "Extension to manage partitioned tables by time or ID",
          relocatable: null,
          installed: false,
          installedVersion: null,
          schemaOid: null
        }
      ];

      // Filter based on params
      let filtered = [...allExtensions];
      
      if (params && params.length > 0) {
        // Check for OID filter
        if (query.includes("e.oid = ANY") && params[0] instanceof Array) {
          const oids = params[0];
          filtered = filtered.filter(ext => ext.oid !== null && oids.includes(ext.oid));
        }
        
        // Check for name filter
        if (query.includes("e.extname = ANY") && params[0] instanceof Array) {
          const names = params[0];
          filtered = filtered.filter(ext => names.includes(ext.name));
        }
        
        // Check for schema filter
        if (query.includes("e.extnamespace = ANY") && params[0] instanceof Array) {
          const schemas = params[0];
          filtered = filtered.filter(ext => ext.schemaOid !== null && schemas.includes(ext.schemaOid));
        }
      }
      
      // Check for installed-only filter
      if (query.includes("e.oid IS NOT NULL")) {
        filtered = filtered.filter(ext => ext.oid !== null);
      }

      return { rows: filtered };
    });
  });

  describe("extensionQueries", () => {
    test("query returns extensions with no filters", async () => {
      const extensions = await extensionQueries.query(mockClient);
      expect(extensions).toHaveLength(3);
      expect(mockClient.query).toHaveBeenCalledTimes(1);
      expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining("select"), []);
    });

    test("query with oid filter", async () => {
      await extensionQueries.query(mockClient, { oids: [1] });
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("WHERE e.oid = ANY($1)"),
        [[1]]
      );
    });

    test("query with name filter", async () => {
      await extensionQueries.query(mockClient, { names: ["pg_stat_statements"] });
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("WHERE ae.name = ANY($1)"),
        [["pg_stat_statements"]]
      );
    });

    test("query with schema filter", async () => {
      await extensionQueries.query(mockClient, { schemaOids: [11] });
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("WHERE e.extnamespace = ANY($1)"),
        [[11]]
      );
    });

    test("query with installed filter", async () => {
      await extensionQueries.query(mockClient, { onlyInstalled: true });
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("WHERE e.oid IS NOT NULL"),
        []
      );
    });

    test("byOid returns a single extension", async () => {
      const extension = await extensionQueries.byOid(mockClient, 1);
      expect(extension).toMatchObject({ oid: 1, name: "pg_stat_statements" });
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });

    test("byOid returns null for non-existent extension", async () => {
      (mockClient.query as jest.Mock).mockResolvedValueOnce({ rows: [] });
      const extension = await extensionQueries.byOid(mockClient, 999);
      expect(extension).toBeNull();
    });

    test("byName returns a single extension", async () => {
      const extension = await extensionQueries.byName(mockClient, "pg_stat_statements");
      expect(extension).toMatchObject({ name: "pg_stat_statements" });
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });

    test("byName returns null for non-existent extension", async () => {
      (mockClient.query as jest.Mock).mockResolvedValueOnce({ rows: [] });
      const extension = await extensionQueries.byName(mockClient, "non_existent");
      expect(extension).toBeNull();
    });

    test("bySchemaOid returns extensions in schema", async () => {
      const extensions = await extensionQueries.bySchemaOid(mockClient, 11);
      expect(extensions).toHaveLength(2); // Only installed extensions have a schemaOid
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });

    test("byName returns a non-installed extension", async () => {
      // Override mock implementation to make sure pg_partman is found
      (mockClient.query as jest.Mock).mockImplementationOnce(() => {
        return { 
          rows: [{
            oid: null,
            name: "pg_partman",
            defaultVersion: "4.5.1",
            comment: "Extension to manage partitioned tables by time or ID",
            relocatable: null,
            installed: false,
            installedVersion: null,
            schemaOid: null
          }]
        };
      });
      
      const extension = await extensionQueries.byName(mockClient, "pg_partman");
      expect(extension).not.toBeNull();
      expect(extension).toMatchObject({ 
        name: "pg_partman", 
        installed: false,
        defaultVersion: "4.5.1"
      });
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });
  });

  describe("createExtensionLoaders", () => {
    test("extensionLoader loads extensions by OID", async () => {
      const { extensionLoader } = createExtensionLoaders(mockClient);
      const results = await extensionLoader.loadMany([1, 2]);
      
      // Check if any errors were returned
      const errors = results.filter(result => result instanceof Error);
      expect(errors).toHaveLength(0);
      
      // Cast to proper type now that we've confirmed there are no errors
      const [ext1, ext2] = results as Array<{
        oid: number;
        name: string;
        defaultVersion: string;
        installed: boolean;
        [key: string]: any;
      } | null>;
      
      expect(ext1).toMatchObject({ oid: 1, name: "pg_stat_statements" });
      expect(ext2).toMatchObject({ oid: 2, name: "pgcrypto" });
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });

    test("extensionByNameLoader loads extensions by name", async () => {
      const { extensionByNameLoader } = createExtensionLoaders(mockClient);
      const results = await extensionByNameLoader.loadMany(["pg_stat_statements", "pgcrypto"]);
      
      // Check if any errors were returned
      const errors = results.filter(result => result instanceof Error);
      expect(errors).toHaveLength(0);
      
      // Cast to proper type now that we've confirmed there are no errors
      const [ext1, ext2] = results as Array<{
        name: string;
        defaultVersion: string;
        installed: boolean;
        [key: string]: any;
      } | null>;
      
      expect(ext1).toMatchObject({ name: "pg_stat_statements" });
      expect(ext2).toMatchObject({ name: "pgcrypto" });
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });

    test("extensionsBySchemaLoader loads extensions by schema OID", async () => {
      const { extensionsBySchemaLoader } = createExtensionLoaders(mockClient);
      const results = await extensionsBySchemaLoader.loadMany([11]);
      
      // Check if any errors were returned
      const errors = results.filter(result => result instanceof Error);
      expect(errors).toHaveLength(0);
      
      // Cast to proper type
      const extsArray = results as Array<Array<{
        schemaOid: number;
        name: string;
        installed: boolean;
        [key: string]: any;
      }>>;
      
      const exts = extsArray[0];
      expect(exts).toHaveLength(2); // Only the installed extensions have a schemaOid
      expect(exts[0]).toMatchObject({ schemaOid: 11 });
      expect(exts[1]).toMatchObject({ schemaOid: 11 });
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });

    test("getAllExtensions returns all extensions", async () => {
      const { getAllExtensions } = createExtensionLoaders(mockClient);
      const extensions = await getAllExtensions();
      
      expect(extensions).toHaveLength(3);
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });

    test("getAllExtensions with filter returns filtered extensions", async () => {
      const { getAllExtensions } = createExtensionLoaders(mockClient);
      const extensions = await getAllExtensions((ext) => ext.installed);
      
      expect(extensions).toHaveLength(2);
      expect(extensions[0]).toMatchObject({ installed: true });
      expect(extensions[1]).toMatchObject({ installed: true });
      expect(mockClient.query).toHaveBeenCalledTimes(1);
    });
  });
}); 