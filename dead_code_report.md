# Dead Code Analysis Report

Based on code coverage analysis and code review, I've identified several likely unused functions and methods in the codebase. These are candidates for removal or refactoring.

## 1. Unused Query Methods

Several specialized query methods in loader modules are never called:

### In `src/loaders/pg_policies.ts`:
- `policyQueries.byNameAndSchema()` - Line 110-123
  - This method is defined but never used anywhere in the codebase
  - It retrieves policies by name and schema, but this functionality is not utilized

### In `src/loaders/pg_attributes.ts`:
- `attributeQueries.queryByTableName()` - Line 124-152
  - This method is exported but never called directly
  - The `attributesByTableNameLoader` is likely using another approach
  - Coverage report shows this area (lines 129-148) is uncovered

### In `context.ts`:
- Several sections of the `queries` object have low coverage:
  - Lines 67-152: Initial database queries that are likely superseded by DataLoaders
  - Lines 434, 496-499: Unused utility functions
  - Lines 560-582: Potentially unused error handling
  - Lines 587-624: Unused DataLoader implementations

## 2. Unused Utility Functions

### In `src/generic.ts`:
- `limitPageSize()` - Line 90-93 
  - This function is exported but only used internally in paginate()
  - It's included in resolvers.ts exports but never called from outside generic.ts

### In `src/resolvers.ts`:
- There's a lot of console logging (lines 865-1051) that could be reduced or removed
- Lines 971-993, 1012, 1041-1042: Uncovered sections related to type resolution

## 3. Unused Parameters and Variables

- Several functions have unused parameters prefixed without `_` prefix
- Lines 70-252 in context.ts: Large sections of uncovered code including dead methods

## 4. Dead Code in Index.ts

- Only 60% of `index.ts` is covered
- Lines 29-33 and 44-50: Dead code sections possibly related to server initialization

## Verification Method

To verify dead code, we can:
1. Comment out the identified sections
2. Run the test suite to confirm functionality is maintained
3. Search for any references using grep/find
4. Try removal in a feature branch to confirm it's safe to delete

## Recommendations

1. Remove the identified unused query methods to simplify the codebase
2. Convert `limitPageSize` to a private function
3. Clean up the `context.ts` file by removing duplicated functionality
4. Reduce console logging in production code
5. Add tests to cover any intentional but currently untested functionality 