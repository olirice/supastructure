export default {
    preset: 'ts-jest/presets/default-esm',
    testEnvironment: 'node',
    transform: {
      '^.+\\.ts$': ['ts-jest', { useESM: true }]
    },
    extensionsToTreatAsEsm: ['.ts'],
    moduleNameMapper: {
      '^(\\.{1,2}/.*)\\.js$': '$1'
    },
    testMatch: ['**/tests/**/*.test.ts'], // Only run test files in `tests/`
    modulePathIgnorePatterns: ['<rootDir>/dist/'] // Ignore compiled JS files
  };