module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/*.test.ts'],
  moduleFileExtensions: ['ts', 'js', 'json', 'node'],
  transform: {
    // tsconfig targets ESNext modules for the rollup build. Under Jest that leaves
    // `await import('ws')` as a native dynamic import, which bypasses the module registry
    // so jest.mock('ws') cannot intercept it and the import never settles. Compile to
    // CommonJS for tests only; the production build is unaffected.
    '^.+\\.tsx?$': ['ts-jest', { tsconfig: { module: 'CommonJS' } }],
  },
};
