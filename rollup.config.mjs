import typescript from '@rollup/plugin-typescript';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import terser from '@rollup/plugin-terser';

const isProd = (process.env.BUILD === 'production');

export default {
  input: 'main.ts',
  output: {
    file: 'main.js',
    // Obsidian's community-plugin installer only fetches main.js/manifest.json/
    // styles.css, so the plugin must be a single self-contained file. Dynamic
    // imports are inlined and merely deferred in evaluation, never split out.
    inlineDynamicImports: true,
    // Inline maps are ~2.2 MB and were previously shipped in production builds.
    sourcemap: isProd ? false : 'inline',
    sourcemapExcludeSources: isProd,
    format: 'cjs',
    exports: 'default',
  },
  external: ['obsidian', 'os', 'http', 'dgram', 'events'],
  plugins: [
    // tsconfig enables inlineSourceMap for editor tooling; production must override
    // it or ~2.2 MB of mappings end up in the shipped bundle.
    typescript({
      sourceMap: !isProd,
      inlineSourceMap: !isProd,
      inlineSources: !isProd,
    }),
    nodeResolve({
      browser: true,
    }),
    commonjs({
      transformMixedEsModules: true,
    }),
    isProd && terser({
      format: { comments: false },
    }),
  ].filter(Boolean),
  onwarn(warning, warn) {
    if (warning.code === 'THIS_IS_UNDEFINED') {
      return;
    }
    warn(warning);
  }
};
