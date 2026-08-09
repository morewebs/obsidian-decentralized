import typescript from '@rollup/plugin-typescript';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import terser from '@rollup/plugin-terser';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

const isProd = (process.env.BUILD === 'production');

/**
 * `ws` declares a "browser" field pointing at a stub whose only job is to throw. With
 * nodeResolve({browser: true}) that stub is what gets bundled — it builds cleanly and then
 * fails at runtime. Resolve `ws` to its Node entry explicitly instead. Flipping `browser`
 * off globally is not an option: peerjs, qrcode and html5-qrcode all want browser builds.
 */
const wsNodeEntry = {
  name: 'ws-node-entry',
  resolveId(source) {
    return source === 'ws' ? require.resolve('ws') : null;
  },
};

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
  // Node builtins stay external — they exist in Obsidian desktop's Electron runtime and must
  // not be bundled. Everything past 'events' is pulled in by `ws`. bufferutil and
  // utf-8-validate are ws's optional native accelerators: they are not installed, and ws
  // already requires them inside try/catch, so leaving them unresolved is the intended path.
  external: [
    'obsidian',
    'os', 'http', 'dgram', 'events',
    'net', 'tls', 'https', 'url', 'crypto', 'zlib', 'stream', 'util', 'buffer',
    'bufferutil', 'utf-8-validate',
  ],
  plugins: [
    wsNodeEntry,
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
