import { nodeResolve } from "@rollup/plugin-node-resolve";
import typescript from "@rollup/plugin-typescript";
import terser from "@rollup/plugin-terser";
import { visualizer } from "rollup-plugin-visualizer";

export default {
  input: "src/index.ts",
  output: {
    file: "dist/index.bundle.js",
    format: "esm",
    sourcemap: false,
  },
  plugins: [
    nodeResolve(),
    typescript({ tsconfig: "./tsconfig.json", declaration: false, outDir: undefined }),
    terser(),
    visualizer({
      filename: "dist/bundle-stats.html",
      gzipSize: true,
      brotliSize: true,
    }),
  ],
};
