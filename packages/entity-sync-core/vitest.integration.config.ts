import { defineConfig } from "vitest/config";

// Integration tests against a real docker-compose stack (see
// tests/integration/docker-compose.test.ts). Run via `pnpm run
// test:integration` — needs Docker, so kept out of the default `pnpm run
// test` unit-test path (see vitest.config.ts).
export default defineConfig({
  test: {
    include: ["tests/integration/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 150_000,
  },
});
