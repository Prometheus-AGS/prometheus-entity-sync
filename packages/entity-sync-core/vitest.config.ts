import { defineConfig } from "vitest/config";

// Default `vitest run` (unit tests, src/__tests__/) never needs Docker.
// Integration tests (tests/integration/) require a real docker-compose
// stack and run only via the separate `test:integration` script.
export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    exclude: ["tests/integration/**"],
  },
});
