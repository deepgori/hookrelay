import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The integration tests share one Postgres database and one RabbitMQ broker, so run files one at a time.
    fileParallelism: false,
    testTimeout: 15_000,
  },
});
