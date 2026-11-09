import { config } from "./config.js";
import { createPool, EventStore, migrate } from "./db.js";
import { handlers } from "./handlers.js";
import { connectQueue } from "./queue.js";
import { startWorker } from "./worker.js";

const pool = createPool(config.databaseUrl);
await migrate(pool);
const queue = await connectQueue(config.amqpUrl, config.retryDelayMs);
const worker = await startWorker({ store: new EventStore(pool), queue, handlers, maxAttempts: config.maxAttempts });
console.log("hookrelay worker consuming events.work");

async function shutdown() {
  await worker.stop();
  await queue.close();
  await pool.end();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
