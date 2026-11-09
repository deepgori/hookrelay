import { createApp } from "./app.js";
import { config } from "./config.js";
import { createPool, EventStore, migrate } from "./db.js";
import { connectQueue } from "./queue.js";
import { sweepOnce } from "./sweeper.js";

const pool = createPool(config.databaseUrl);
await migrate(pool);
const store = new EventStore(pool);
const queue = await connectQueue(config.amqpUrl, config.retryDelayMs);

const app = createApp({ store, queue, secret: config.webhookSecret, toleranceSec: config.signatureToleranceSec });
const server = app.listen(config.port, () => console.log(`hookrelay listening on :${config.port}`));

const sweeper = setInterval(() => {
  sweepOnce(store, queue, config.sweepAfterMs)
    .then((n) => n && console.log(`[sweeper] requeued ${n} stuck event(s)`))
    .catch((err) => console.error("[sweeper]", err));
}, 10_000);

async function shutdown() {
  clearInterval(sweeper);
  server.close();
  await queue.close();
  await pool.end();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
