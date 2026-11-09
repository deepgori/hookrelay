import pg from "pg";
import { createPool, EventStore, migrate } from "../src/db.js";
import type { EventRef } from "../src/queue.js";

// Tests get their own database so they never wipe events from a local dev run.
const ADMIN_URL = process.env.DATABASE_URL ?? "postgres://hookrelay:hookrelay@localhost:5433/hookrelay";
export const TEST_DATABASE_URL = ADMIN_URL.replace(/\/[^/]+$/, "/hookrelay_test");
export const AMQP_URL = process.env.AMQP_URL ?? "amqp://guest:guest@localhost:5673";

let created = false;
async function ensureTestDatabase() {
  if (created) return;
  const admin = new pg.Client({ connectionString: ADMIN_URL });
  await admin.connect();
  const exists = await admin.query("SELECT 1 FROM pg_database WHERE datname = 'hookrelay_test'");
  if (exists.rowCount === 0) await admin.query("CREATE DATABASE hookrelay_test");
  await admin.end();
  created = true;
}

export async function freshStore(): Promise<{ pool: pg.Pool; store: EventStore }> {
  await ensureTestDatabase();
  const pool = createPool(TEST_DATABASE_URL);
  await migrate(pool);
  await pool.query("TRUNCATE events");
  return { pool, store: new EventStore(pool) };
}

/** A stand-in for RabbitMQ that just records what was published where. */
export function recordingQueue() {
  const published: { queue: string; ref: EventRef }[] = [];
  return {
    published,
    retryQueue: "events.retry.test",
    async publish(queue: string, ref: EventRef) {
      published.push({ queue, ref });
    },
  };
}
