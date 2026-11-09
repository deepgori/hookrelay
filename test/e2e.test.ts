// End to end against the real RabbitMQ from docker-compose: HTTP in, worker out, including a real
// delayed retry through the retry queue's message TTL.
import amqp from "amqplib";
import type pg from "pg";
import request from "supertest";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createApp } from "../src/app.js";
import type { EventStore } from "../src/db.js";
import { handlers } from "../src/handlers.js";
import { connectQueue, QUEUES, retryQueueName, type Queue } from "../src/queue.js";
import { sign } from "../src/signature.js";
import { startWorker } from "../src/worker.js";
import { AMQP_URL, freshStore } from "./helpers.js";

const secret = "test-secret";
let pool: pg.Pool;
let store: EventStore;
let queue: Queue;
let worker: { stop: () => Promise<unknown> };

beforeAll(async () => {
  ({ pool, store } = await freshStore());
  // Start from empty queues so leftovers from a dev run can't interfere. A short retry delay keeps the test fast.
  const conn = await amqp.connect(AMQP_URL);
  const ch = await conn.createChannel();
  for (const q of [...Object.values(QUEUES), retryQueueName(150)]) await ch.deleteQueue(q);
  await conn.close();

  queue = await connectQueue(AMQP_URL, 150);
  worker = await startWorker({ store, queue, handlers, maxAttempts: 4 });
});

afterAll(async () => {
  await worker?.stop();
  await queue?.close();
  await pool?.end();
});

async function waitForStatus(id: string, status: string, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const row = await store.get("acme", id);
    if (row?.status === status) return row;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`${id} never reached ${status}; last seen ${(await store.get("acme", id))?.status}`);
}

const post = (body: string) =>
  request(createApp({ store, queue, secret, toleranceSec: 300 }))
    .post("/webhooks/acme")
    .set("x-signature", sign(body, secret))
    .send(body);

describe("end to end", () => {
  it("delivers a webhook through RabbitMQ to the worker", async () => {
    expect((await post(JSON.stringify({ id: "e2e_ok", type: "call.completed", data: {} }))).status).toBe(202);
    const row = await waitForStatus("e2e_ok", "processed");
    expect(row.attempts).toBe(1);
  });

  it("retries a failing event through the delay queue until it succeeds", async () => {
    await post(JSON.stringify({ id: "e2e_flaky", type: "demo.flaky", data: { failTimes: 2 } }));
    const row = await waitForStatus("e2e_flaky", "processed");
    expect(row.attempts).toBe(3);
  });

  it("dead-letters an event that never succeeds, and replay brings it back", async () => {
    await post(JSON.stringify({ id: "e2e_dead", type: "demo.flaky", data: { failTimes: 99 } }));
    const dead = await waitForStatus("e2e_dead", "dead");
    expect(dead.attempts).toBe(4);
    expect(dead.last_error).toContain("simulated downstream failure");

    const replay = await request(createApp({ store, queue, secret, toleranceSec: 300 })).post("/events/acme/e2e_dead/replay");
    expect(replay.status).toBe(202);
    // It fails again (failTimes is still 99), which shows the replay really ran a fresh set of attempts.
    const again = await waitForStatus("e2e_dead", "dead");
    expect(again.attempts).toBe(4);
  });
});
