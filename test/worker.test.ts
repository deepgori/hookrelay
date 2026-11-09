import type pg from "pg";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import type { EventStore } from "../src/db.js";
import type { Handler } from "../src/handlers.js";
import { QUEUES, type Queue } from "../src/queue.js";
import { handleMessage } from "../src/worker.js";
import { freshStore, recordingQueue } from "./helpers.js";

let pool: pg.Pool;
let store: EventStore;
let queue: ReturnType<typeof recordingQueue>;
const ref = { provider: "acme", id: "evt_1" };

beforeEach(async () => {
  ({ pool, store } = await freshStore());
  queue = recordingQueue();
  await store.insertIfNew({ ...ref, type: "call.completed", payload: {} });
});
afterAll(async () => pool?.end());

const run = (handler: Handler, maxAttempts = 3) =>
  handleMessage({ store, queue: queue as unknown as Queue, handlers: { "call.completed": handler }, maxAttempts }, ref);

describe("worker", () => {
  it("processes an event and records one attempt", async () => {
    expect(await run(async () => {})).toBe("processed");
    const row = await store.get("acme", "evt_1");
    expect(row?.status).toBe("processed");
    expect(row?.attempts).toBe(1);
  });

  it("sends a failure to the retry queue and keeps the error", async () => {
    expect(await run(async () => Promise.reject(new Error("CRM timed out")))).toBe("retry");
    expect(queue.published).toEqual([{ queue: queue.retryQueue, ref }]);
    expect((await store.get("acme", "evt_1"))?.last_error).toBe("CRM timed out");
  });

  it("succeeds on a later attempt after earlier failures", async () => {
    let calls = 0;
    const flaky: Handler = async () => {
      calls += 1;
      if (calls < 3) throw new Error("flaky");
    };
    expect(await run(flaky)).toBe("retry");
    expect(await run(flaky)).toBe("retry");
    expect(await run(flaky)).toBe("processed");
    expect((await store.get("acme", "evt_1"))?.attempts).toBe(3);
  });

  it("moves an event to the dead-letter queue after the last attempt", async () => {
    const broken: Handler = async () => Promise.reject(new Error("still down"));
    await run(broken);
    await run(broken);
    expect(await run(broken)).toBe("dead");
    expect(queue.published.at(-1)).toEqual({ queue: QUEUES.dead, ref });
    expect((await store.get("acme", "evt_1"))?.status).toBe("dead");
  });

  it("skips a redelivered message that was already processed, so the work runs once", async () => {
    let calls = 0;
    const counting: Handler = async () => {
      calls += 1;
    };
    await run(counting);
    expect(await run(counting)).toBe("skipped");
    expect(calls).toBe(1);
  });

  it("dead-letters an event type with no handler instead of retrying forever", async () => {
    await store.insertIfNew({ provider: "acme", id: "evt_2", type: "unknown.type", payload: {} });
    const deps = { store, queue: queue as unknown as Queue, handlers: {}, maxAttempts: 1 };
    expect(await handleMessage(deps, { provider: "acme", id: "evt_2" })).toBe("dead");
  });
});
