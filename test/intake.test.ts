import type pg from "pg";
import request from "supertest";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/app.js";
import type { EventStore } from "../src/db.js";
import { QUEUES } from "../src/queue.js";
import { sign } from "../src/signature.js";
import { sweepOnce } from "../src/sweeper.js";
import { freshStore, recordingQueue } from "./helpers.js";

const secret = "test-secret";
let pool: pg.Pool;
let store: EventStore;
let queue: ReturnType<typeof recordingQueue>;

beforeEach(async () => {
  ({ pool, store } = await freshStore());
  queue = recordingQueue();
});
afterAll(async () => pool?.end());

const app = () => createApp({ store, queue, secret, toleranceSec: 300 });
const deliver = (body: string, header = sign(body, secret)) =>
  request(app()).post("/webhooks/acme").set("content-type", "application/json").set("x-signature", header).send(body);

describe("webhook intake", () => {
  const body = JSON.stringify({ id: "evt_1", type: "call.completed", data: { from: "+15550100" } });

  it("accepts a signed event, stores it and publishes it once", async () => {
    const res = await deliver(body);
    expect(res.status).toBe(202);
    expect(queue.published).toEqual([{ queue: QUEUES.work, ref: { provider: "acme", id: "evt_1" } }]);
    expect((await store.get("acme", "evt_1"))?.status).toBe("queued");
  });

  it("treats a repeated delivery as a duplicate and does not publish it again", async () => {
    await deliver(body);
    const again = await deliver(body);
    expect(again.status).toBe(200);
    expect(again.body.status).toBe("duplicate");
    expect(queue.published).toHaveLength(1);
  });

  it("handles concurrent duplicate deliveries without double-publishing", async () => {
    const results = await Promise.all(Array.from({ length: 8 }, () => deliver(body)));
    expect(results.filter((r) => r.status === 202)).toHaveLength(1);
    expect(queue.published).toHaveLength(1);
  });

  it("rejects a bad signature without storing anything", async () => {
    const res = await deliver(body, sign(body, "wrong-secret"));
    expect(res.status).toBe(401);
    expect(await store.get("acme", "evt_1")).toBeNull();
    expect(queue.published).toHaveLength(0);
  });

  it("rejects a signed body that isn't a valid event", async () => {
    const bad = JSON.stringify({ type: "call.completed" });
    expect((await deliver(bad)).status).toBe(400);
  });

  it("keeps the event if publishing fails, and the sweeper publishes it later", async () => {
    const failing = { publish: async () => Promise.reject(new Error("broker down")) };
    const res = await request(createApp({ store, queue: failing, secret, toleranceSec: 300 }))
      .post("/webhooks/acme")
      .set("x-signature", sign(body, secret))
      .send(body);
    expect(res.status).toBe(202);
    expect((await store.get("acme", "evt_1"))?.status).toBe("received");

    expect(await sweepOnce(store, queue, 0)).toBe(1);
    expect(queue.published).toEqual([{ queue: QUEUES.work, ref: { provider: "acme", id: "evt_1" } }]);
    expect((await store.get("acme", "evt_1"))?.status).toBe("queued");
  });
});
