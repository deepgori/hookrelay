import express, { type NextFunction, type Request, type Response } from "express";
import { z } from "zod";
import { verify } from "./signature.js";
import type { EventStore } from "./db.js";
import { QUEUES, type Queue } from "./queue.js";

const EventBody = z.object({
  id: z.string().min(1).max(200),
  type: z.string().min(1).max(100),
  data: z.record(z.unknown()),
});

const PROVIDER = /^[a-z0-9-]{1,40}$/;

export interface AppDeps {
  store: EventStore;
  queue: Pick<Queue, "publish">;
  secret: string;
  toleranceSec: number;
}

// Express 4 doesn't catch rejected promises from async handlers, so pass them to the error handler.
const route =
  (fn: (req: Request, res: Response) => Promise<unknown>) => (req: Request, res: Response, next: NextFunction) =>
    fn(req, res).catch(next);

export function createApp({ store, queue, secret, toleranceSec }: AppDeps) {
  const app = express();

  // The signature covers the exact bytes the sender sent, so read the body raw.
  // Parsing to JSON first and re-serializing could change whitespace or key order and break the check.
  app.post("/webhooks/:provider", express.raw({ type: "*/*", limit: "1mb" }), route(async (req, res) => {
    const provider = req.params.provider ?? "";
    if (!PROVIDER.test(provider)) return res.status(404).json({ error: "unknown provider" });

    const raw: Buffer = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
    const check = verify(raw, req.header("x-signature"), secret, toleranceSec);
    if (!check.ok) return res.status(401).json({ error: `signature ${check.reason}` });

    let parsed;
    try {
      parsed = EventBody.safeParse(JSON.parse(raw.toString("utf8")));
    } catch {
      return res.status(400).json({ error: "body is not JSON" });
    }
    if (!parsed.success) return res.status(400).json({ error: "invalid event", issues: parsed.error.issues });

    const { id, type, data } = parsed.data;
    const isNew = await store.insertIfNew({ provider, id, type, payload: data });
    if (!isNew) {
      // Providers retry when they don't get a 2xx in time. Answering 200 tells them to stop,
      // and we don't publish again, so the worker never sees the duplicate.
      return res.status(200).json({ status: "duplicate", id });
    }

    // Saved first, published second. If the process dies in between, the event stays 'received'
    // and the sweeper publishes it later, so nothing is lost.
    try {
      await queue.publish(QUEUES.work, { provider, id });
      await store.markQueued(provider, id);
    } catch (err) {
      console.error(`publish failed for ${provider}:${id}, sweeper will retry`, err);
    }
    return res.status(202).json({ status: "accepted", id });
  }));

  app.get("/events/:provider/:id", route(async (req, res) => {
    const { provider = "", id = "" } = req.params;
    const row = await store.get(provider, id);
    if (!row) return res.status(404).json({ error: "not found" });
    const { payload: _payload, ...summary } = row;
    return res.json(summary);
  }));

  // Manual replay for events that landed in the dead-letter queue, once the cause is fixed.
  app.post("/events/:provider/:id/replay", route(async (req, res) => {
    const { provider = "", id = "" } = req.params;
    const reset = await store.resetForReplay(provider, id);
    if (!reset) return res.status(409).json({ error: "only dead events can be replayed" });
    await queue.publish(QUEUES.work, { provider, id });
    return res.status(202).json({ status: "requeued", id });
  }));

  app.get("/health", (_req, res) => res.json({ ok: true }));

  // If Postgres is down we answer 500, and the provider retries the delivery later. That's the behavior we want:
  // a 2xx would tell them we have the event when we don't.
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    console.error(err);
    res.status(500).json({ error: "internal error" });
  });

  return app;
}
