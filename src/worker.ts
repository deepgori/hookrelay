import type { ConsumeMessage } from "amqplib";
import type { EventStore } from "./db.js";
import type { Handler } from "./handlers.js";
import { QUEUES, type EventRef, type Queue } from "./queue.js";

export interface WorkerDeps {
  store: EventStore;
  queue: Queue;
  handlers: Record<string, Handler>;
  maxAttempts: number;
}

export type Outcome = "processed" | "retry" | "dead" | "skipped";

/**
 * Handles one message. Always ends by acking it: a failure is moved to the retry or dead queue
 * explicitly, rather than nacked, so we control the delay and count attempts in Postgres.
 */
export async function handleMessage(deps: WorkerDeps, ref: EventRef): Promise<Outcome> {
  const { store, queue, handlers, maxAttempts } = deps;
  const event = await store.get(ref.provider, ref.id);

  // Already done (a redelivery after a crash, or a duplicate publish from the sweeper): don't do the work twice.
  if (!event || event.status === "processed" || event.status === "dead") return "skipped";

  const handler = handlers[event.type];
  try {
    if (!handler) throw new Error(`no handler for event type "${event.type}"`);
    await handler(event, event.attempts + 1);
    await store.recordAttempt(ref.provider, ref.id, null);
    await store.markProcessed(ref.provider, ref.id);
    return "processed";
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const attempts = await store.recordAttempt(ref.provider, ref.id, message);
    if (attempts >= maxAttempts) {
      await store.markDead(ref.provider, ref.id);
      await queue.publish(QUEUES.dead, ref);
      console.warn(`[worker] ${ref.provider}:${ref.id} dead after ${attempts} attempts: ${message}`);
      return "dead";
    }
    await queue.publish(queue.retryQueue, ref);
    console.warn(`[worker] ${ref.provider}:${ref.id} attempt ${attempts} failed, retrying: ${message}`);
    return "retry";
  }
}

export async function startWorker(deps: WorkerDeps, onOutcome?: (ref: EventRef, outcome: Outcome) => void) {
  const { channel } = deps.queue;
  // At most 10 unacked messages per worker, so one slow handler can't pull the whole backlog into memory.
  await channel.prefetch(10);
  const { consumerTag } = await channel.consume(QUEUES.work, async (msg: ConsumeMessage | null) => {
    if (!msg) return;
    let ref: EventRef;
    try {
      ref = JSON.parse(msg.content.toString("utf8"));
    } catch {
      channel.ack(msg); // unreadable message: retrying won't help
      return;
    }
    try {
      const outcome = await handleMessage(deps, ref);
      channel.ack(msg);
      onOutcome?.(ref, outcome);
    } catch (err) {
      // Postgres or RabbitMQ itself failed. Put the message back so it isn't lost.
      console.error(`[worker] infrastructure error on ${ref.provider}:${ref.id}`, err);
      channel.nack(msg, false, true);
    }
  });
  return { stop: () => channel.cancel(consumerTag) };
}
