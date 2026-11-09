import type { EventStore } from "./db.js";
import { QUEUES, type Queue } from "./queue.js";

/**
 * Finds events that were saved but never published (the process died, or RabbitMQ was down,
 * between the insert and the publish) and publishes them. Running it twice is harmless:
 * the worker skips anything already processed.
 */
export async function sweepOnce(store: EventStore, queue: Pick<Queue, "publish">, olderThanMs: number): Promise<number> {
  const stuck = await store.findStuck(olderThanMs);
  for (const ref of stuck) {
    await queue.publish(QUEUES.work, ref);
    await store.markQueued(ref.provider, ref.id);
  }
  return stuck.length;
}
