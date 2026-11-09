import type { EventRow } from "./db.js";

// What to do with each event type. In a real integration these would call a CRM or another
// downstream API. Here they just log, plus one deliberately flaky type to show retries working.
//
// Handlers must be safe to run twice for the same event: RabbitMQ delivers at least once, so a crash
// after the work but before the ack means the message comes back.
export type Handler = (event: EventRow, attempt: number) => Promise<void>;

export const handlers: Record<string, Handler> = {
  async "call.completed"(event) {
    console.log(`[crm] logging call ${event.id}`, event.payload);
  },

  async "message.received"(event) {
    console.log(`[crm] syncing message ${event.id}`, event.payload);
  },

  // Demo only: fails its first `failTimes` attempts, then succeeds. Set failTimes high to see it go to the dead-letter queue.
  async "demo.flaky"(event, attempt) {
    const failTimes = Number((event.payload as { failTimes?: number }).failTimes ?? 2);
    if (attempt <= failTimes) throw new Error(`simulated downstream failure (attempt ${attempt} of ${failTimes} planned)`);
    console.log(`[demo] ${event.id} succeeded on attempt ${attempt}`);
  },
};
