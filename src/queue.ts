import amqp from "amqplib";

// Three queues:
//   events.work          the worker consumes from here
//   events.retry.<N>ms   a failed message waits here for N ms (message TTL), then RabbitMQ
//                        dead-letters it back onto events.work for another try
//   events.dead          messages that used up every attempt; nothing consumes it, a person replays them
// The delay is part of the retry queue's name because RabbitMQ won't let you redeclare a queue
// with different settings. Changing the delay just creates a new queue.
export const QUEUES = { work: "events.work", dead: "events.dead" } as const;
export const retryQueueName = (delayMs: number) => `events.retry.${delayMs}ms`;

// Messages only carry the event's key. The worker reads the payload from Postgres,
// so the database stays the single source of truth and messages stay small.
export interface EventRef {
  provider: string;
  id: string;
}

export interface Queue {
  publish(queue: string, ref: EventRef): Promise<void>;
  retryQueue: string;
  channel: amqp.ConfirmChannel;
  close(): Promise<void>;
}

export async function connectQueue(url: string, retryDelayMs: number): Promise<Queue> {
  const connection = await amqp.connect(url);
  // A confirm channel makes publish wait until RabbitMQ has accepted the message,
  // so "published" in our code means the broker really has it.
  const channel = await connection.createConfirmChannel();

  await channel.assertQueue(QUEUES.work, { durable: true });
  const retryQueue = retryQueueName(retryDelayMs);
  await channel.assertQueue(retryQueue, {
    durable: true,
    arguments: {
      "x-message-ttl": retryDelayMs,
      "x-dead-letter-exchange": "",
      "x-dead-letter-routing-key": QUEUES.work,
    },
  });
  await channel.assertQueue(QUEUES.dead, { durable: true });

  return {
    channel,
    retryQueue,
    async publish(queue, ref) {
      channel.sendToQueue(queue, Buffer.from(JSON.stringify(ref)), {
        persistent: true,
        contentType: "application/json",
        messageId: `${ref.provider}:${ref.id}`,
      });
      await channel.waitForConfirms();
    },
    async close() {
      await channel.close().catch(() => {});
      await connection.close().catch(() => {});
    },
  };
}
