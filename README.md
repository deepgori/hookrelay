# hookrelay

A small webhook intake service in TypeScript. It takes events from outside providers, checks they're genuine, stores each one exactly once, and processes them through RabbitMQ with retries and a dead-letter queue.

It's the part of an integration that usually gets written quickly and then breaks quietly: a provider retries a delivery and the CRM gets the same call logged twice, or the downstream API is down for a minute and events vanish.

## How a webhook moves through it

```
provider ──POST /webhooks/:provider──▶ verify signature ──▶ INSERT ... ON CONFLICT DO NOTHING
                                                               │ new          │ already seen
                                                               ▼              ▼
                                                     publish to events.work   200 "duplicate"
                                                               │
                                     worker ◀──────────────────┘
                                       │ ok ──────────▶ status = processed
                                       │ failed, attempts left ──▶ events.retry.<N>ms ──(TTL)──▶ events.work
                                       │ failed, out of attempts ─▶ events.dead  (replay by hand)
```

## Decisions worth explaining

**Signatures over the raw body.** The sender signs `timestamp.body` with HMAC-SHA256, the same scheme Stripe uses. The route reads the body as raw bytes, because parsing to JSON and re-serializing can change whitespace and break the check. Requests older than five minutes are rejected even when correctly signed, so a captured request can't be replayed. The comparison uses `timingSafeEqual`.

**Exactly-once intake, at-least-once processing.** Providers retry when they don't get a 2xx fast enough, so the same event arrives more than once. The `(provider, id)` primary key makes the insert a no-op the second time, and the duplicate is never published. RabbitMQ itself delivers at least once, so the worker checks the row before doing any work and skips events that are already processed. Handlers still need to be safe to run twice, because a crash after the work but before the ack brings the message back.

**Save first, then publish, plus a sweeper.** The event is committed to Postgres before it's published. If the process dies between the two, or RabbitMQ is down, the row stays `received`, and a sweeper publishes anything stuck in that state for more than 30 seconds. This is a simplified transactional outbox: the events table doubles as the outbox.

**Retries without a scheduler.** A failed message goes to a retry queue whose message TTL is the delay. When the TTL runs out RabbitMQ dead-letters it back onto the work queue. Attempts are counted in Postgres, not in message headers, so the count survives redeliveries. After the last attempt the event goes to `events.dead`, which nothing consumes. `POST /events/:provider/:id/replay` puts it back once the cause is fixed. The retry queue's name includes the delay, because RabbitMQ refuses to redeclare a queue with different settings.

**Messages carry only the key.** The queue holds `{provider, id}`, and the worker reads the payload from Postgres, which stays the single source of truth.

## What I'd add for production

- Exponential backoff, using a few retry queues with growing delays instead of one fixed delay.
- One signing secret per provider, kept in a secret manager, with support for rotating it.
- Metrics and alerts on dead-letter queue depth and processing latency.
- A real outbox table written in the same transaction as business data, once the service does more than store events.

## Run it

```bash
docker compose up -d --wait      # Postgres on 5433, RabbitMQ on 5673 (management UI on 15673)
npm install
npm start                        # API on :3080
npm run worker                   # in a second terminal

npm run send -- call.completed   # sends a signed event, then the same delivery again
npm run send -- demo.flaky 2     # fails twice, succeeds on the third attempt
npm run send -- demo.flaky 9     # runs out of attempts and lands in events.dead
```

## Tests

```bash
npm test
```

20 tests: signature checks (tampering, wrong secret, replay of an old request), intake (duplicates, eight concurrent copies of the same delivery, a broker outage recovered by the sweeper), the worker's retry and dead-letter logic, and end-to-end runs through the real RabbitMQ, including a delayed retry and a replay from the dead-letter queue. The tests use their own `hookrelay_test` database.
