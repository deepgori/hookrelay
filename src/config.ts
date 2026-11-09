// All settings come from the environment, with defaults that match docker-compose.yml.
export const config = {
  port: Number(process.env.PORT ?? 3080),
  databaseUrl: process.env.DATABASE_URL ?? "postgres://hookrelay:hookrelay@localhost:5433/hookrelay",
  amqpUrl: process.env.AMQP_URL ?? "amqp://guest:guest@localhost:5673",
  // Shared secret each sender signs with. In production this would be per provider and stored in a secret manager.
  webhookSecret: process.env.WEBHOOK_SECRET ?? "dev-secret-change-me",
  // Reject signatures older than this, so a captured request can't be replayed later.
  signatureToleranceSec: Number(process.env.SIGNATURE_TOLERANCE_SEC ?? 300),
  maxAttempts: Number(process.env.MAX_ATTEMPTS ?? 5),
  retryDelayMs: Number(process.env.RETRY_DELAY_MS ?? 2000),
  // An event still "received" after this long was saved but never published (crash in between). The sweeper requeues it.
  sweepAfterMs: Number(process.env.SWEEP_AFTER_MS ?? 30_000),
};
