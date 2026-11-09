import pg from "pg";

export type EventStatus = "received" | "queued" | "processed" | "dead";

export interface EventRow {
  id: string;
  provider: string;
  type: string;
  payload: unknown;
  status: EventStatus;
  attempts: number;
  last_error: string | null;
  received_at: Date;
  processed_at: Date | null;
}

// One row per provider event. The primary key (provider, id) is what makes intake exactly-once:
// a provider that retries the same delivery hits the same key, and the insert does nothing.
const SCHEMA = `
CREATE TABLE IF NOT EXISTS events (
  provider     text        NOT NULL,
  id           text        NOT NULL,
  type         text        NOT NULL,
  payload      jsonb       NOT NULL,
  status       text        NOT NULL DEFAULT 'received'
               CHECK (status IN ('received', 'queued', 'processed', 'dead')),
  attempts     integer     NOT NULL DEFAULT 0,
  last_error   text,
  received_at  timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz,
  PRIMARY KEY (provider, id)
);
CREATE INDEX IF NOT EXISTS events_status_received_idx ON events (status, received_at);
`;

export function createPool(connectionString: string): pg.Pool {
  return new pg.Pool({ connectionString, max: 10 });
}

export async function migrate(pool: pg.Pool): Promise<void> {
  await pool.query(SCHEMA);
}

export class EventStore {
  constructor(private readonly pool: pg.Pool) {}

  /** Returns true if this is the first time we've seen the event, false if it's a duplicate delivery. */
  async insertIfNew(e: { provider: string; id: string; type: string; payload: unknown }): Promise<boolean> {
    const res = await this.pool.query(
      `INSERT INTO events (provider, id, type, payload) VALUES ($1, $2, $3, $4)
       ON CONFLICT (provider, id) DO NOTHING`,
      [e.provider, e.id, e.type, JSON.stringify(e.payload)],
    );
    return res.rowCount === 1;
  }

  async get(provider: string, id: string): Promise<EventRow | null> {
    const res = await this.pool.query<EventRow>(`SELECT * FROM events WHERE provider = $1 AND id = $2`, [provider, id]);
    return res.rows[0] ?? null;
  }

  async markQueued(provider: string, id: string): Promise<void> {
    await this.pool.query(`UPDATE events SET status = 'queued' WHERE provider = $1 AND id = $2 AND status = 'received'`, [
      provider,
      id,
    ]);
  }

  /** Counts one processing attempt and returns the new total. */
  async recordAttempt(provider: string, id: string, error: string | null): Promise<number> {
    const res = await this.pool.query<{ attempts: number }>(
      `UPDATE events SET attempts = attempts + 1, last_error = $3
       WHERE provider = $1 AND id = $2 RETURNING attempts`,
      [provider, id, error],
    );
    return res.rows[0]?.attempts ?? 0;
  }

  async markProcessed(provider: string, id: string): Promise<void> {
    await this.pool.query(
      `UPDATE events SET status = 'processed', processed_at = now(), last_error = NULL WHERE provider = $1 AND id = $2`,
      [provider, id],
    );
  }

  async markDead(provider: string, id: string): Promise<void> {
    await this.pool.query(`UPDATE events SET status = 'dead' WHERE provider = $1 AND id = $2`, [provider, id]);
  }

  /** Puts a dead event back in line with a fresh attempt count. Returns false if it wasn't dead. */
  async resetForReplay(provider: string, id: string): Promise<boolean> {
    const res = await this.pool.query(
      `UPDATE events SET status = 'queued', attempts = 0, last_error = NULL
       WHERE provider = $1 AND id = $2 AND status = 'dead'`,
      [provider, id],
    );
    return res.rowCount === 1;
  }

  /** Events saved but never published, e.g. the process died between the insert and the publish. */
  async findStuck(olderThanMs: number, limit = 100): Promise<Pick<EventRow, "provider" | "id">[]> {
    const res = await this.pool.query<Pick<EventRow, "provider" | "id">>(
      `SELECT provider, id FROM events
       WHERE status = 'received' AND received_at < now() - ($1 || ' milliseconds')::interval
       ORDER BY received_at LIMIT $2`,
      [String(olderThanMs), limit],
    );
    return res.rows;
  }
}
