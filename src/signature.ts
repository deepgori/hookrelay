import { createHmac, timingSafeEqual } from "node:crypto";

// Header format, the same shape Stripe uses:  x-signature: t=<unix seconds>,v1=<hex hmac>
// The HMAC covers "<timestamp>.<raw body>", so changing either the body or the time breaks it.

export function sign(rawBody: string | Buffer, secret: string, timestamp = Math.floor(Date.now() / 1000)): string {
  const mac = createHmac("sha256", secret).update(`${timestamp}.`).update(rawBody).digest("hex");
  return `t=${timestamp},v1=${mac}`;
}

export type VerifyResult = { ok: true } | { ok: false; reason: "missing" | "malformed" | "stale" | "mismatch" };

export function verify(
  rawBody: Buffer,
  header: string | undefined,
  secret: string,
  toleranceSec: number,
  now = Math.floor(Date.now() / 1000),
): VerifyResult {
  if (!header) return { ok: false, reason: "missing" };

  const parts = Object.fromEntries(
    header.split(",").map((kv) => {
      const i = kv.indexOf("=");
      return [kv.slice(0, i).trim(), kv.slice(i + 1).trim()];
    }),
  );
  const timestamp = Number(parts.t);
  const given = parts.v1;
  if (!Number.isInteger(timestamp) || !given || !/^[0-9a-f]{64}$/.test(given)) return { ok: false, reason: "malformed" };

  // Check age before the HMAC so an old captured request is rejected even if it was validly signed.
  if (Math.abs(now - timestamp) > toleranceSec) return { ok: false, reason: "stale" };

  const expected = createHmac("sha256", secret).update(`${timestamp}.`).update(rawBody).digest();
  const givenBytes = Buffer.from(given, "hex");
  // timingSafeEqual takes the same time whether the first byte or the last one differs,
  // so an attacker can't learn the signature one byte at a time from response timing.
  if (givenBytes.length !== expected.length || !timingSafeEqual(givenBytes, expected)) {
    return { ok: false, reason: "mismatch" };
  }
  return { ok: true };
}
