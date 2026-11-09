import { describe, expect, it } from "vitest";
import { sign, verify } from "../src/signature.js";

const secret = "test-secret";
const body = Buffer.from('{"id":"evt_1","type":"call.completed","data":{}}');
const now = 1_800_000_000;

describe("signature", () => {
  it("accepts a correctly signed body", () => {
    expect(verify(body, sign(body, secret, now), secret, 300, now)).toEqual({ ok: true });
  });

  it("rejects a body changed after signing", () => {
    const tampered = Buffer.from(body.toString().replace("evt_1", "evt_2"));
    expect(verify(tampered, sign(body, secret, now), secret, 300, now)).toEqual({ ok: false, reason: "mismatch" });
  });

  it("rejects the wrong secret", () => {
    expect(verify(body, sign(body, "other", now), secret, 300, now)).toEqual({ ok: false, reason: "mismatch" });
  });

  it("rejects a replay of an old, validly signed request", () => {
    const header = sign(body, secret, now - 301);
    expect(verify(body, header, secret, 300, now)).toEqual({ ok: false, reason: "stale" });
  });

  it("rejects missing and malformed headers", () => {
    expect(verify(body, undefined, secret, 300, now)).toEqual({ ok: false, reason: "missing" });
    expect(verify(body, "v1=abc", secret, 300, now)).toEqual({ ok: false, reason: "malformed" });
    expect(verify(body, `t=${now},v1=zz`, secret, 300, now)).toEqual({ ok: false, reason: "malformed" });
  });
});
