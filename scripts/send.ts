// Sends a signed webhook to the local server, then sends the exact same delivery again
// to show the duplicate being caught.
//   npm run send -- call.completed
//   npm run send -- demo.flaky 2      (fails twice, succeeds on the third attempt)
//   npm run send -- demo.flaky 9      (runs out of attempts and lands in the dead-letter queue)
import { randomUUID } from "node:crypto";
import { config } from "../src/config.js";
import { sign } from "../src/signature.js";

const type = process.argv[2] ?? "call.completed";
const failTimes = Number(process.argv[3] ?? 2);
const id = `evt_${randomUUID().slice(0, 8)}`;
const body = JSON.stringify({ id, type, data: { failTimes, from: "+15550100", durationSec: 84 } });
const url = `http://localhost:${config.port}/webhooks/acme`;

for (const attempt of ["first delivery", "provider retry"]) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", "x-signature": sign(body, config.webhookSecret) },
    body,
  });
  console.log(`${attempt}: ${res.status}`, await res.json());
}
console.log(`check status: curl localhost:${config.port}/events/acme/${id}`);
