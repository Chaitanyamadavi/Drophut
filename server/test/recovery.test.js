import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import WebSocket from "ws";

test("registration retries and private participant recovery", { timeout: 12_000 }, async (t) => {
  const port = process.env.TEST_SIGNALING_PORT ?? "18808";
  const server = spawn(process.execPath, ["server/server.js"], {
    env: { ...process.env, PORT: port, PIPE_RECONNECT_MS: "1200" },
  });
  t.after(() => server.kill());
  await Promise.race([
    once(server.stdout, "data"),
    once(server, "exit").then(() => assert.fail("The signaling server did not start")),
  ]);

  async function connect() {
    const socket = new WebSocket("ws://127.0.0.1:" + port);
    t.after(() => socket.terminate());
    const inbox = [];
    socket.on("message", (raw) => inbox.push(JSON.parse(raw)));
    await once(socket, "open");
    socket.sendMessage = (message) => socket.send(JSON.stringify(message));
    socket.expect = async (type) => {
      for (let attempt = 0; attempt < 100; attempt += 1) {
        const position = inbox.findIndex((message) => message.type === type);
        if (position !== -1) return inbox.splice(position, 1)[0];
        await delay(10);
      }
      assert.fail("Expected " + type + ", received " + JSON.stringify(inbox));
    };
    return socket;
  }

  const code = "123456";
  const senderToken = randomBytes(32).toString("hex");
  const receiverToken = randomBytes(32).toString("hex");
  const create = { type: "create-pipe", code, token: senderToken };
  const original = await connect();
  original.sendMessage(create);
  // Simulate losing the creation reply, with the old socket closing after replacement.
  const sender = await connect();
  const originalClosed = once(original, "close");
  sender.sendMessage(create);
  assert.deepEqual(await sender.expect("pipe-created"), { type: "pipe-created", code });
  const [oldCloseCode] = await Promise.race([
    originalClosed,
    delay(1500).then(() => assert.fail("Original socket was not replaced")),
  ]);
  assert.equal(oldCloseCode, 4001);

  sender.close();
  await once(sender, "close");
  const receiver = await connect();
  receiver.sendMessage({ type: "join-pipe", code, token: receiverToken, reset: true });
  assert.deepEqual(await receiver.expect("pipe-joined"), { type: "pipe-joined", code });

  const intruder = await connect();
  intruder.sendMessage({ type: "resume-pipe", code, role: "receiver", token: senderToken });
  await intruder.expect("pipe-expired");
  intruder.sendMessage({ type: "join-pipe", code, token: randomBytes(32).toString("hex") });
  await intruder.expect("error");

  const resumedSender = await connect();
  resumedSender.sendMessage({ type: "resume-pipe", code, role: "sender", token: senderToken });
  await resumedSender.expect("pipe-resumed");
  assert.equal((await resumedSender.expect("peer-ready")).reset, true);

  receiver.close();
  await resumedSender.expect("peer-away");
  const resumedReceiver = await connect();
  resumedReceiver.sendMessage({ type: "resume-pipe", code, role: "receiver", token: receiverToken });
  await resumedReceiver.expect("pipe-resumed");
  assert.equal((await resumedSender.expect("peer-ready")).reset, false);
  resumedSender.sendMessage({ type: "webrtc-offer", code, offer: { type: "offer", sdp: "test" }, token: senderToken });
  assert.deepEqual(await resumedReceiver.expect("webrtc-offer"), { type: "webrtc-offer", code, offer: { type: "offer", sdp: "test" } });

  resumedReceiver.close();
  await resumedSender.expect("peer-away");
  await delay(1250);
  await resumedSender.expect("pipe-expired");
  const expired = await connect();
  expired.sendMessage({ type: "resume-pipe", code, role: "receiver", token: receiverToken });
  await expired.expect("pipe-expired");

  const waiting = await connect();
  waiting.sendMessage({ ...create, code: "654321" });
  await waiting.expect("pipe-created");
  waiting.close();
  await once(waiting, "close");
  await delay(1250);
  expired.sendMessage({ type: "resume-pipe", code: "654321", role: "sender", token: senderToken });
  await expired.expect("pipe-expired");
});
