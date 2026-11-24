import { createServer } from "node:http";
import { WebSocket, WebSocketServer } from "ws";
import { createStatsHandler } from "./stats.js";

const port = Number(process.env.PORT || 8080);
const grace = Number(process.env.PIPE_RECONNECT_MS ?? 90_000);
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("PORT must be an integer between 1 and 65535");
}
if (!Number.isSafeInteger(grace) || grace < 1 || grace > 2_147_483_647) {
  throw new Error("PIPE_RECONNECT_MS must be a positive timer interval");
}

const handleStats = createStatsHandler({
  databaseUrl: process.env.DATABASE_URL,
  path: process.env.STATS_DB_PATH,
  since: process.env.STATS_VISITORS_SINCE,
});
const server = createServer(handleStats);
const wss = new WebSocketServer({ server, maxPayload: 128 * 1024 });
server.listen(port, "0.0.0.0");
const pipes = new Map();

function send(socket, message) {
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
}

function expire(code, pipe, type = "pipe-expired") {
  if (pipes.get(code) !== pipe) return;
  pipes.delete(code);
  for (const participant of [pipe.sender, pipe.receiver]) {
    if (!participant) continue;
    clearTimeout(participant.timer);
    send(participant.socket, { type, code });
  }
}

wss.on("connection", (socket) => {
  let membership;
  socket.alive = true;
  socket.on("pong", () => { socket.alive = true; });
  socket.on("error", () => console.error("Signaling socket error"));

  socket.on("message", (raw) => {
    const reject = (message) => send(socket, { type: "error", message });
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      return reject("Invalid JSON message");
    }
    if (!message || typeof message !== "object" || Array.isArray(message)) return reject("Invalid message");
    const { type, code, token } = message;
    if (type === "ping") return send(socket, { type: "pong" });
    if (typeof code !== "string" || !/^\d{6}$/.test(code)) return reject("Pipe code must be exactly 6 digits");

    let pipe = pipes.get(code);
    if ([pipe?.sender, pipe?.receiver].some((participant) => participant?.deadline && participant.deadline <= Date.now())) {
      expire(code, pipe);
      pipe = undefined;
    }
    if (type === "create-pipe" || type === "join-pipe" || type === "resume-pipe") {
      if (typeof token !== "string" || !/^[a-f0-9]{64}$/.test(token)) return reject("Invalid session credentials");
      const role = type === "create-pipe" ? "sender" : type === "join-pipe" ? "receiver" : message.role;
      if (role !== "sender" && role !== "receiver") return reject("Invalid participant role");
      if (membership && (membership.code !== code || membership.role !== role || membership.pipe !== pipe)) {
        return reject("Socket is already registered");
      }

      if (!pipe) {
        if (type === "resume-pipe") return send(socket, { type: "pipe-expired", code });
        if (role === "receiver") return reject("Pipe not found");
        pipe = { sender: null, receiver: null };
        pipes.set(code, pipe);
      }

      let participant = pipe[role];
      if (participant && participant.token !== token) {
        if (type === "resume-pipe") return send(socket, { type: "pipe-expired", code });
        return reject(role === "sender" ? "This pipe code is already in use" : "This pipe already has a peer");
      }
      if (!participant && type === "resume-pipe") return send(socket, { type: "pipe-expired", code });
      participant ??= { token };
      const oldSocket = participant.socket;
      clearTimeout(participant.timer);
      participant.deadline = 0;
      participant.socket = socket;
      pipe[role] = participant;
      membership = { code, role, pipe, participant };
      if (oldSocket && oldSocket !== socket) oldSocket.close(4001, "Session replaced");
      send(socket, { type: type === "create-pipe" ? "pipe-created" : type === "join-pipe" ? "pipe-joined" : "pipe-resumed", code });

      if (role === "receiver" && message.reset === true) pipe.reset = true;
      const sender = pipe.sender?.socket;
      const receiver = pipe.receiver?.socket;
      if (sender?.readyState === WebSocket.OPEN && receiver?.readyState === WebSocket.OPEN) {
        send(receiver, { type: "peer-ready", code });
        send(sender, { type: "peer-ready", code, reset: Boolean(pipe.reset) });
        pipe.reset = false;
      }
      return;
    }

    if (!pipe || membership?.pipe !== pipe || membership.participant.socket !== socket) {
      return send(socket, { type: "pipe-expired", code });
    }
    if (type === "leave-pipe") return expire(code, pipe, "pipe-closed");
    const recipient = pipe[membership.role === "sender" ? "receiver" : "sender"]?.socket;
    if (type === "peer-reset" && membership.role === "receiver") {
      if (recipient?.readyState !== WebSocket.OPEN) pipe.reset = true;
      return send(recipient, { type: "peer-ready", code, reset: true });
    }
    if (type === "peer-check" && membership.role === "sender") {
      if (recipient?.readyState === WebSocket.OPEN) send(socket, { type: "peer-ready", code });
      return;
    }
    if (type === "webrtc-offer" || type === "webrtc-answer") {
      const field = type === "webrtc-offer" ? "offer" : "answer";
      const description = message[field];
      if (description?.type !== field || typeof description.sdp !== "string") return reject("Invalid WebRTC session description");
      return send(recipient, { type, code, [field]: { type: field, sdp: description.sdp } });
    }
    if (type === "webrtc-ice") {
      const candidate = message.candidate;
      if (typeof candidate?.candidate !== "string") return reject("Invalid ICE candidate");
      return send(recipient, {
        type, code,
        candidate: { candidate: candidate.candidate, sdpMid: candidate.sdpMid, sdpMLineIndex: candidate.sdpMLineIndex, usernameFragment: candidate.usernameFragment },
      });
    }
    reject("Unknown message type");
  });

  socket.on("close", () => {
    if (!membership) return;
    const { code, role, pipe, participant } = membership;
    if (pipes.get(code) !== pipe || participant.socket !== socket) return;
    participant.socket = null;
    participant.deadline = Date.now() + grace;
    participant.timer = setTimeout(() => expire(code, pipe), grace);
    participant.timer.unref();
    send(pipe[role === "sender" ? "receiver" : "sender"]?.socket, { type: "peer-away", code });
  });
});

const heartbeat = setInterval(() => {
  for (const socket of wss.clients) {
    if (!socket.alive) socket.terminate();
    else {
      socket.alive = false;
      socket.ping();
    }
  }
}, 15_000);
heartbeat.unref();

wss.on("listening", () => console.log(`Signaling server listening on 0.0.0.0:${port}`));
wss.on("error", (error) => {
  console.error("Signaling server error:", error);
  process.exitCode = 1;
});
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => {
    clearInterval(heartbeat);
    for (const socket of wss.clients) socket.terminate();
    wss.close();
    server.close(() => { void handleStats.close()?.catch(() => {}); });
  });
}
