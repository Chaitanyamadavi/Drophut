import { WebSocketServer } from "ws";

const port = Number(process.env.PORT || 8080);
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("PORT must be an integer between 1 and 65535");
}

// Bind to all interfaces so remote clients can reach the signaling server.
const wss = new WebSocketServer({ port, host: "0.0.0.0" });

// Each code maps to its creator and, after joining, the connected peer.
const pipes = new Map();

function send(socket, message) {
  if (socket.readyState === socket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

wss.on("connection", (socket) => {
  console.log("New client connected");

  socket.on("error", (error) => {
    console.error("WebSocket client error:", error);
  });

  socket.on("message", (rawMessage) => {
    let message;

    try {
      message = JSON.parse(rawMessage.toString());
    } catch {
      send(socket, { type: "error", message: "Invalid JSON message" });
      return;
    }

    if (!message || typeof message !== "object" || Array.isArray(message)) {
      send(socket, { type: "error", message: "Invalid message format" });
      return;
    }

    const { type, code } = message;

    if (typeof code !== "string" || !/^\d{6}$/.test(code)) {
      send(socket, { type: "error", message: "Pipe code must be exactly 6 digits" });
      return;
    }

    if (type === "create-pipe") {
      if (pipes.has(code)) {
        send(socket, { type: "error", code, message: "This pipe code is already in use" });
        return;
      }

      // The creator socket is stored with the code so a joiner can find it.
      pipes.set(code, { creator: socket, peer: null });
      send(socket, { type: "pipe-created", code });
      return;
    }

    if (type === "join-pipe") {
      const pipe = pipes.get(code);
      if (!pipe || pipe.creator.readyState !== pipe.creator.OPEN) {
        send(socket, { type: "error", code, message: "Pipe not found" });
        return;
      }
      if (pipe.creator === socket || pipe.peer) {
        send(socket, { type: "error", code, message: "This pipe already has a peer" });
        return;
      }

      pipe.peer = socket;
      // A successful join notifies both the creator and the joining client.
      send(pipe.creator, { type: "peer-joined", code });
      send(socket, { type: "peer-joined", code });
      return;
    }

    if (type === "webrtc-offer" || type === "webrtc-answer" || type === "webrtc-ice") {
      const pipe = pipes.get(code);
      if (!pipe || !pipe.peer) {
        send(socket, { type: "error", code, message: "Pipe is not connected to a peer" });
        return;
      }

      const recipient = pipe.creator === socket ? pipe.peer : pipe.peer === socket ? pipe.creator : null;
      if (!recipient) {
        send(socket, { type: "error", code, message: "Socket is not part of this pipe" });
        return;
      }

      if (type === "webrtc-offer" || type === "webrtc-answer") {
        const description = type === "webrtc-offer" ? message.offer : message.answer;
        const expectedType = type === "webrtc-offer" ? "offer" : "answer";
        if (!description || typeof description !== "object" || description.type !== expectedType || typeof description.sdp !== "string") {
          send(socket, { type: "error", code, message: "Invalid WebRTC session description" });
          return;
        }
        send(recipient, { type, code, [type === "webrtc-offer" ? "offer" : "answer"]: description });
        return;
      }

      if (!message.candidate || typeof message.candidate !== "object" || typeof message.candidate.candidate !== "string") {
        send(socket, { type: "error", code, message: "Invalid ICE candidate" });
        return;
      }
      send(recipient, { type, code, candidate: message.candidate });
      return;
    }

    send(socket, { type: "error", code, message: "Unknown message type" });
  });

  socket.on("close", () => {
    // Tell the remaining participant to tear down its peer connection, then remove the pipe.
    for (const [code, pipe] of pipes) {
      if (pipe.creator === socket || pipe.peer === socket) {
        const remainingPeer = pipe.creator === socket ? pipe.peer : pipe.creator;
        if (remainingPeer) {
          send(remainingPeer, { type: "error", code, message: "The other peer disconnected" });
        }
        pipes.delete(code);
      }
    }
    console.log("Client disconnected");
  });
});

wss.on("listening", () => {
  console.log(`Signaling server listening on 0.0.0.0:${port}`);
});

wss.on("error", (error) => {
  console.error("WebSocket server error:", error);
  process.exitCode = 1;
});

let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${signal} received; closing WebSocket server`);

  for (const socket of wss.clients) {
    socket.close(1001, "Server shutting down");
  }

  wss.close((error) => {
    pipes.clear();
    if (error) {
      console.error("Error while closing WebSocket server:", error);
      process.exitCode = 1;
      return;
    }
    console.log("WebSocket server closed");
  });
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));
