import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const directory = await mkdtemp(join(tmpdir(), "drophut-browser-"));
const processes = [];
const sockets = [];
const senderPort = "18809";
const vitePort = "18810";
const debugPort = "18811";
const site = "http://127.0.0.1:" + vitePort;
const debug = "http://127.0.0.1:" + debugPort;

function launch(command, args, env = {}) {
  const child = spawn(command, args, { env: { ...process.env, ...env }, stdio: "ignore" });
  processes.push(child);
  return child;
}

async function until(probe, description, timeout = 12_000) {
  const deadline = Date.now() + timeout;
  do {
    const value = await probe();
    if (value) return value;
    await delay(50);
  } while (Date.now() < deadline);
  throw new Error("Timed out waiting for " + description);
}

async function reachable(url) {
  return fetch(url).then((response) => response.ok).catch(() => false);
}

const instrument = `
  window.test = { sockets: [], peers: [], creates: 0, checkpoints: [], held: null, holdAt: 8 };
  const OriginalSocket = window.WebSocket;
  const OriginalPeer = window.RTCPeerConnection;
  window.WebSocket = class extends OriginalSocket {
    constructor(...args) {
      super(...args);
      this.signaling = new URL(args[0]).port === "${senderPort}";
      if (!this.signaling) return;
      window.test.sockets.push(this);
      const mode = sessionStorage.getItem("socket-mode");
      this.blocked = mode === "delayed" || mode === "stalled";
      this.addEventListener("open", (event) => {
        if (mode === "stalled") return;
        if (mode === "delayed") {
          window.test.openSocket = () => {
            this.blocked = false;
            this.handleOpen?.(event);
          };
        } else this.handleOpen?.(event);
      });
    }
    get readyState() { return this.blocked && super.readyState === 1 ? 0 : super.readyState; }
    set onopen(handler) {
      if (this.signaling) this.handleOpen = handler;
      else super.onopen = handler;
    }
    get onopen() { return this.signaling ? this.handleOpen : super.onopen; }
    set onmessage(handler) {
      if (!this.signaling || sessionStorage.getItem("socket-mode") !== "lost-reply") {
        super.onmessage = handler;
        return;
      }
      super.onmessage = (event) => {
        if (!window.test.lostReply && JSON.parse(event.data).type === "pipe-created") {
          window.test.lostReply = true;
          return;
        }
        handler?.(event);
      };
    }
    send(data) {
      if (this.signaling && JSON.parse(data).type === "create-pipe") window.test.creates++;
      return super.send(data);
    }
  };
  window.RTCPeerConnection = class extends OriginalPeer {
    constructor(...args) {
      super(...args);
      window.test.peers.push(this);
    }
  };
  const originalSend = RTCDataChannel.prototype.send;
  RTCDataChannel.prototype.send = function(data) {
    if (typeof data === "string") {
      const message = JSON.parse(data);
      if (message.type === "file-received" && window.test.holdCompletion) {
        window.test.completionHeld = true;
        return;
      }
      if (message.type === "file-checkpoint") window.test.checkpoints.push(message.nextChunk);
      if (message.type === "chunk-ack" && message.nextChunk === window.test.holdAt) {
        window.test.held = () => originalSend.call(this, data);
        return;
      }
    }
    if (data instanceof Blob && window.test.duplicateFirstChunk) {
      window.test.duplicateFirstChunk = false;
      originalSend.call(this, data);
    }
    return originalSend.call(this, data);
  };
  const setTimer = window.setTimeout;
  if (sessionStorage.getItem("socket-mode") === "stalled") {
    window.setTimeout = (callback, ms, ...args) => setTimer(callback,
      [25000, 1000, 2000, 4000].includes(ms) ? 50 : ms, ...args);
  } else if (sessionStorage.getItem("socket-mode") === "lost-reply") {
    window.setTimeout = (callback, ms, ...args) => setTimer(callback, ms === 8000 ? 80 : ms, ...args);
  }
`;

async function tab(mode) {
  const endpoint = await fetch(debug + "/json/new?about:blank", { method: "PUT" }).then((response) => response.json());
  const socket = new WebSocket(endpoint.webSocketDebuggerUrl);
  sockets.push(socket);
  await once(socket, "open");
  const requests = new Map();
  let sequence = 0;
  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    const settle = requests.get(message.id);
    if (settle) {
      requests.delete(message.id);
      settle(message);
    }
  });
  function call(method, params = {}) {
    return new Promise((resolve, reject) => {
      const id = ++sequence;
      requests.set(id, (message) => message.error ? reject(new Error(message.error.message)) : resolve(message.result));
      socket.send(JSON.stringify({ id, method, params }));
    });
  }
  async function evaluate(expression) {
    const { result, exceptionDetails } = await call("Runtime.evaluate", { expression, returnByValue: true });
    if (exceptionDetails) throw new Error(result.description);
    return result.value;
  }
  await call("Page.enable");
  await call("Browser.setDownloadBehavior", { behavior: "allow", downloadPath: directory });
  await call("Page.addScriptToEvaluateOnNewDocument", {
    source: `if (location.origin === ${JSON.stringify(site)}) {
      if (!sessionStorage.getItem("test-initialized")) {
        sessionStorage.setItem("test-initialized", "yes");
        sessionStorage.setItem("drophut-terms-accepted", "accepted");
        sessionStorage.setItem("socket-mode", ${JSON.stringify(mode ?? "")});
      }
      ${instrument}
    }`,
  });
  await call("Page.navigate", { url: site });
  const page = {
    call,
    evaluate,
    text: async (text) => {
      try {
        await until(() => evaluate(`document.body?.innerText.includes(${JSON.stringify(text)})`), text);
      } catch (error) {
        throw new Error((await evaluate("document.body?.innerText")) ?? "", { cause: error });
      }
    },
    click: (text) => until(() => evaluate(`(() => {
      const button = [...document.querySelectorAll("button")].find(entry =>
        entry.innerText.trim() === ${JSON.stringify(text)} && !entry.disabled);
      if (!button) return false;
      button.click();
      return true;
    })()`), "button " + text),
    wait: (expression) => until(() => evaluate(expression), expression),
    file: async (selector, ...files) => {
      const { root } = await call("DOM.getDocument");
      const { nodeId } = await call("DOM.querySelector", { nodeId: root.nodeId, selector });
      assert.ok(nodeId, selector);
      await call("DOM.setFileInputFiles", { nodeId, files });
    },
  };
  await page.text("Send Files");
  return page;
}

try {
  launch(process.execPath, ["server/server.js"], { PORT: senderPort });
  launch(process.execPath, ["node_modules/vite/bin/vite.js", "--host", "127.0.0.1", "--port", vitePort, "--strictPort"], {
    VITE_SIGNALING_URL: "ws://127.0.0.1:" + senderPort,
  });
  launch(process.env.CHROME_BIN ?? "google-chrome", [
    "--headless", "--no-sandbox", "--disable-dev-shm-usage", "--no-first-run",
    "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
    "--user-data-dir=" + join(directory, "chrome"), "--remote-debugging-port=" + debugPort, "about:blank",
  ]);
  await Promise.all([until(() => reachable(site), "Vite"), until(() => reachable(debug + "/json/version"), "Chrome")]);

  const stalled = await tab("stalled");
  await stalled.click("Send Files");
  await stalled.click("Create Pipe");
  await stalled.text("Could not connect to signaling.");
  assert.equal(await stalled.evaluate("test.creates"), 0);
  await stalled.evaluate('sessionStorage.removeItem("socket-mode")');
  await stalled.click("Retry");
  await stalled.wait("document.querySelector('.pipe-code')?.innerText");
  assert.equal(await stalled.evaluate("test.creates"), 1);
  console.log("Stalled handshake offers Retry");

  const preopened = await tab();
  await preopened.click("Send Files");
  await preopened.wait("test.sockets.at(-1)?.readyState === 1");
  await preopened.click("Create Pipe");
  await preopened.wait("document.querySelector('.pipe-code')?.innerText");
  assert.equal(await preopened.evaluate("test.creates"), 1);
  console.log("Create Pipe reuses the preopened socket");

  const lostReply = await tab("lost-reply");
  await lostReply.click("Send Files");
  await lostReply.click("Create Pipe");
  await lostReply.wait("test.lostReply === true");
  assert.equal(await lostReply.evaluate("document.querySelector('.pipe-code')"), null);
  const attemptedCode = await lostReply.evaluate('JSON.parse(sessionStorage.getItem("drophut-sender")).code');
  const replayedCode = await lostReply.wait("document.querySelector('.pipe-code')?.innerText");
  assert.equal(replayedCode, attemptedCode);
  assert.equal(await lostReply.evaluate("test.creates"), 2);
  console.log("Lost registration reply retries the original pipe and code");

  const sender = await tab("delayed");
  await sender.click("Send Files");
  await sender.wait("test.openSocket");
  await sender.click("Create Pipe");
  assert.equal(await sender.evaluate("test.creates"), 0);
  assert.equal(await sender.evaluate("document.querySelector('.pipe-code')"), null);
  await sender.evaluate("test.openSocket()");
  const code = await sender.wait("document.querySelector('.pipe-code')?.innerText");
  assert.equal(await sender.evaluate("test.creates"), 1);
  await sender.evaluate('sessionStorage.removeItem("socket-mode")');
  console.log("Create Pipe queues one request before the socket opens");

  const receiver = await tab();
  await receiver.click("Join a Pipe");
  await receiver.wait("Boolean(document.querySelector('#pipe-code'))");
  await receiver.evaluate(`(() => {
    const input = document.querySelector("#pipe-code");
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set.call(input, ${JSON.stringify(code)});
    input.dispatchEvent(new Event("input", { bubbles: true }));
  })()`);
  await receiver.click("Join Pipe");
  await sender.text("Ready to send");

  const bytes = randomBytes(900_003);
  const file = join(directory, "original", "payload.bin");
  const empty = join(directory, "original", "empty.bin");
  await mkdir(join(directory, "original"));
  await mkdir(join(directory, "wrong"));
  await writeFile(file, bytes);
  await writeFile(empty, "");
  await sender.evaluate("test.duplicateFirstChunk = true");
  await sender.file("#queue-file-picker", file, empty);
  await sender.click("Send queue");
  await receiver.wait("Boolean(test.held)");

  // Drop each signaling socket while the live DataChannel continues independently.
  await sender.evaluate("test.sockets.at(-1).close()");
  await receiver.evaluate("test.sockets.at(-1).close()");
  await receiver.evaluate("test.holdAt = 16; test.held(); test.held = null");
  await receiver.wait("Boolean(test.held)");
  assert.equal(await sender.evaluate("test.peers.length"), 1);
  await Promise.all([sender.wait("test.sockets.at(-1)?.readyState === 1"), receiver.wait("test.sockets.at(-1)?.readyState === 1")]);
  console.log("Transfer advances with both signaling sockets disconnected");

  await receiver.evaluate("test.holdAt = 24; test.held = null");
  await sender.evaluate("test.peers.at(-1).close()");
  await receiver.wait("test.checkpoints.includes(16)");
  await receiver.wait("Boolean(test.held)");
  console.log("DataChannel renegotiates and resumes at the saved offset");

  await receiver.call("Page.reload");
  await receiver.wait("test.checkpoints.includes(24)");
  await receiver.wait("Boolean(test.held) || test.checkpoints.length > 0");
  console.log("Receiver reload restores its checkpoint");

  await sender.text("All queued files were received successfully");
  const downloaded = join(directory, "payload.bin");
  await until(() => readFile(downloaded).catch(() => false), "download");
  assert.deepEqual(await readFile(downloaded), bytes);
  assert.equal((await readFile(join(directory, "empty.bin"))).length, 0);
  console.log("Recovered multi-file queue downloads are byte-correct with a duplicate chunk");

  await receiver.evaluate("test.holdAt = 8; test.held = null");
  const reloadFile = join(directory, "original", "reload.bin");
  const wrongReload = join(directory, "wrong", "reload.bin");
  await writeFile(reloadFile, bytes);
  await writeFile(wrongReload, Buffer.alloc(bytes.length));
  await sender.file("#queue-file-picker", reloadFile);
  await sender.click("Send queue");
  await receiver.wait("Boolean(test.held)");
  await receiver.evaluate("test.holdAt = -1");
  await sender.call("Page.reload");
  await sender.text("Reselect reload.bin");
  await sender.text("Ready to send");
  await sender.file('[aria-label="Reselect the original file"]', wrongReload);
  await sender.text("The selected file does not match.");
  await receiver.evaluate("test.holdCompletion = true");
  await sender.file('[aria-label="Reselect the original file"]', reloadFile);
  await receiver.wait("test.completionHeld === true");
  await receiver.evaluate("test.holdCompletion = false");
  await sender.evaluate("test.peers.at(-1).close()");
  await sender.text("All queued files were received successfully");
  assert.deepEqual(await readFile(join(directory, "reload.bin")), bytes);
  assert.deepEqual((await readdir(directory)).filter((name) => name.startsWith("reload")), ["reload.bin"]);
  console.log("Sender reload rejects changed bytes and resumes with the original file");
  console.log("Completion replay after losing the final acknowledgment causes no duplicate download");
} finally {
  for (const socket of sockets) socket.close();
  for (const child of processes.reverse()) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill();
      await once(child, "exit");
    }
  }
  await rm(directory, { recursive: true, force: true, maxRetries: 3 });
}
