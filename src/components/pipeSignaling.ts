export type Role = "sender" | "receiver";
export type Session = { code: string; token: string; confirmed: boolean };
export type Signal = {
  type: string;
  code?: string;
  message?: string;
  reset?: boolean;
  offer?: RTCSessionDescriptionInit;
  answer?: RTCSessionDescriptionInit;
  candidate?: RTCIceCandidateInit;
};
export type SignalState = "connecting" | "connected" | "retrying" | "failed";

const HANDSHAKE_MS = 25_000;
const REPLY_MS = 8_000;
const MAX_ATTEMPTS = 4;

export function readSession(role: Role): Session | null {
  try {
    const session: Session | null = JSON.parse(sessionStorage.getItem(`drophut-${role}`) ?? "null");
    return session && /^\d{6}$/.test(session.code) && /^[a-f0-9]{64}$/.test(session.token) ? session : null;
  } catch {
    return null;
  }
}

function saveSession(role: Role, session: Session | null) {
  try {
    if (session) sessionStorage.setItem(`drophut-${role}`, JSON.stringify(session));
    else sessionStorage.removeItem(`drophut-${role}`);
  } catch {
    // The live tab can still resume using its in-memory credentials.
  }
}

export class PipeSignaling {
  session: Session | null;
  private socket: WebSocket | null = null;
  private timer = 0;
  private attempts = 0;
  private stopped = false;
  private registered = false;

  private role: Role;
  private onState: (state: SignalState) => void;
  private onMessage: (message: Signal) => void;
  private needsPeer: () => boolean;
  private checkPeer: () => void;

  constructor(role: Role, onState: (state: SignalState) => void,
    onMessage: (message: Signal) => void, needsPeer: () => boolean, checkPeer: () => void) {
    this.role = role;
    this.onState = onState;
    this.onMessage = onMessage;
    this.needsPeer = needsPeer;
    this.checkPeer = checkPeer;
    this.session = readSession(role);
    window.addEventListener("online", this.wake);
    document.addEventListener("visibilitychange", this.wake);
  }

  register(code: string) {
    if (this.session) return;
    const token = Array.from(crypto.getRandomValues(new Uint8Array(32)), (byte) => byte.toString(16).padStart(2, "0")).join("");
    this.session = { code, token, confirmed: false };
    saveSession(this.role, this.session);
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.attempts = 1;
      this.onState("connecting");
      this.request(this.socket);
    }
    else if (!this.timer) this.retry();
  }

  private request(socket: WebSocket) {
    const session = this.session;
    if (!session) return;
    this.arm(socket, REPLY_MS);
    try {
      socket.send(JSON.stringify({
        type: session.confirmed ? "resume-pipe" : this.role === "sender" ? "create-pipe" : "join-pipe",
        code: session.code, token: session.token, role: this.role, reset: this.needsPeer(),
      }));
    } catch {
      this.replace(socket);
    }
  }

  private arm(socket: WebSocket, delay: number) {
    this.clearTimer();
    this.timer = window.setTimeout(() => this.replace(socket), delay);
  }

  private clearTimer() {
    window.clearTimeout(this.timer);
    this.timer = 0;
  }

  private replace(socket: WebSocket) {
    if (this.socket !== socket) return;
    this.socket = null;
    this.registered = false;
    this.clearTimer();
    socket.close();
    this.schedule();
  }

  private schedule() {
    if (this.stopped) return;
    if (this.attempts >= MAX_ATTEMPTS) {
      this.onState("failed");
      return;
    }
    this.onState("retrying");
    this.timer = window.setTimeout(() => {
      this.timer = 0;
      this.open();
    }, 1000 * 2 ** (this.attempts - 1));
  }

  open() {
    if (this.stopped || this.socket) return;
    this.clearTimer();
    this.attempts += 1;
    this.onState(this.attempts === 1 && !this.session?.confirmed ? "connecting" : "retrying");
    let socket: WebSocket;
    try {
      socket = new WebSocket(import.meta.env.VITE_SIGNALING_URL ||
        (import.meta.env.DEV ? "ws://localhost:8080" : "wss://drophut.onrender.com"));
    } catch {
      this.schedule();
      return;
    }
    this.socket = socket;
    this.arm(socket, HANDSHAKE_MS);
    socket.onopen = () => {
      if (this.socket !== socket) return;
      this.clearTimer();
      if (this.session) this.request(socket);
      else {
        this.attempts = 0;
        this.onState("connected");
      }
    };
    socket.onclose = (event) => {
      if (this.socket !== socket) return;
      if (event.code === 4001) {
        this.stop();
        this.onMessage({ type: "error", message: "This session was resumed in another tab." });
        return;
      }
      this.replace(socket);
    };
    socket.onmessage = (event) => {
      if (this.socket !== socket) return;
      let message: Signal;
      try {
        message = JSON.parse(event.data);
      } catch {
        this.replace(socket);
        return;
      }
      if (message.type === "pong") {
        if (this.registered || !this.session) this.clearTimer();
        return;
      }
      if (!this.session || (message.code && message.code !== this.session.code)) return;
      if (["pipe-created", "pipe-joined", "pipe-resumed"].includes(message.type)) {
        this.clearTimer();
        this.session.confirmed = true;
        saveSession(this.role, this.session);
        this.registered = true;
        this.attempts = 0;
        this.onState("connected");
      }
      this.onMessage(message);
    };
  }

  send(message: Signal) {
    const socket = this.socket;
    if (!this.registered || socket?.readyState !== WebSocket.OPEN) return false;
    try {
      socket.send(JSON.stringify(message));
      return true;
    } catch {
      this.replace(socket);
      return false;
    }
  }

  retry = () => {
    this.clearTimer();
    this.attempts = 0;
    if (this.socket) {
      const socket = this.socket;
      this.socket = null;
      this.registered = false;
      socket.close();
    }
    this.open();
  };

  private wake = (event: Event) => {
    if (this.stopped || (event.type === "visibilitychange" && document.visibilityState === "hidden")) return;
    this.checkPeer();
    const socket = this.socket;
    if (!socket || socket.readyState >= WebSocket.CLOSING) {
      this.retry();
    } else if (socket.readyState === WebSocket.OPEN && (this.registered || !this.session) && !this.timer) {
      this.arm(socket, 4000);
      try {
        socket.send(JSON.stringify({ type: "ping" }));
      } catch {
        this.replace(socket);
      }
    }
  };

  retire() {
    saveSession(this.role, null);
    this.stop();
  }

  forget() {
    this.session = null;
    this.retire();
  }

  stop() {
    this.stopped = true;
    this.clearTimer();
    const socket = this.socket;
    this.socket = null;
    this.registered = false;
    socket?.close();
    window.removeEventListener("online", this.wake);
    document.removeEventListener("visibilitychange", this.wake);
  }
}
