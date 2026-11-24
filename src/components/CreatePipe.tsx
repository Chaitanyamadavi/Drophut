import { useEffect, useRef, useState, type ChangeEvent, type DragEvent } from "react";
import { fingerprint, type FileMetadata } from "./pipeCheckpoint";
import { PipeSignaling, readSession, type Signal, type SignalState } from "./pipeSignaling";

type CreatePipeProps = {
  onHome: () => void;
};

type SendStatus = "selecting" | "creating" | "waiting" | "connecting" | "connected" | "transfer";
type TransferStatus = "idle" | "preparing" | "sending" | "sent" | "error";
type QueueItem = { id: string; file: File; status: "waiting" | "sending" | "completed" | "failed"; bytesSent: number; error?: string };
type PendingFile = FileMetadata & { code: string; digest: string };
const PENDING_FILE_KEY = "drophut-pending-file";
const CANCELLATIONS_KEY = "drophut-cancelled-files";

function readPendingFile(): PendingFile | null {
  try {
    const stored: PendingFile | null = JSON.parse(sessionStorage.getItem(PENDING_FILE_KEY) ?? "null");
    return stored?.code === readSession("sender")?.code ? stored : null;
  } catch {
    return null;
  }
}

function storePendingFile(file: PendingFile | null) {
  try {
    if (file) sessionStorage.setItem(PENDING_FILE_KEY, JSON.stringify(file));
    else sessionStorage.removeItem(PENDING_FILE_KEY);
  } catch {
    // Recovery can still use the File kept in the live tab.
  }
}

const CHUNK_SIZE = 16 * 1024;
const SEND_WINDOW_CHUNKS = 8;
const ACK_TIMEOUT_MS = 20_000;
const ACK_RETRIES = 2;
const BUFFER_LOW_WATER = 256 * 1024;
const BUFFER_HIGH_WATER = 512 * 1024;
// Binary chunk frame: uint16 ID length, uint32 chunk index, UTF-8 transfer ID, then raw file bytes.
const CHUNK_FRAME_HEADER_BYTES = 6;

const ICE_CONFIGURATION: RTCConfiguration = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
};

function formatFileSize(bytes: number) {
  if (bytes < 1024) return bytes + " B";
  const units = ["KB", "MB", "GB", "TB"];
  let size = bytes / 1024;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return size.toFixed(size >= 10 ? 0 : 1) + " " + units[unit];
}

function getTransferClock() { return performance.now(); }

function createLocalId() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    const values = crypto.getRandomValues(new Uint32Array(4));
    return Array.from(values, (value) => value.toString(16)).join("");
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function CreatePipe({ onHome }: CreatePipeProps) {
  const [reselect, setReselect] = useState(readPendingFile);
  const [verifying, setVerifying] = useState(false);
  const activeFileRef = useRef<PendingFile | null>(null);
  const cancellationsRef = useRef(new Set<string>());
  const [queue, setQueue] = useState<QueueItem[]>([]);
  const [textDraft, setTextDraft] = useState("");
  const [queueRunning, setQueueRunning] = useState(false);
  const queueRunningRef = useRef(false);
  const files = queue.map((item) => item.file);
  const [code, setCode] = useState("");
  const [status, setStatus] = useState<SendStatus>(() => readSession("sender") ? "creating" : "selecting");
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);
  const [transferStatus, setTransferStatus] = useState<TransferStatus>("idle");
  const [transferredBytes, setTransferredBytes] = useState(0);
  const [signalingStatus, setSignalingStatus] = useState<SignalState>("connecting");
  const [pipeNotice, setPipeNotice] = useState("");
  const signalingRef = useRef<PipeSignaling | null>(null);
  const peerConnectionRef = useRef<RTCPeerConnection | null>(null);
  const dataChannelRef = useRef<RTCDataChannel | null>(null);
  const receiverReadyRef = useRef(false);
  const receiverReadyWaitRef = useRef<{ channel: RTCDataChannel; resolve: () => void; reject: (reason: Error) => void; timeout: number } | null>(null);
  const queueRef = useRef<QueueItem[]>([]);
  const queueCancelledRef = useRef(false);
  const draggedQueueIdRef = useRef<string | null>(null);
  const transferStatusRef = useRef<TransferStatus>("idle");
  const transferIdRef = useRef<string | null>(null);
  const transferFileSizeRef = useRef<number | null>(null);
  const lastProgressUpdateRef = useRef(0);
  const lastAcknowledgedChunkRef = useRef(0);
  const highestSentChunkRef = useRef(0);
  const peerTransferErrorRef = useRef<string | null>(null);
  const fileEndSentRef = useRef(false);
  const transferCancelledRef = useRef(false);
  const cancelWaitRef = useRef<(() => void) | null>(null);
  const pendingControlRef = useRef<{ type: string; transferId: string; nextChunk?: number; resolve: () => void; reject: (reason: Error) => void } | null>(null);
  const pendingIceRef = useRef<RTCIceCandidateInit[]>([]);
  const connectionTimeoutRef = useRef<number | null>(null);
  const statusRef = useRef<SendStatus>("selecting");
  const expiredRef = useRef(false);
  const sessionCodeRef = useRef<string | null>(null);

  function clearTimers() {
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
  }

  function closePeerConnection() {
    const readyChannel = dataChannelRef.current;
    receiverReadyRef.current = false;
    const readyWait = receiverReadyWaitRef.current;
    if (readyChannel && readyWait?.channel === readyChannel) {
      window.clearTimeout(readyWait.timeout);
      receiverReadyWaitRef.current = null;
      readyWait.reject(new Error("The direct connection closed while waiting for the receiver to be ready."));
    }
    const channel = dataChannelRef.current;
    dataChannelRef.current = null;
    if (channel) {
      console.info("[DropHut sender] Closing DataChannel", channel.readyState);
      channel.onopen = null;
      channel.onmessage = null;
      channel.onclose = null;
      channel.onerror = null;
      channel.close();
      console.info("[DropHut sender] DataChannel state after close", channel.readyState);
    }
    const peer = peerConnectionRef.current;
    peerConnectionRef.current = null;
    if (peer) {
      console.info("[DropHut sender] Closing RTCPeerConnection", peer.connectionState);
      peer.onicecandidate = null;
      peer.oniceconnectionstatechange = null;
      peer.onicegatheringstatechange = null;
      peer.onconnectionstatechange = null;
      peer.close();
      console.info("[DropHut sender] RTCPeerConnection state after close", peer.connectionState);
    }
    pendingIceRef.current = [];
  }

  function recoverPeer() {
    const code = sessionCodeRef.current;
    if (!code) return;
    if (expiredRef.current) {
      fail("The direct connection closed after the pipe expired. Create a new pipe to continue.");
      return;
    }
    closePeerConnection();
    cancelWaitRef.current?.();
    pendingControlRef.current?.reject(new Error("The direct connection was interrupted."));
    pendingControlRef.current = null;
    setPipeNotice("Transfer paused. Reconnecting to the receiver…");
    setStatus("connecting");
    statusRef.current = "connecting";
    signal({ type: "peer-check", code });
  }

  function fail(message: string) {
    storePendingFile(null);
    cancellationsRef.current.clear();
    saveCancellations();
    setReselect(null);
    signalingRef.current?.forget();
    signalingRef.current = null;
    sessionCodeRef.current = null;
    clearTimers();
    closePeerConnection();
    setError(message);
    setPipeNotice("");
    transferCancelledRef.current = true;
    queueCancelledRef.current = true;
    cancelWaitRef.current?.();
    pendingControlRef.current?.reject(new Error(message));
    pendingControlRef.current = null;
    setCode("");
    transferStatusRef.current = "idle";
    setTransferStatus("idle");
    setTransferredBytes(0);
    updateQueue((current) => current.map((item) =>
      item.status === "sending" ? { ...item, status: "waiting", bytesSent: 0 } : item,
    ));
    setStatus("selecting");
    statusRef.current = "selecting";
    setSignalingStatus("connected");
  }

  function signal(message: Signal) {
    signalingRef.current?.send(message);
  }

  function canKeepChannel() {
    const peer = peerConnectionRef.current;
    return dataChannelRef.current?.readyState === "open" && !!peer &&
      peer.connectionState !== "failed" && peer.connectionState !== "closed";
  }

  function channelHealthy() {
    return dataChannelRef.current?.readyState === "open" &&
      peerConnectionRef.current?.connectionState === "connected";
  }

  function startConnectionTimeout() {
    if (connectionTimeoutRef.current !== null) return;
    connectionTimeoutRef.current = window.setTimeout(() => {
      connectionTimeoutRef.current = null;
      if (!channelHealthy()) recoverPeer();
    }, 25000);
  }

  function markDataChannelOpen(channel: RTCDataChannel) {
    if (dataChannelRef.current !== channel) return;
    flushCancellations(channel);
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
    if (!expiredRef.current) setPipeNotice("");
    if (transferStatusRef.current === "sent" || transferStatusRef.current === "error") {
      transferStatusRef.current = "idle";
      setTransferStatus("idle");
      setTransferredBytes(0);
    }
    const next = queueRunningRef.current ? "transfer" : "connected";
    setStatus(next);
    statusRef.current = next;
  }

  function setupDataChannel(channel: RTCDataChannel) {
    receiverReadyRef.current = false;
    const previousReadyWait = receiverReadyWaitRef.current;
    if (previousReadyWait) { window.clearTimeout(previousReadyWait.timeout); receiverReadyWaitRef.current = null; previousReadyWait.reject(new Error("The direct connection changed before the receiver was ready.")); }
    dataChannelRef.current = channel;
    console.info("[DropHut sender] DataChannel created", channel.label, channel.readyState);
    channel.bufferedAmountLowThreshold = BUFFER_LOW_WATER;
    channel.onopen = () => markDataChannelOpen(channel);
    channel.onmessage = (event) => {
      if (dataChannelRef.current !== channel || typeof event.data !== "string") return;
      try {
        const message = JSON.parse(event.data) as { type?: string; transferId?: string; message?: string; nextChunk?: number; size?: number };
        if (message.type === "receiver-ready") {
          if (dataChannelRef.current !== channel || channel.readyState !== "open") return;
          receiverReadyRef.current = true;
          console.info("[DropHut sender] Receiver is ready");
          const pending = receiverReadyWaitRef.current;
          if (pending?.channel === channel) {
            window.clearTimeout(pending.timeout);
            receiverReadyWaitRef.current = null;
            pending.resolve();
          }
          return;
        }
        if (message.type === "file-cancelled" && message.transferId && cancellationsRef.current.delete(message.transferId)) {
          saveCancellations();
          return;
        }
        const transferId = transferIdRef.current;
        if (!transferId || message.transferId !== transferId) return;
        if (message.type === "file-checkpoint") {
          const pending = pendingControlRef.current;
          const next = message.nextChunk;
          if (pending?.type !== "file-checkpoint" || pending.transferId !== transferId ||
              typeof next !== "number" || !Number.isSafeInteger(next) || next < 0 ||
              next > (activeFileRef.current?.totalChunks ?? -1)) return;
          lastAcknowledgedChunkRef.current = next;
          highestSentChunkRef.current = next;
          pendingControlRef.current = null;
          pending.resolve();
          return;
        }
        if (message.type === "chunk-ack") {
          const acknowledged = message.nextChunk;
          if (transferFileSizeRef.current === null || !Number.isSafeInteger(acknowledged) || (acknowledged as number) <= lastAcknowledgedChunkRef.current || (acknowledged as number) > highestSentChunkRef.current) return;
          lastAcknowledgedChunkRef.current = acknowledged as number;
          const pending = pendingControlRef.current;
          if (pending?.type === "chunk-ack" && pending.transferId === transferId && pending.nextChunk !== undefined && (acknowledged as number) >= pending.nextChunk) {
            pendingControlRef.current = null;
            pending.resolve();
          }
          return;
        }
        if (message.type === "file-received") {
          if (!fileEndSentRef.current || transferFileSizeRef.current === null) return;
          if (message.size !== transferFileSizeRef.current) {
            peerTransferErrorRef.current = "The receiver completion acknowledgment did not match the expected file size.";
            transferCancelledRef.current = true;
            pendingControlRef.current?.reject(new Error(peerTransferErrorRef.current));
            pendingControlRef.current = null;
            return;
          }
          if (pendingControlRef.current?.type === "file-received") {
            const pending = pendingControlRef.current;
            pendingControlRef.current = null;
            pending.resolve();
          }
          return;
        }
        if (message.type === "file-error") {
          peerTransferErrorRef.current = message.message || "The receiver could not accept this file.";
          transferCancelledRef.current = true;
          cancelWaitRef.current?.();
          const reason = new Error(peerTransferErrorRef.current);
          pendingControlRef.current?.reject(reason);
          pendingControlRef.current = null;
          return;
        }
        if (message.type === "file-cancelled") {
          cancelWaitRef.current?.();
          peerTransferErrorRef.current = "The receiver cancelled this file.";
          transferCancelledRef.current = true;
          cancelWaitRef.current?.();
          pendingControlRef.current?.reject(new Error(peerTransferErrorRef.current));
          pendingControlRef.current = null;
          return;
        }
      } catch { console.warn("[DropHut sender] Ignored invalid DataChannel response"); }
    };
    channel.onclose = () => { if (dataChannelRef.current === channel) recoverPeer(); };
    channel.onerror = () => { if (dataChannelRef.current === channel) recoverPeer(); };
    if (channel.readyState === "open") markDataChannelOpen(channel);
  }

  async function flushPendingIce(peer: RTCPeerConnection) {
    const candidates = pendingIceRef.current.splice(0);
    for (const candidate of candidates) {
      await peer.addIceCandidate(candidate);
    console.info("[DropHut sender] Applied remote ICE candidate");
    }
  }

  async function addIceCandidate(candidate: RTCIceCandidateInit) {
    const peer = peerConnectionRef.current;
    if (!peer || !peer.remoteDescription) {
      pendingIceRef.current.push(candidate);
      return;
    }
    await peer.addIceCandidate(candidate);
  }

  async function startOffer(pipeCode: string) {
    if (peerConnectionRef.current) return;
    console.info("[DropHut sender] Creating RTCPeerConnection");
    const peer = new RTCPeerConnection(ICE_CONFIGURATION);
    peerConnectionRef.current = peer;
    console.info("[DropHut sender] RTCPeerConnection created");

    // Create the DataChannel before creating the offer so it is included in the SDP.
    const channel = peer.createDataChannel("data", { ordered: true });
    setupDataChannel(channel);

    peer.onicegatheringstatechange = () => {
      console.info("[DropHut sender] ICE gathering state:", peer.iceGatheringState);
    };
    console.info("[DropHut sender] RTCPeerConnection created", { iceGatheringState: peer.iceGatheringState, iceConnectionState: peer.iceConnectionState, connectionState: peer.connectionState });
    peer.onicecandidate = (event) => {
      if (event.candidate) {
        console.info("[DropHut sender] Generated ICE candidate", event.candidate.toJSON());
        signal({ type: "webrtc-ice", code: pipeCode, candidate: event.candidate.toJSON() });
      } else {
        console.info("[DropHut sender] ICE candidate gathering complete");
      }
    };
    peer.oniceconnectionstatechange = () => {
      console.info("[DropHut sender] ICE connection state:", peer.iceConnectionState);
      if (peer.iceConnectionState === "failed" && peerConnectionRef.current === peer) recoverPeer();
      if (peer.iceConnectionState === "disconnected") {
        if (statusRef.current === "connected") { setStatus("connecting"); statusRef.current = "connecting"; }
        startConnectionTimeout();
      }
    };
    peer.onconnectionstatechange = () => {
      console.info("[DropHut sender] Peer connection state:", peer.connectionState);
      if (peer.connectionState === "failed" && peerConnectionRef.current === peer) recoverPeer();
      if (peer.connectionState === "closed" && peerConnectionRef.current === peer) recoverPeer();
      if (peer.connectionState === "disconnected") {
        if (statusRef.current === "connected") { setStatus("connecting"); statusRef.current = "connecting"; }
        startConnectionTimeout();
      }
      if (peer.connectionState === "connected" && dataChannelRef.current?.readyState === "open") {
        markDataChannelOpen(dataChannelRef.current);
      }
    };

    try {
      console.info("[DropHut sender] Creating SDP offer");
      const offer = await peer.createOffer();
      console.debug("[DropHut sender] SDP offer created", offer);
      await peer.setLocalDescription(offer);
      const local = peer.localDescription;
      console.debug("[DropHut sender] Local description set", local);
      if (peerConnectionRef.current !== peer || !local) return;
      signal({ type: "webrtc-offer", code: pipeCode, offer: { type: local.type, sdp: local.sdp } });
      console.info("[DropHut sender] Sent SDP offer through signaling WebSocket");
    } catch (reason) {
      if (peerConnectionRef.current !== peer) return;
      fail(reason instanceof Error ? reason.message : "Could not create a WebRTC offer.");
    }
  }

  function updateQueue(update: (current: QueueItem[]) => QueueItem[]) {
    const next = update(queueRef.current);
    queueRef.current = next;
    setQueue(next);
  }

  function selectFiles(fileList: FileList | null) {
    if (!fileList) return;
    const additions = Array.from(fileList);
    updateQueue((current) => {
      const next = [...current];
      for (const file of additions) {
        const duplicate = next.some((item) => item.file.name === file.name && item.file.size === file.size && item.file.lastModified === file.lastModified);
        if (duplicate) continue;
        const id = createLocalId();
        next.push({ id, file, status: "waiting", bytesSent: 0 });
      }
      return next;
    });
    setError("");
  }

  function handleFileChange(event: ChangeEvent<HTMLInputElement>) {
    selectFiles(event.target.files);
    event.target.value = "";
  }

  function handleDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    selectFiles(event.dataTransfer.files);
  }

  function handleSignal(message: Signal) {
    const pipeCode = sessionCodeRef.current;
    if (!pipeCode) return;
    if (message.type === "pipe-expired") {
      if (canKeepChannel()) {
        expiredRef.current = true;
        signalingRef.current?.retire();
        setSignalingStatus("connected");
        storePendingFile(null);
        saveCancellations();
        setPipeNotice("The pipe’s reconnect window expired. The current direct connection is still active.");
      } else {
        fail("This pipe expired. Create a new pipe to continue.");
      }
    } else if (message.type === "pipe-closed") {
      fail("The other device closed the pipe.");
    } else if (message.type === "error") {
      fail(message.message || "The signaling server reported an error.");
    } else if (message.type === "peer-away") {
      setPipeNotice("The receiver’s signaling connection dropped. Their place is reserved while they reconnect.");
    } else if (message.type === "pipe-created" || message.type === "pipe-resumed") {
      if (statusRef.current === "creating") {
        setCode(pipeCode);
        setStatus("waiting");
        statusRef.current = "waiting";
      }
    } else if (message.type === "peer-ready") {
      if (!message.reset && canKeepChannel()) {
        setPipeNotice("");
        return;
      }
      closePeerConnection();
      pendingControlRef.current?.reject(new Error("The direct connection is being replaced."));
      pendingControlRef.current = null;
      cancelWaitRef.current?.();
      if (queueRunningRef.current) setPipeNotice("Transfer paused. Reconnecting to the receiver…");
      setStatus("connecting");
      statusRef.current = "connecting";
      startConnectionTimeout();
      void startOffer(pipeCode);
    } else if (message.type === "webrtc-answer" && message.answer) {
      const peer = peerConnectionRef.current;
      if (!peer || peer.remoteDescription || peer.signalingState !== "have-local-offer") return;
      void peer.setRemoteDescription(message.answer).then(() => {
        if (peerConnectionRef.current === peer) return flushPendingIce(peer);
      }).catch(() => { /* A replacement offer may already be in flight. */ });
    } else if (message.type === "webrtc-ice" && message.candidate) {
      void addIceCandidate(message.candidate).catch(() => { /* Ignore ICE for a replaced negotiation. */ });
    }
  }

  function checkPeer() {
    const peer = peerConnectionRef.current;
    if (!peer) return;
    if (peer.connectionState === "failed" || peer.connectionState === "closed" ||
        dataChannelRef.current?.readyState === "closed") recoverPeer();
    else if (peer.connectionState === "disconnected") startConnectionTimeout();
  }

  function startSignaling() {
    signalingRef.current?.stop();
    expiredRef.current = false;
    const connection = new PipeSignaling("sender", setSignalingStatus, handleSignal, () => !canKeepChannel(), checkPeer);
    signalingRef.current = connection;
    return connection;
  }

  useEffect(() => {
    const connection = startSignaling();
    if (connection.session) {
      sessionCodeRef.current = connection.session.code;
      statusRef.current = "creating";

    }
    connection.open();
    return () => {
      signalingRef.current?.stop();
      sessionCodeRef.current = null;
      clearTimers();
      closePeerConnection();
      queueCancelledRef.current = true;
      transferCancelledRef.current = true;
      cancelWaitRef.current?.();
      pendingControlRef.current?.reject(new Error("Transfer paused."));
      pendingControlRef.current = null;
    };
    // Socket callbacks use refs to read current connection and transfer state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function createPipe() {
    const connection = signalingRef.current ?? startSignaling();
    if (!connection || connection.session) return;
    const random = crypto.getRandomValues(new Uint32Array(1))[0];
    const newCode = String(100000 + (random % 900000));
    setError("");
    setStatus("creating");
    statusRef.current = "creating";
    sessionCodeRef.current = newCode;
    connection.register(newCode);
  }

  async function copyCode() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1800);
    } catch {
      setError("Could not copy the code. You can select and copy it instead.");
    }
  }

  async function waitForBufferDrain(channel: RTCDataChannel) {
    if (channel.bufferedAmount <= BUFFER_HIGH_WATER) return;
    await new Promise<void>((resolve, reject) => {
      let lastAmount = channel.bufferedAmount;
      let stallTimeout: number;
      let pollTimeout: number;
      const cleanup = () => {
        channel.removeEventListener("bufferedamountlow", onLow);
        channel.removeEventListener("close", onClose);
        channel.removeEventListener("error", onError);
        window.clearTimeout(stallTimeout);
        window.clearTimeout(pollTimeout);
        if (cancelWaitRef.current === onCancel) cancelWaitRef.current = null;
      };
      const finish = (reason?: Error) => { cleanup(); if (reason) reject(reason); else resolve(); };
      const onLow = () => finish();
      const onClose = () => finish(new Error("The direct connection closed while sending."));
      const onError = () => finish(new Error("The data channel failed while sending."));
      const onCancel = () => finish(new Error("Transfer cancelled."));
      const resetStallTimeout = () => {
        window.clearTimeout(stallTimeout);
        stallTimeout = window.setTimeout(() => finish(new Error("The DataChannel buffer stopped draining.")), 5 * 60 * 1000);
      };
      const pollBuffer = () => {
        const amount = channel.bufferedAmount;
        if (amount <= BUFFER_LOW_WATER) { finish(); return; }
        if (amount < lastAmount) { lastAmount = amount; resetStallTimeout(); }
        pollTimeout = window.setTimeout(pollBuffer, 250);
      };
      cancelWaitRef.current = onCancel;
      channel.addEventListener("bufferedamountlow", onLow);
      channel.addEventListener("close", onClose);
      channel.addEventListener("error", onError);
      resetStallTimeout();
      pollTimeout = window.setTimeout(pollBuffer, 250);
      if (channel.bufferedAmount <= BUFFER_LOW_WATER) finish();
    });
  }

  function waitForReceiverReady(channel: RTCDataChannel): Promise<void> {
    if (receiverReadyRef.current) return Promise.resolve();
    if (dataChannelRef.current !== channel || channel.readyState !== "open") return Promise.reject(new Error("The direct connection closed before the receiver was ready."));
    console.info("[DropHut sender] Waiting for receiver-ready");
    return new Promise<void>((resolve, reject) => {
      const timeout = window.setTimeout(() => {
        if (receiverReadyWaitRef.current?.channel === channel) receiverReadyWaitRef.current = null;
        reject(new Error("Timed out waiting for the receiver to become ready. Please try again."));
      }, 30_000);
      receiverReadyWaitRef.current = { channel, resolve: () => { window.clearTimeout(timeout); receiverReadyWaitRef.current = null; resolve(); }, reject: (reason) => { window.clearTimeout(timeout); receiverReadyWaitRef.current = null; reject(reason); }, timeout };
      if (receiverReadyRef.current && receiverReadyWaitRef.current?.channel === channel) {
        window.clearTimeout(timeout);
        receiverReadyWaitRef.current = null;
        resolve();
      }
    });
  }
  function waitForTransferControl(type: string, transferId: string, nextChunk?: number, timeoutMs = ACK_TIMEOUT_MS) {
    return new Promise<void>((resolve, reject) => {
      const finish = (callback: () => void) => { window.clearTimeout(timeout); callback(); };
      const pending = { type, transferId, nextChunk, resolve: () => finish(resolve), reject: (reason: Error) => finish(() => reject(reason)) };
      const timeout = window.setTimeout(() => {
        if (pendingControlRef.current === pending) pendingControlRef.current = null;
        reject(new Error(type === "chunk-ack" ? "Timed out waiting for chunk ACK at " + nextChunk + "." : "Timed out waiting for the receiver to confirm the completed file."));
      }, timeoutMs);
      pendingControlRef.current = pending;
    });
  }

  async function waitForChunkAck(channel: RTCDataChannel, transferId: string, nextChunk: number) {
    if (lastAcknowledgedChunkRef.current >= nextChunk) return;
    for (let attempt = 0; attempt <= ACK_RETRIES; attempt += 1) {
      try { await waitForTransferControl("chunk-ack", transferId, nextChunk); return; }
      catch (reason) {
        if (transferCancelledRef.current || peerTransferErrorRef.current || attempt === ACK_RETRIES || channel.readyState !== "open") throw reason;
        console.warn("[DropHut sender] Chunk ACK timed out; requesting cumulative ACK", { transferId, expectedNextChunk: nextChunk, retry: attempt + 1, bufferedAmount: channel.bufferedAmount });
        channel.send(JSON.stringify({ type: "chunk-sync", transferId, expectedNextChunk: nextChunk }));
      }
    }
  }
  function cancelLocalTransfer() {
    if (transferCancelledRef.current && transferStatusRef.current === "idle") return;
    console.info("[DropHut sender] Transfer cancelled", { transferId: transferIdRef.current });
    transferCancelledRef.current = true;
    cancelWaitRef.current?.();
    pendingControlRef.current?.reject(new Error("Transfer cancelled."));
    pendingControlRef.current = null;
    transferIdRef.current = null; transferFileSizeRef.current = null; fileEndSentRef.current = false; transferStatusRef.current = "idle";
    setTransferStatus("idle"); setTransferredBytes(0); setError("");
    if (dataChannelRef.current?.readyState === "open") { setStatus("connected"); statusRef.current = "connected"; }
  }

  function saveCancellations() {
    try {
      const ids = Array.from(cancellationsRef.current);
      if (ids.length && !expiredRef.current) {
        sessionStorage.setItem(CANCELLATIONS_KEY, JSON.stringify({ code: sessionCodeRef.current, ids }));
      } else {
        sessionStorage.removeItem(CANCELLATIONS_KEY);
      }
    } catch {
      // The live tab can still replay cancellations.
    }
  }

  function flushCancellations(channel: RTCDataChannel) {
    if (channel.readyState !== "open") return;
    for (const transferId of cancellationsRef.current) {
      try {
        channel.send(JSON.stringify({ type: "file-cancel", transferId }));
      } catch {
        return;
      }
    }
  }

  function cancelTransfer() {
    const transferId = activeFileRef.current?.transferId ?? reselect?.transferId;
    const channel = dataChannelRef.current;
    queueCancelledRef.current = true;
    storePendingFile(null);
    setReselect(null);
    if (transferId) {
      cancellationsRef.current.add(transferId);
      saveCancellations();
      if (channel) flushCancellations(channel);
    }
    updateQueue((current) => current.map((item) => item.status === "sending" ? { ...item, status: "waiting", bytesSent: 0, error: undefined } : item));
    cancelLocalTransfer();
  }

  async function waitForChannel(code: string) {
    while (sessionCodeRef.current === code && !queueCancelledRef.current && !transferCancelledRef.current) {
      const channel = dataChannelRef.current;
      if (channel?.readyState === "open") return channel;
      await new Promise<void>((resolve) => window.setTimeout(resolve, 250));
    }
    throw new Error("Transfer stopped while waiting for the direct connection.");
  }

  async function sendFile(file: File, queueId: string, resumed?: PendingFile) {
    const code = sessionCodeRef.current;
    if (!code) throw new Error("The pipe session ended.");
    transferCancelledRef.current = false;
    peerTransferErrorRef.current = null;
    transferStatusRef.current = "preparing";
    setTransferStatus("preparing");
    setTransferredBytes(0);

    const metadata: PendingFile = resumed ?? {
      code,
      transferId: createLocalId(),
      name: file.name,
      mimeType: file.type || "application/octet-stream",
      size: file.size,
      totalChunks: Math.ceil(file.size / CHUNK_SIZE),
      chunkSize: CHUNK_SIZE,
      digest: await fingerprint(file, () => queueCancelledRef.current || sessionCodeRef.current !== code),
    };
    if (queueCancelledRef.current || sessionCodeRef.current !== code) throw new Error("Transfer stopped.");
    activeFileRef.current = metadata;
    if (!expiredRef.current) storePendingFile(metadata);
    const { transferId, totalChunks, size, mimeType, name } = metadata;
    transferIdRef.current = transferId;
    transferFileSizeRef.current = size;
    const transferIdBytes = new TextEncoder().encode(transferId);
    const chunkHeaderLength = CHUNK_FRAME_HEADER_BYTES + transferIdBytes.length;

    try {
      for (;;) {
        const channel = await waitForChannel(code);
        try {
          await waitForReceiverReady(channel);
          if (transferCancelledRef.current || queueCancelledRef.current) throw new Error("Transfer cancelled.");
          const peerLimit = peerConnectionRef.current?.sctp?.maxMessageSize;
          if (peerLimit && CHUNK_SIZE + chunkHeaderLength > peerLimit) throw new Error("The direct connection cannot carry the selected chunk size.");
          fileEndSentRef.current = false;
          lastAcknowledgedChunkRef.current = 0;
          highestSentChunkRef.current = 0;
          channel.send(JSON.stringify({ type: "file-start", transferId, name, mimeType, size, totalChunks, chunkSize: CHUNK_SIZE }));
          await waitForTransferControl("file-checkpoint", transferId);
          const savedChunk = lastAcknowledgedChunkRef.current;
          if (!expiredRef.current) setPipeNotice(savedChunk ? "Resuming from the receiver’s saved progress." : "");
          setTransferredBytes(Math.min(size, savedChunk * CHUNK_SIZE));
          transferStatusRef.current = "sending";
          setTransferStatus("sending");

          for (let start = savedChunk; start < totalChunks; start += SEND_WINDOW_CHUNKS) {
            const end = Math.min(totalChunks, start + SEND_WINDOW_CHUNKS);
            for (let index = start; index < end; index += 1) {
              if (channel.bufferedAmount > BUFFER_HIGH_WATER) await waitForBufferDrain(channel);
              if (transferCancelledRef.current || queueCancelledRef.current) throw new Error("Transfer cancelled.");
              if (dataChannelRef.current !== channel || channel.readyState !== "open") throw new Error("The direct connection closed.");
              const offset = index * CHUNK_SIZE;
              const header = new Uint8Array(chunkHeaderLength);
              const view = new DataView(header.buffer);
              view.setUint16(0, transferIdBytes.length);
              view.setUint32(2, index);
              header.set(transferIdBytes, CHUNK_FRAME_HEADER_BYTES);
              channel.send(new Blob([header, file.slice(offset, Math.min(offset + CHUNK_SIZE, size))]));
              highestSentChunkRef.current = index + 1;
            }
            await waitForChunkAck(channel, transferId, end);
            const now = getTransferClock();
            if (end === totalChunks || now - lastProgressUpdateRef.current >= 100) {
              const bytesSent = Math.min(size, end * CHUNK_SIZE);
              setTransferredBytes(bytesSent);
              updateQueue((current) => current.map((item) => item.id === queueId ? { ...item, bytesSent } : item));
              lastProgressUpdateRef.current = now;
            }
          }

          for (let attempt = 0; ; attempt += 1) {
            if (transferCancelledRef.current || queueCancelledRef.current) throw new Error("Transfer cancelled.");
            channel.send(JSON.stringify({ type: "file-end", transferId, totalChunks, size }));
            fileEndSentRef.current = true;
            try {
              await waitForTransferControl("file-received", transferId);
              break;
            } catch (reason) {
              if (attempt === ACK_RETRIES || transferCancelledRef.current || channel.readyState !== "open") throw reason;
            }
          }
          if (!expiredRef.current) setPipeNotice("");
          transferStatusRef.current = "sent";
          setTransferStatus("sent");
          break;
        } catch (reason) {
          if (peerTransferErrorRef.current) throw new Error(peerTransferErrorRef.current, { cause: reason });
          if (queueCancelledRef.current || transferCancelledRef.current || sessionCodeRef.current !== code ||
              (dataChannelRef.current === channel && channel.readyState === "open")) throw reason;
          setPipeNotice("Transfer paused. Waiting for the direct connection…");
        }
      }
    } catch (reason) {
      const channel = dataChannelRef.current;
      if (!queueCancelledRef.current && !peerTransferErrorRef.current && channel?.readyState === "open") {
        try { channel.send(JSON.stringify({ type: "file-cancel", transferId })); } catch { /* Expiry will clear the checkpoint. */ }
      }
      throw reason;
    } finally {
      activeFileRef.current = null;
      pendingControlRef.current = null;
      transferIdRef.current = null;
      transferFileSizeRef.current = null;
      fileEndSentRef.current = false;
      if (sessionCodeRef.current === code) storePendingFile(null);
    }
  }

  async function reselectFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = "";
    const pending = reselect;
    if (!file || !pending || verifying) return;
    const code = sessionCodeRef.current;
    setVerifying(true);
    setError("");
    try {
      if (file.name !== pending.name || file.size !== pending.size ||
          await fingerprint(file, () => sessionCodeRef.current !== code) !== pending.digest) {
        throw new Error("Choose the original file. The selected file does not match.");
      }
      if (sessionCodeRef.current !== code) return;
      const item: QueueItem = { id: createLocalId(), file, status: "waiting", bytesSent: 0 };
      updateQueue((current) => [item, ...current]);
      setReselect(null);
      await sendQueue({ id: item.id, file: pending });
    } catch (reason) {
      if (sessionCodeRef.current === code) setError(reason instanceof Error ? reason.message : "Could not verify the selected file.");
    } finally {
      setVerifying(false);
    }
  }

  async function sendQueue(resumed?: { id: string; file: PendingFile }) {
    const channel = dataChannelRef.current;
    if (queueRunningRef.current || !queueRef.current.some((item) => item.status === "waiting") || (!resumed && channel?.readyState !== "open")) return;
    queueRunningRef.current = true;
    queueCancelledRef.current = false;
    setQueueRunning(true); setError("");
    if (channel?.readyState === "open") { setStatus("transfer"); statusRef.current = "transfer"; }
    const pending = queueRef.current.filter((item) => item.status === "waiting");
    for (const item of pending) {
      if (queueCancelledRef.current) break;
      updateQueue((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "sending", bytesSent: 0, error: undefined } : entry));
      try {
        await sendFile(item.file, item.id, resumed?.id === item.id ? resumed.file : undefined);
        updateQueue((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "completed", bytesSent: entry.file.size, error: undefined } : entry));
      } catch (reason) {
        const message = reason instanceof Error ? reason.message : "Could not send this file.";
        console.error("[DropHut sender] Queue item failed", { name: item.file.name, message });
        if (!queueCancelledRef.current) {
          updateQueue((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "failed", error: message } : entry));
          setError(`${item.file.name}: ${message}`);
        }
        if (queueCancelledRef.current || dataChannelRef.current?.readyState !== "open") break;
      }
    }
    queueRunningRef.current = false;
    setQueueRunning(false);
    const allComplete = queueRef.current.length > 0 && queueRef.current.every((item) => item.status === "completed");
    if (allComplete) { transferStatusRef.current = "sent"; setTransferStatus("sent"); setError(""); }
    else if (!queueCancelledRef.current) { transferStatusRef.current = queueRef.current.some((item) => item.status === "failed") ? "error" : "idle"; setTransferStatus(transferStatusRef.current); }
  }

  function retryFailedFiles() {
    updateQueue((current) => current.map((item) => item.status === "failed" ? { ...item, status: "waiting", bytesSent: 0, error: undefined } : item));
    setError("");
    transferStatusRef.current = "idle"; setTransferStatus("idle");
  }

  async function sendTextMessage() {
    const channel = dataChannelRef.current;
    const text = textDraft.trim();
    if (!text || !channel || channel.readyState !== "open" || queueRunning) return;
    const byteLength = new TextEncoder().encode(text).byteLength;
    if (byteLength > 48 * 1024) { setError("Text messages must be 48 KB or smaller."); return; }
    const message = JSON.stringify({ type: "text-message", id: createLocalId(), text });
    const messageBytes = new TextEncoder().encode(message).byteLength;
    const maxMessageSize = peerConnectionRef.current?.sctp?.maxMessageSize || 64 * 1024;
    if (messageBytes > Math.min(48 * 1024, maxMessageSize)) { setError("This text message is too large to send in one DataChannel message."); return; }
    try {
      if (channel.bufferedAmount > BUFFER_HIGH_WATER) await waitForBufferDrain(channel);
      if (dataChannelRef.current !== channel || channel.readyState !== "open") throw new Error("The direct connection is not open.");
      channel.send(message);
      setTextDraft(""); setError("");
      console.info("[DropHut sender] Text message sent", { bytes: byteLength });
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Could not send the text message."); }
  }

  function removeQueuedFile(id: string) {
    updateQueue((current) => current.filter((item) => item.id !== id || item.status !== "waiting"));
  }

  function reorderQueuedFile(targetId: string) {
    const sourceId = draggedQueueIdRef.current; draggedQueueIdRef.current = null;
    if (!sourceId || sourceId === targetId) return;
    updateQueue((current) => {
      const from = current.findIndex((item) => item.id === sourceId && item.status === "waiting");
      const to = current.findIndex((item) => item.id === targetId && item.status === "waiting");
      if (from < 0 || to < 0) return current;
      const next = [...current]; const [moved] = next.splice(from, 1); next.splice(to, 0, moved); return next;
    });
  }

  function handleQueueFileChange(event: ChangeEvent<HTMLInputElement>) {
    selectFiles(event.target.files); event.target.value = "";
  }

  const totalQueueBytes = queue.reduce((total, item) => total + item.file.size, 0);
  const completedQueueBytes = queue.reduce((total, item) => total + (item.status === "completed" ? item.file.size : item.status === "sending" ? transferredBytes : 0), 0);
  const overallQueuePercent = totalQueueBytes > 0 ? Math.min(100, Math.floor(completedQueueBytes / totalQueueBytes * 100)) : 0;

  function leavePipe() {
    const connection = signalingRef.current;
    if (connection?.session) connection.send({ type: "leave-pipe", code: connection.session.code });
    connection?.forget();
    storePendingFile(null);

    onHome();
  }

  return (
    <main className="app-shell flow-shell">
      <header className="topbar">
        <button className="brand-button" onClick={leavePipe}><span className="brand-mark small" aria-hidden="true">D</span>Drophut</button>
        <button className="text-button" onClick={leavePipe}>← Home</button>
      </header>

      <section className="flow-content" aria-labelledby="send-title">
        <p className="eyebrow">SEND FILES</p>
        <h1 id="send-title">{status === "selecting" || status === "creating" ? "Choose files to send" : status === "waiting" ? "Your Pipe" : status === "connected" ? "Ready to send" : status === "transfer" ? "Transfer" : "Connecting to receiver"}</h1>

        {(status === "selecting" || status === "creating") && (
          <div className="send-file-selection">
            <div className="upload-card" onDragOver={(event) => event.preventDefault()} onDrop={handleDrop}>
              <div className="upload-card-icon" aria-hidden="true">
                <svg className="folder-art" viewBox="0 0 72 64" fill="none">
                  <path d="M7 17.5A5.5 5.5 0 0 1 12.5 12h15l6 7H59a5 5 0 0 1 5 5v25a5 5 0 0 1-5 5H13a6 6 0 0 1-6-6V17.5Z" fill="currentColor" fill-opacity=".12" stroke="currentColor" stroke-width="2.5" stroke-linejoin="round"/>
                  <path d="M8 27h55" stroke="currentColor" stroke-opacity=".55" stroke-width="2"/>
                </svg>
                <span className="upload-arrow-badge"><svg viewBox="0 0 20 20" fill="none"><path d="M10 14V4m0 0L6 8m4-4 4 4M4 15v1.5A1.5 1.5 0 0 0 5.5 18h9a1.5 1.5 0 0 0 1.5-1.5V15" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg></span>
              </div>
              <div className="upload-card-copy">
                <span className="upload-eyebrow">SEND FILES</span>
                <strong>Drop your files here</strong>
                <span className="upload-support">or choose files from your device</span>
              </div>
              <div className="upload-card-action">
                <label htmlFor="file-picker" className="upload-browse-button">Browse files <span aria-hidden="true">↗</span></label>
                <span className="upload-limit-note">Any file type <i aria-hidden="true">·</i> Multiple files</span>
              </div>
              <input id="file-picker" className="visually-hidden" type="file" multiple onChange={handleFileChange} />
            </div>
            {queue.length > 0 ? (
              <div className="file-panel transfer-queue-panel">
                <div className="section-heading"><h2>Transfer Queue</h2><span>{queue.length} {queue.length === 1 ? "file" : "files"}</span></div>
                <ol className="transfer-queue-list">
                  {queue.map((item, index) => (
                    <li className="transfer-queue-item" key={item.id} draggable={item.status === "waiting"} onDragStart={(event) => { draggedQueueIdRef.current = item.id; event.dataTransfer.effectAllowed = "move"; }} onDragOver={(event) => { if (item.status === "waiting") event.preventDefault(); }} onDrop={(event) => { event.preventDefault(); reorderQueuedFile(item.id); }} onDragEnd={() => { draggedQueueIdRef.current = null; }}>
                      <span className="queue-position">{index + 1}</span>
                      <span className="queue-file-icon" aria-hidden="true">{item.file.type.startsWith("image/") ? "▧" : item.file.type.startsWith("video/") ? "▶" : "↗"}</span>
                      <span className="queue-file-info"><strong>{item.file.name}</strong><small>{formatFileSize(item.file.size)} · {item.file.type || "File"}</small></span>
                      <span className={`queue-file-status ${item.status}`} title={item.error}>{item.status === "waiting" ? "Waiting" : item.status === "sending" ? "Sending" : item.status === "completed" ? "Completed" : "Failed"}</span>
                      <span className="queue-grip" title={item.status === "waiting" ? "Drag to reorder" : undefined} aria-hidden="true">{item.status === "waiting" ? "⠿" : item.status === "completed" ? "✓" : item.status === "failed" ? "!" : "•••"}</span>{item.status === "waiting" ? <button className="queue-remove" type="button" aria-label={`Remove ${item.file.name}`} onClick={() => removeQueuedFile(item.id)}>×</button> : null}
                    </li>
                  ))}
                </ol>
                <button className="button button-primary button-full" onClick={createPipe} disabled={status === "creating"}>
                  {status === "creating" ? "Creating your pipe…" : "Create Pipe"}
                </button>
              </div>
            ) : (
              <div className="file-panel empty-queue-prompt">
                <p className="pipe-hint">You can add files after the receiver connects.</p>
                <button className="button button-primary button-full" onClick={createPipe} disabled={status === "creating"}>{status === "creating" ? "Creating your pipe…" : "Create Pipe"}</button>
              </div>
            )}
          </div>
        )}

        {(status === "waiting" || status === "connecting") && (
          <div className="pipe-card">
            <p className="muted-label">YOUR PIPE CODE</p>
            <div className="pipe-code">{code}</div>
            <p className="pipe-hint">Share this code with the receiver.</p>
            <button className="button button-secondary copy-button" onClick={copyCode}>{copied ? "Copied ✓" : "Copy Code"}</button>
            <div className="waiting-line"><span className={'status-dot' + (status === "waiting" ? " pulse" : "")} />{status === "waiting" ? "Waiting for receiver…" : "Connecting directly…"}</div>
            {queueRunning && <button className="button button-secondary button-full" onClick={cancelTransfer}>Cancel current file</button>}
            <p className="file-count">{files.length} {files.length === 1 ? "file" : "files"} ready</p>
          </div>
        )}

        {(status === "connected" || status === "transfer") && (
          <div className="pipe-card connected-card transfer-workspace">
            <div className="connected-label"><span className="status-dot" />Connected ✓</div>
            <div className="section-heading queue-section-heading"><h2>Transfer Queue</h2><span>{queue.filter((item) => item.status === "completed").length} / {queue.length} completed</span></div>
            <ol className="transfer-queue-list active-queue-list">
              {queue.map((item, index) => (
                <li className={`transfer-queue-item ${item.status === "sending" ? "is-active" : ""}`} key={item.id} draggable={item.status === "waiting"} onDragStart={(event) => { draggedQueueIdRef.current = item.id; event.dataTransfer.effectAllowed = "move"; }} onDragOver={(event) => { if (item.status === "waiting") event.preventDefault(); }} onDrop={(event) => { event.preventDefault(); reorderQueuedFile(item.id); }} onDragEnd={() => { draggedQueueIdRef.current = null; }}>
                  <span className="queue-position">{index + 1}</span>
                  <span className="queue-file-icon" aria-hidden="true">{item.file.type.startsWith("image/") ? "▧" : item.file.type.startsWith("video/") ? "▶" : "↗"}</span>
                  <span className="queue-file-info"><strong>{item.file.name}</strong><small>{formatFileSize(item.file.size)} · {item.status === "sending" ? `${formatFileSize(item.bytesSent)} sent` : item.file.type || "File"}</small>{item.status === "failed" && item.error && <small className="queue-file-error">{item.error}</small>}{item.status === "sending" && <span className="queue-progress-track"><i style={{ width: `${item.file.size ? Math.min(100, item.bytesSent / item.file.size * 100) : 100}%` }} /></span>}</span>
                  <span className={`queue-file-status ${item.status}`}>{item.status === "waiting" ? "Waiting" : item.status === "sending" ? "Sending" : item.status === "completed" ? "Completed" : "Failed"}</span>
                  <span className="queue-grip" title={item.status === "waiting" ? "Drag to reorder" : undefined} aria-hidden="true">{item.status === "waiting" ? "⠿" : item.status === "completed" ? "✓" : item.status === "failed" ? "!" : "•••"}</span>{item.status === "waiting" ? <button className="queue-remove" type="button" aria-label={`Remove ${item.file.name}`} onClick={() => removeQueuedFile(item.id)}>×</button> : null}
                </li>
              ))}
            </ol>
            <div className="queue-overall-progress">
              <span>Overall progress</span>
              <strong>{formatFileSize(completedQueueBytes)} / {formatFileSize(totalQueueBytes)} · {overallQueuePercent}%</strong>
            </div>
            <div className="queue-progress-track overall"><i style={{ width: `${overallQueuePercent}%` }} /></div>
            <div className="queue-actions">
              <label className="button button-secondary queue-add-button" htmlFor="queue-file-picker">+ Add files</label>
              <input id="queue-file-picker" className="visually-hidden" type="file" multiple onChange={handleQueueFileChange} />
              <button className="button button-primary queue-send-button" onClick={() => { void sendQueue(); }} disabled={!!reselect || queueRunning || !queue.some((item) => item.status === "waiting")}>{queueRunning ? "Sending queue…" : "Send queue"}</button>
            </div>
            {queueRunning && <button className="button button-secondary button-full" onClick={cancelTransfer}>Cancel current file</button>}
            {queue.some((item) => item.status === "failed") && !queueRunning && <button className="button button-secondary button-full" onClick={retryFailedFiles}>Retry failed files</button>}
            {transferStatus === "sent" && queue.every((item) => item.status === "completed") && <p className="queue-success">All queued files were received successfully ✓</p>}
            <div className="text-share-box">
              <label htmlFor="text-share-input">Send a text message <span>Up to 48 KB</span></label>
              <textarea id="text-share-input" value={textDraft} onChange={(event) => setTextDraft(event.target.value)} maxLength={48 * 1024} placeholder="Write a message to the receiver…" disabled={queueRunning} />
              <button className="button button-secondary button-full" onClick={() => { void sendTextMessage(); }} disabled={queueRunning || !textDraft.trim()}>Send text</button>
            </div>
          </div>
        )}

        {reselect && (
          <div className="pipe-card">
            <p>Reselect {reselect.name} to resume the interrupted transfer.</p>
            <input aria-label="Reselect the original file" type="file" disabled={verifying} onChange={(event) => { void reselectFile(event); }} />
            {verifying && <p role="status">Verifying the selected file…</p>}
            <button className="button button-secondary" disabled={verifying} onClick={cancelTransfer}>Cancel interrupted file</button>
          </div>
        )}
        {queueRunning && transferStatus === "preparing" && <p role="status">Preparing file for transfer…</p>}
        {pipeNotice && <p className="pipe-hint" role="status">{pipeNotice}</p>}
        {error && <p className="error-message" role="alert">{error}</p>}
        {signalingStatus === "retrying" && (
          <p className="signaling-note" role="status">
            {status === "connected" || status === "transfer" ? "Signaling reconnecting; the direct connection is still active." : "Signaling reconnecting; keeping this pipe session available."}
          </p>
        )}
        {signalingStatus === "connecting" && <p role="status">Connecting to signaling…</p>}
        {signalingStatus === "failed" && (
          <div role="status">
            <p>Could not connect to signaling.</p>
            <button className="button button-secondary" onClick={() => {
              if (signalingRef.current?.session) signalingRef.current.retry();
              else startSignaling().open();
            }}>Retry</button>
          </div>
        )}
      </section>
      <p className="privacy-note">Files are sent directly to the receiver, one at a time in 16 KB chunks.</p>
    </main>
  );
}

export default CreatePipe;
