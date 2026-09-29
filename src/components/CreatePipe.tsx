import { useEffect, useRef, useState, type ChangeEvent, type DragEvent } from "react";

type CreatePipeProps = {
  onHome: () => void;
};

type PipeMessage = {
  type?: string;
  code?: string;
  message?: string;
  offer?: RTCSessionDescriptionInit;
  answer?: RTCSessionDescriptionInit;
  candidate?: RTCIceCandidateInit;
};

type SendStatus = "selecting" | "creating" | "waiting" | "connecting" | "connected" | "transfer";
type TransferStatus = "idle" | "preparing" | "sending" | "sent" | "error";
type QueueItem = { id: string; file: File; status: "waiting" | "sending" | "completed" | "failed"; bytesSent: number; error?: string };
const CHUNK_SIZE = 16 * 1024;
const BUFFER_LOW_WATER = 256 * 1024;
const BUFFER_HIGH_WATER = 512 * 1024;
// Binary chunk frame: uint16 ID length, uint32 chunk index, UTF-8 transfer ID, then raw file bytes.
const CHUNK_FRAME_HEADER_BYTES = 6;

const ICE_CONFIGURATION: RTCConfiguration = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
};

function getSignalingUrl() {
  return import.meta.env.VITE_SIGNALING_URL ||
    (import.meta.env.DEV ? "ws://localhost:8080" : "wss://drophut.onrender.com");
}

type SignalPayload = {
  type: "webrtc-offer" | "webrtc-answer" | "webrtc-ice";
  code: string;
  offer?: RTCSessionDescriptionInit;
  answer?: RTCSessionDescriptionInit;
  candidate?: RTCIceCandidateInit;
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
  const [queue, setQueue] = useState<QueueItem[]>([]);
  const [textDraft, setTextDraft] = useState("");
  const [queueRunning, setQueueRunning] = useState(false);
  const queueRunningRef = useRef(false);
  const files = queue.map((item) => item.file);
  const [code, setCode] = useState("");
  const [status, setStatus] = useState<SendStatus>("selecting");
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);
  const [transferStatus, setTransferStatus] = useState<TransferStatus>("idle");
  const [transferredBytes, setTransferredBytes] = useState(0);
  const [signalingStatus, setSignalingStatus] = useState<"connected" | "reconnecting">("connected");
  const [pipeNotice, setPipeNotice] = useState("");
  const socketRef = useRef<WebSocket | null>(null);
  const mountedRef = useRef(false);
  const peerConnectionRef = useRef<RTCPeerConnection | null>(null);
  const dataChannelRef = useRef<RTCDataChannel | null>(null);
  const queueRef = useRef<QueueItem[]>([]);
  const queueCancelledRef = useRef(false);
  const draggedQueueIdRef = useRef<string | null>(null);
  const transferStatusRef = useRef<TransferStatus>("idle");
  const transferTimeoutRef = useRef<number | null>(null);
  const transferIdRef = useRef<string | null>(null);
  const transferFileSizeRef = useRef<number | null>(null);
  const transferStartedAtRef = useRef(0);
  const maxBufferedAmountRef = useRef(0);
  const backpressureWaitsRef = useRef(0);
  const lastProgressUpdateRef = useRef(0);
  const transferSequenceRef = useRef(0);
  const lastAcknowledgedChunkRef = useRef(0);
  const transferTotalChunksRef = useRef(0);
  const peerTransferErrorRef = useRef<string | null>(null);
  const fileEndSentRef = useRef(false);
  const transferCancelledRef = useRef(false);
  const cancelWaitRef = useRef<(() => void) | null>(null);
  const pendingControlRef = useRef<{ type: string; transferId: string; nextChunk?: number; resolve: () => void; reject: (reason: Error) => void } | null>(null);
  const pendingIceRef = useRef<RTCIceCandidateInit[]>([]);
  const responseTimeoutRef = useRef<number | null>(null);
  const connectionTimeoutRef = useRef<number | null>(null);
  const statusRef = useRef<SendStatus>("selecting");
  const sessionCodeRef = useRef<string | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const pendingSignalsRef = useRef<SignalPayload[]>([]);
  const remotePeerDisconnectedRef = useRef(false);
  const channelErrorTimeoutRef = useRef<number | null>(null);

  function clearTimers() {
    if (responseTimeoutRef.current !== null) {
      window.clearTimeout(responseTimeoutRef.current);
      responseTimeoutRef.current = null;
    }
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
    if (transferTimeoutRef.current !== null) {
      window.clearTimeout(transferTimeoutRef.current);
      transferTimeoutRef.current = null;
    }
  }

  function closePeerConnection() {
    if (channelErrorTimeoutRef.current !== null) window.clearTimeout(channelErrorTimeoutRef.current);
    channelErrorTimeoutRef.current = null;
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

  function handlePeerDisconnected() {
    if (!sessionCodeRef.current) return;
    const transferActive = transferStatusRef.current === "preparing" || transferStatusRef.current === "sending";
    const completedTransfer = transferStatusRef.current === "sent";
    console.info("[DropHut sender] Receiver disconnected", { transferActive, completedTransfer });

    if (transferActive) {
      peerTransferErrorRef.current = "Receiver disconnected during transfer.";
      transferCancelledRef.current = true;
      cancelWaitRef.current?.();
      pendingControlRef.current?.reject(new Error("Receiver disconnected during transfer."));
      pendingControlRef.current = null;
      transferStatusRef.current = "error";
      setTransferStatus("error");
      setPipeNotice("Transfer interrupted because the receiver disconnected.");
    } else if (completedTransfer) {
      setPipeNotice("File sent successfully. Receiver disconnected. No receiver is currently available.");
    } else {
      setPipeNotice("Receiver disconnected. No receiver is currently available.");
    }

    if (connectionTimeoutRef.current !== null) window.clearTimeout(connectionTimeoutRef.current);
    connectionTimeoutRef.current = null;
    if (transferTimeoutRef.current !== null) window.clearTimeout(transferTimeoutRef.current);
    transferTimeoutRef.current = null;
    transferIdRef.current = null;
    transferFileSizeRef.current = null;
    fileEndSentRef.current = false;
    closePeerConnection();
    setError("");
    setStatus("waiting");
    statusRef.current = "waiting";
  }

  function fail(message: string) {
    console.error("[DropHut sender] Connection failure", { transferId: transferIdRef.current, message });
    peerTransferErrorRef.current = message;
    transferCancelledRef.current = true;
    cancelWaitRef.current?.();
    pendingControlRef.current?.reject(new Error(message));
    pendingControlRef.current = null;
    sessionCodeRef.current = null;
    if (reconnectTimerRef.current !== null) window.clearTimeout(reconnectTimerRef.current);
    reconnectTimerRef.current = null;
    pendingSignalsRef.current = [];
    clearTimers();
    closePeerConnection();
    const socket = socketRef.current;
    socketRef.current = null;
    if (socket) {
      socket.onopen = null;
      socket.onmessage = null;
      socket.onerror = null;
      socket.onclose = null;
      socket.close();
    }
    if (!mountedRef.current) return;
    setError(message);
    setCode("");
    setStatus("selecting");
    statusRef.current = "selecting";
    setSignalingStatus("connected");
  }

  function sendSignal(payload: SignalPayload) {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      pendingSignalsRef.current.push(payload);
      if (pendingSignalsRef.current.length > 256) pendingSignalsRef.current.shift();
      if (sessionCodeRef.current) scheduleSignalingReconnect(sessionCodeRef.current);
      return true;
    }
    try {
      socket.send(JSON.stringify(payload));
      return true;
    } catch {
      pendingSignalsRef.current.push(payload);
      if (pendingSignalsRef.current.length > 256) pendingSignalsRef.current.shift();
      if (sessionCodeRef.current) scheduleSignalingReconnect(sessionCodeRef.current);
      return true;
    }
  }

  function flushPendingSignals() {
    const queued = pendingSignalsRef.current.splice(0);
    for (const payload of queued) {
      if (payload.type === "webrtc-offer" || payload.type === "webrtc-answer") continue;
      if (!sendSignal(payload)) pendingSignalsRef.current.push(payload);
    }
  }

  function scheduleSignalingReconnect(pipeCode: string) {
    if (!mountedRef.current || sessionCodeRef.current !== pipeCode || reconnectTimerRef.current !== null) return;
    setSignalingStatus("reconnecting");
    const attempt = reconnectAttemptsRef.current++;
    const delay = Math.min(1000 * 2 ** Math.min(attempt, 4), 15000);
    console.warn("[DropHut sender] Signaling unavailable; retrying", { pipeCode, attempt: attempt + 1, delay });
    reconnectTimerRef.current = window.setTimeout(() => {
      reconnectTimerRef.current = null;
      if (!socketRef.current || socketRef.current.readyState === WebSocket.CLOSED) openSignalingSocket(pipeCode);
    }, delay);
  }

  function startConnectionTimeout() {
    if (connectionTimeoutRef.current !== null) return;
    connectionTimeoutRef.current = window.setTimeout(() => {
      fail("Could not establish a direct connection. Please try again on the same Wi-Fi network.");
    }, 25000);
  }

  function markDataChannelOpen(channel: RTCDataChannel) {
    if (dataChannelRef.current !== channel) return;
    console.info("[DropHut sender] DataChannel state: open");
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
    setError("");
    setPipeNotice("");
    if (transferStatusRef.current === "sent" || transferStatusRef.current === "error") {
      transferStatusRef.current = "idle";
      setTransferStatus("idle");
      setTransferredBytes(0);
    }
    setStatus("connected");
    statusRef.current = "connected";
  }

  function setupDataChannel(channel: RTCDataChannel) {
    dataChannelRef.current = channel;
    console.info("[DropHut sender] DataChannel created", channel.label, channel.readyState);
    remotePeerDisconnectedRef.current = false;
    channel.bufferedAmountLowThreshold = BUFFER_LOW_WATER;
    channel.onopen = () => markDataChannelOpen(channel);
    channel.onmessage = (event) => {
      if (typeof event.data !== "string") return;
      try {
        const message = JSON.parse(event.data) as { type?: string; transferId?: string; message?: string; nextChunk?: number; size?: number };
        const transferId = transferIdRef.current;
        if (!transferId || message.transferId !== transferId) return;
        if (message.type === "receiver-ready") {
          if (pendingControlRef.current?.type === "receiver-ready") {
            const pending = pendingControlRef.current;
            pendingControlRef.current = null;
            pending.resolve();
          }
          return;
        }
        if (message.type === "chunk-ack") {
          const size = transferFileSizeRef.current;
          if (size === null || !Number.isSafeInteger(message.nextChunk) || (message.nextChunk as number) <= lastAcknowledgedChunkRef.current || (message.nextChunk as number) > transferTotalChunksRef.current) return;
          lastAcknowledgedChunkRef.current = message.nextChunk as number;
          console.debug("[DropHut sender] Receiver acknowledged chunks", { transferId, nextChunk: message.nextChunk, bufferedAmount: dataChannelRef.current?.bufferedAmount ?? 0 });
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
          peerTransferErrorRef.current = "The receiver cancelled this file.";
          transferCancelledRef.current = true;
          cancelWaitRef.current?.();
          pendingControlRef.current?.reject(new Error(peerTransferErrorRef.current));
          pendingControlRef.current = null;
          return;
        }
        if (message.type === "receiver-ready") return;
        peerTransferErrorRef.current = "Received an unexpected transfer response.";
        transferCancelledRef.current = true;
        cancelWaitRef.current?.();
        pendingControlRef.current?.reject(new Error(peerTransferErrorRef.current));
        pendingControlRef.current = null;
      } catch { console.warn("[DropHut sender] Ignored invalid DataChannel response"); }
    };
    channel.onclose = () => {
      console.info("[DropHut sender] DataChannel state: closed");
      if (dataChannelRef.current !== channel) return;
      if (channelErrorTimeoutRef.current !== null) return;
      handlePeerDisconnected();
    };
    channel.onerror = (event) => {
      console.error("[DropHut sender] DataChannel error", event);
      if (dataChannelRef.current !== channel) return;
      const peer = peerConnectionRef.current;
      if (peer?.connectionState === "failed") {
        fail("The WebRTC connection failed.");
        return;
      }
      if (remotePeerDisconnectedRef.current || peer?.connectionState === "disconnected" || peer?.connectionState === "closed") {
        handlePeerDisconnected();
        return;
      }
      if (channelErrorTimeoutRef.current !== null) return;
      channelErrorTimeoutRef.current = window.setTimeout(() => {
        channelErrorTimeoutRef.current = null;
        if (dataChannelRef.current !== channel) return;
        const currentPeerState = peerConnectionRef.current?.connectionState;
        if (remotePeerDisconnectedRef.current || currentPeerState === "disconnected" || currentPeerState === "closed") {
          handlePeerDisconnected();
        } else {
          const transferActive = transferStatusRef.current === "preparing" || transferStatusRef.current === "sending";
          fail(transferActive ? "Transfer interrupted by a DataChannel error." : "The data channel encountered an error.");
        }
      }, 1200);
    };
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
        if (!sendSignal({ type: "webrtc-ice", code: pipeCode, candidate: event.candidate.toJSON() })) {
          fail("Lost the signaling connection while exchanging ICE candidates.");
        }
      } else {
        console.info("[DropHut sender] ICE candidate gathering complete");
      }
    };
    peer.oniceconnectionstatechange = () => {
      console.info("[DropHut sender] ICE connection state:", peer.iceConnectionState);
      if (peer.iceConnectionState === "failed") {
        const transferActive = transferStatusRef.current === "preparing" || transferStatusRef.current === "sending";
        fail(transferActive ? "Transfer interrupted because the WebRTC connection failed." : "The WebRTC connection failed. Try both devices on the same Wi-Fi network.");
      }
      if (peer.iceConnectionState === "disconnected") {
        if (statusRef.current === "connected") { setStatus("connecting"); statusRef.current = "connecting"; }
        startConnectionTimeout();
      }
    };
    peer.onconnectionstatechange = () => {
      console.info("[DropHut sender] Peer connection state:", peer.connectionState);
      if (peer.connectionState === "failed") {
        const transferActive = transferStatusRef.current === "preparing" || transferStatusRef.current === "sending";
        fail(transferActive ? "Transfer interrupted because the WebRTC connection failed." : "The WebRTC connection failed.");
      }
      if (peer.connectionState === "closed" && peerConnectionRef.current === peer) handlePeerDisconnected();
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
      if (!local || !sendSignal({ type: "webrtc-offer", code: pipeCode, offer: { type: local.type, sdp: local.sdp } })) {
        throw new Error("Could not send the WebRTC offer.");
      }
      console.info("[DropHut sender] Sent SDP offer through signaling WebSocket");
    } catch (reason) {
      fail(reason instanceof Error ? reason.message : "Could not create a WebRTC offer.");
    }
  }

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      sessionCodeRef.current = null;
      if (reconnectTimerRef.current !== null) window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
      transferCancelledRef.current = true;
      cancelWaitRef.current?.();
      pendingControlRef.current?.reject(new Error("Component unmounted."));
      pendingControlRef.current = null;
      clearTimers();
      closePeerConnection();
      const socket = socketRef.current;
      socketRef.current = null;
      if (socket) {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        socket.close();
      }
    };
  }, []);

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

  function openSignalingSocket(pipeCode: string) {
    if (socketRef.current && socketRef.current.readyState !== WebSocket.CLOSED) return;
    let socket: WebSocket;
    try {
      socket = new WebSocket(getSignalingUrl());
    } catch {
      scheduleSignalingReconnect(pipeCode);
      return;
    }
    socketRef.current = socket;
    socket.onopen = () => {
      if (responseTimeoutRef.current !== null) window.clearTimeout(responseTimeoutRef.current);
      responseTimeoutRef.current = window.setTimeout(() => {
        if (socketRef.current === socket && socket.readyState === WebSocket.OPEN) socket.close();
      }, 12000);
      try {
        socket.send(JSON.stringify({ type: "create-pipe", code: pipeCode }));
      } catch {
        socket.close();
      }
    };

    socket.onmessage = (event) => {
      if (responseTimeoutRef.current !== null) {
        window.clearTimeout(responseTimeoutRef.current);
        responseTimeoutRef.current = null;
      }
      let message: PipeMessage;
      try {
        message = JSON.parse(event.data);
      } catch {
        fail("Received an invalid response from the server.");
        return;
      }
      if (!message || typeof message !== "object" || Array.isArray(message)) {
        fail("Received an invalid response from the server.");
        return;
      }
      if (message.code !== undefined && message.code !== pipeCode) return;

      if (message.type === "error") {
        const retryable = message.message === "The other peer disconnected" ||
          (reconnectAttemptsRef.current > 0 && message.message === "This pipe code is already in use");
        if (retryable) {
          console.warn("[DropHut sender] Signaling session needs recovery", message.message);
          if (message.message === "The other peer disconnected") {
            remotePeerDisconnectedRef.current = true;
            handlePeerDisconnected();
          }
          socket.close();
        } else {
          fail(message.message || "The signaling server reported an error.");
        }
      } else if (message.type === "pipe-created" && message.code === pipeCode) {
        reconnectAttemptsRef.current = 0;
        setSignalingStatus("connected");
        if (statusRef.current === "creating") {
          setCode(pipeCode);
          setStatus("waiting");
          statusRef.current = "waiting";
        }
      } else if (message.type === "peer-joined" && message.code === pipeCode) {
        remotePeerDisconnectedRef.current = false;
        setPipeNotice("");
        setSignalingStatus("connected");
        reconnectAttemptsRef.current = 0;
        if (statusRef.current === "waiting") {
          setStatus("connecting");
          statusRef.current = "connecting";
          startConnectionTimeout();
          void startOffer(pipeCode).catch((reason: unknown) => {
            fail(reason instanceof Error ? reason.message : "Could not start the WebRTC offer.");
          });
        } else {
          const peer = peerConnectionRef.current;
          if (peer?.signalingState === "have-local-offer" && peer.localDescription && dataChannelRef.current?.readyState !== "open") {
            sendSignal({ type: "webrtc-offer", code: pipeCode, offer: { type: peer.localDescription.type, sdp: peer.localDescription.sdp } });
          }
          flushPendingSignals();
        }
      } else if (message.type === "webrtc-answer" && message.answer) {
        const peer = peerConnectionRef.current;
        if (!peer || peer.remoteDescription) return;
        console.debug("[DropHut sender] Received SDP answer", message.answer);
        void peer.setRemoteDescription(message.answer).then(() => {
          console.debug("[DropHut sender] Remote description set", peer.remoteDescription);
          return flushPendingIce(peer);
        }).catch((reason: unknown) => {
          fail(reason instanceof Error ? reason.message : "Could not apply the WebRTC answer.");
        });
      } else if (message.type === "webrtc-ice" && message.candidate) {
        console.info("[DropHut sender] Received remote ICE candidate", message.candidate);
        void addIceCandidate(message.candidate).catch((reason: unknown) => {
          fail(reason instanceof Error ? reason.message : "Could not apply an ICE candidate.");
        });
      } else {
        fail("Received an unexpected signaling response.");
      }
    };

    socket.onerror = (event) => {
      console.warn("[DropHut sender] WebSocket error; preserving peer connection and retrying", event);
    };
    socket.onclose = () => {
      if (socketRef.current !== socket) return;
      socketRef.current = null;
      if (responseTimeoutRef.current !== null) {
        window.clearTimeout(responseTimeoutRef.current);
        responseTimeoutRef.current = null;
      }
      if (sessionCodeRef.current === pipeCode) scheduleSignalingReconnect(pipeCode);
    };
  }

  function createPipe() {
    if (socketRef.current || status === "creating") return;

    const random = new Uint32Array(1);
    crypto.getRandomValues(random);
    const newCode = String(100000 + (random[0] % 900000));
    transferStatusRef.current = "idle";
    transferCancelledRef.current = false;
    setTransferredBytes(0);
    setTransferStatus("idle");
    setCode("");
    setError("");
    setStatus("creating");
    statusRef.current = "creating";

    sessionCodeRef.current = newCode;
    reconnectAttemptsRef.current = 0;
    openSignalingSocket(newCode);
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

  function waitForTransferControl(type: string, transferId: string) {
    return new Promise<void>((resolve, reject) => {
      const timeout = window.setTimeout(() => {
        pendingControlRef.current = null;
        reject(new Error("The receiver did not confirm the completed file."));
      }, 5 * 60 * 1000);
      const finish = (callback: () => void) => { window.clearTimeout(timeout); callback(); };
      pendingControlRef.current = { type, transferId, resolve: () => finish(resolve), reject: (reason) => finish(() => reject(reason)) };
    });
  }

  function cancelLocalTransfer() {
    if (transferCancelledRef.current && transferStatusRef.current === "idle") return;
    console.info("[DropHut sender] Transfer cancelled", { transferId: transferIdRef.current });
    transferCancelledRef.current = true;
    cancelWaitRef.current?.();
    pendingControlRef.current?.reject(new Error("Transfer cancelled."));
    pendingControlRef.current = null;
    if (transferTimeoutRef.current !== null) { window.clearTimeout(transferTimeoutRef.current); transferTimeoutRef.current = null; }
    transferIdRef.current = null; transferFileSizeRef.current = null; fileEndSentRef.current = false; transferStatusRef.current = "idle";
    setTransferStatus("idle"); setTransferredBytes(0); setError("");
    setStatus("connected"); statusRef.current = "connected";
  }

  function cancelTransfer() {
    const transferId = transferIdRef.current; const channel = dataChannelRef.current;
    queueCancelledRef.current = true;
    if (transferId && channel?.readyState === "open") { try { channel.send(JSON.stringify({ type: "file-cancel", transferId })); } catch { /* Local cancellation still proceeds. */ } }
    updateQueue((current) => current.map((item) => item.status === "sending" ? { ...item, status: "waiting", bytesSent: 0, error: undefined } : item));
    cancelLocalTransfer();
  }

  async function sendFile(file: File, queueId: string) {
    const channel = dataChannelRef.current;
    if (!channel || channel.readyState !== "open") throw new Error("The direct connection is not open.");
    const transferId = `${sessionCodeRef.current ?? "pipe"}-${++transferSequenceRef.current}-${createLocalId()}`;
    const transferIdBytes = new TextEncoder().encode(transferId);
    const chunkHeaderLength = CHUNK_FRAME_HEADER_BYTES + transferIdBytes.byteLength;
    const peerLimit = peerConnectionRef.current?.sctp?.maxMessageSize;
    if (transferIdBytes.byteLength > 0xffff || (peerLimit && peerLimit > 0 && CHUNK_SIZE + chunkHeaderLength > peerLimit)) {
      throw new Error("The connection cannot carry the selected chunk size and transfer header.");
    }
    transferIdRef.current = transferId; transferFileSizeRef.current = file.size; fileEndSentRef.current = false; transferCancelledRef.current = false;
    transferStartedAtRef.current = getTransferClock(); maxBufferedAmountRef.current = channel.bufferedAmount; backpressureWaitsRef.current = 0; lastProgressUpdateRef.current = 0;
    transferStatusRef.current = "preparing"; setTransferStatus("preparing"); setTransferredBytes(0); setError("");
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    transferTotalChunksRef.current = totalChunks; lastAcknowledgedChunkRef.current = 0; peerTransferErrorRef.current = null;
    console.info("[DropHut sender] Transfer started", { transferId, name: file.name, mimeType: file.type || "application/octet-stream", fileSize: file.size, totalChunks, chunkSize: CHUNK_SIZE });
    try {
      channel.send(JSON.stringify({ type: "file-start", transferId, name: file.name, mimeType: file.type || "application/octet-stream", size: file.size, totalChunks, chunkSize: CHUNK_SIZE }));
      if (transferCancelledRef.current) throw new Error("Transfer cancelled.");
      transferStatusRef.current = "sending"; setTransferStatus("sending");
      let sentChunks = 0;
      let firstChunkDelayMs = 0;
      for (let index = 0; index < totalChunks; index += 1) {
        if (transferCancelledRef.current) throw new Error("Transfer cancelled.");
        if (dataChannelRef.current !== channel || channel.readyState !== "open") throw new Error("The direct connection closed before the file could be sent.");
        if (index > 0 && channel.bufferedAmount > BUFFER_HIGH_WATER) {
          backpressureWaitsRef.current += 1;
          await waitForBufferDrain(channel);
        }
        if (transferCancelledRef.current) throw new Error("Transfer cancelled.");
        const offset = index * CHUNK_SIZE;
        const chunkHeader = new Uint8Array(chunkHeaderLength);
        const headerView = new DataView(chunkHeader.buffer);
        headerView.setUint16(0, transferIdBytes.byteLength);
        headerView.setUint32(2, index);
        chunkHeader.set(transferIdBytes, CHUNK_FRAME_HEADER_BYTES);
        const chunkPayload = file.slice(offset, Math.min(offset + CHUNK_SIZE, file.size));
        channel.send(new Blob([chunkHeader, chunkPayload], { type: "application/octet-stream" }));
        sentChunks = index + 1;
        if (index === 0) firstChunkDelayMs = getTransferClock() - transferStartedAtRef.current;
        maxBufferedAmountRef.current = Math.max(maxBufferedAmountRef.current, channel.bufferedAmount);
        const sentBytes = Math.min(file.size, sentChunks * CHUNK_SIZE);
        const now = getTransferClock();
        if (index === 0 || now - lastProgressUpdateRef.current >= 100) {
          setTransferredBytes(sentBytes);
          updateQueue((current) => current.map((item) => item.id === queueId ? { ...item, bytesSent: sentBytes } : item));
          lastProgressUpdateRef.current = now;
        }
      }
      if (transferCancelledRef.current) throw new Error("Transfer cancelled.");
      if (dataChannelRef.current !== channel || channel.readyState !== "open") throw new Error("The direct connection closed before the file could be completed.");
      channel.send(JSON.stringify({ type: "file-end", transferId, totalChunks, size: file.size }));
      fileEndSentRef.current = true;
      console.info("[DropHut sender] Sent all chunks and file-end", { transferId, totalChunks, size: file.size, firstChunkDelayMs: Math.round(firstChunkDelayMs), maxBufferedAmount: maxBufferedAmountRef.current, backpressureWaits: backpressureWaitsRef.current });
      await waitForTransferControl("file-received", transferId);
      if (transferCancelledRef.current || transferIdRef.current !== transferId) throw new Error("Transfer cancelled.");
      const durationMs = getTransferClock() - transferStartedAtRef.current;
      const mibPerSecond = durationMs > 0 ? file.size / (1024 * 1024) / (durationMs / 1000) : 0;
      console.info("[DropHut sender] Transfer completed and acknowledged", { transferId, fileSize: file.size, totalChunks, durationMs: Math.round(durationMs), mibPerSecond: Number(mibPerSecond.toFixed(2)), maxBufferedAmount: maxBufferedAmountRef.current, backpressureWaits: backpressureWaitsRef.current });
      if (transferTimeoutRef.current !== null) { window.clearTimeout(transferTimeoutRef.current); transferTimeoutRef.current = null; }
      pendingControlRef.current = null; transferIdRef.current = null; transferFileSizeRef.current = null; fileEndSentRef.current = false; transferCancelledRef.current = false;
      transferStatusRef.current = "sent"; setTransferStatus("sent");
    } catch (reason) {
      if (transferTimeoutRef.current !== null) { window.clearTimeout(transferTimeoutRef.current); transferTimeoutRef.current = null; }
      const activeTransferId = transferIdRef.current;
      if (activeTransferId && dataChannelRef.current === channel && channel.readyState === "open") {
        try { channel.send(JSON.stringify({ type: "file-cancel", transferId: activeTransferId })); } catch { /* Receiver cleanup is best effort after a local failure. */ }
      }
      pendingControlRef.current?.reject(reason instanceof Error ? reason : new Error("Transfer failed."));
      pendingControlRef.current = null; transferIdRef.current = null; transferFileSizeRef.current = null; transferTotalChunksRef.current = 0; fileEndSentRef.current = false;
      const failure = peerTransferErrorRef.current; peerTransferErrorRef.current = null;
      throw failure ? new Error(failure) : reason;
    }
  }

  async function sendQueue() {
    const channel = dataChannelRef.current;
    if (queueRunningRef.current || !queueRef.current.some((item) => item.status === "waiting") || !channel || channel.readyState !== "open") return;
    queueRunningRef.current = true;
    queueCancelledRef.current = false;
    setQueueRunning(true); setError(""); setStatus("transfer"); statusRef.current = "transfer";
    const pending = queueRef.current.filter((item) => item.status === "waiting");
    for (const item of pending) {
      if (queueCancelledRef.current) break;
      if (dataChannelRef.current !== channel || channel.readyState !== "open") { setError("The direct connection closed. Waiting files remain in the queue."); break; }
      updateQueue((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "sending", bytesSent: 0, error: undefined } : entry));
      try {
        await sendFile(item.file, item.id);
        updateQueue((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "completed", bytesSent: entry.file.size, error: undefined } : entry));
      } catch (reason) {
        const message = reason instanceof Error ? reason.message : "Could not send this file.";
        console.error("[DropHut sender] Queue item failed", { name: item.file.name, message });
        if (!queueCancelledRef.current) {
          updateQueue((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "failed", error: message } : entry));
          setError(`${item.file.name}: ${message}`);
        }
        if (queueCancelledRef.current || channel.readyState !== "open") break;
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

  return (
    <main className="app-shell flow-shell">
      <header className="topbar">
        <button className="brand-button" onClick={onHome}><span className="brand-mark small" aria-hidden="true">D</span>Drophut</button>
        <button className="text-button" onClick={onHome}>← Home</button>
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
            {pipeNotice && <p className="pipe-hint" role="status">{pipeNotice}</p>}
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
              <button className="button button-primary queue-send-button" onClick={() => { void sendQueue(); }} disabled={queueRunning || !queue.some((item) => item.status === "waiting")}>{queueRunning ? "Sending queue…" : "Send queue"}</button>
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

        {error && <p className="error-message" role="alert">{error}</p>}
        {signalingStatus === "reconnecting" && (
          <p className="signaling-note" role="status">
            {status === "connected" || status === "transfer" ? "Signaling reconnecting; the direct connection is still active." : "Signaling reconnecting; keeping this pipe session available."}
          </p>
        )}
      </section>
      <p className="privacy-note">Files are sent directly to the receiver, one at a time in 16 KB chunks.</p>
    </main>
  );
}

export default CreatePipe;
