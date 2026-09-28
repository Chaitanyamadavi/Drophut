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
const CHUNK_SIZE = 16 * 1024;
const BUFFER_LOW_WATER = 256 * 1024;
const BUFFER_HIGH_WATER = 512 * 1024;

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

function CreatePipe({ onHome }: CreatePipeProps) {
  const [files, setFiles] = useState<File[]>([]);
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
  const transferStatusRef = useRef<TransferStatus>("idle");
  const transferTimeoutRef = useRef<number | null>(null);
  const transferIdRef = useRef<string | null>(null);
  const transferFileSizeRef = useRef<number | null>(null);
  const transferStartedAtRef = useRef(0);
  const maxBufferedAmountRef = useRef(0);
  const backpressureWaitsRef = useRef(0);
  const lastProgressUpdateRef = useRef(0);
  const transferSequenceRef = useRef(0);
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
        if (message.transferId && message.transferId === transferIdRef.current) {
          const pending = pendingControlRef.current;
          if (pending && message.type === pending.type && pending.nextChunk !== undefined && message.nextChunk !== pending.nextChunk) return;
          if (pending && message.type === pending.type) {
            pendingControlRef.current = null;
            pending.resolve();
          } else if (message.type === "chunk-ack" || message.type === "receiver-ready") {
            return;
          } else if (message.type === "file-received" && fileEndSentRef.current && transferFileSizeRef.current !== null && message.size === transferFileSizeRef.current) {
            const durationMs = performance.now() - transferStartedAtRef.current;
            const mibPerSecond = durationMs > 0 ? message.size / (1024 * 1024) / (durationMs / 1000) : 0;
            console.info("[DropHut sender] Transfer completed and acknowledged", { transferId: message.transferId, fileSize: message.size, totalChunks: Math.ceil(message.size / CHUNK_SIZE), durationMs: Math.round(durationMs), mibPerSecond: Number(mibPerSecond.toFixed(2)), maxBufferedAmount: maxBufferedAmountRef.current, backpressureWaits: backpressureWaitsRef.current });
            if (transferTimeoutRef.current !== null) window.clearTimeout(transferTimeoutRef.current);
            transferTimeoutRef.current = null;
            pendingControlRef.current = null;
            transferIdRef.current = null;
            transferFileSizeRef.current = null;
            fileEndSentRef.current = false;
            transferCancelledRef.current = false;
            transferStatusRef.current = "sent";
            setTransferStatus("sent");
          } else if (message.type === "file-error") {
            transferCancelledRef.current = true;
            cancelWaitRef.current?.();
            if (transferTimeoutRef.current !== null) { window.clearTimeout(transferTimeoutRef.current); transferTimeoutRef.current = null; }
            pendingControlRef.current?.reject(new Error(message.message || "The receiver could not accept this file."));
            pendingControlRef.current = null;
            transferStatusRef.current = "error";
            setTransferStatus("error");
            setError(message.message || "The receiver could not accept this file.");
          } else if (message.type === "file-cancelled" && (transferStatusRef.current === "preparing" || transferStatusRef.current === "sending")) cancelLocalTransfer();
          else if (transferStatusRef.current === "preparing" || transferStatusRef.current === "sending") {
            pendingControlRef.current?.reject(new Error("Received an unexpected transfer response."));
            pendingControlRef.current = null;
            transferCancelledRef.current = true;
            transferStatusRef.current = "error";
            setTransferStatus("error");
            setError("Received an unexpected transfer response.");
          }
        }
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

  function selectFiles(fileList: FileList | null) {
    if (!fileList || statusRef.current !== "selecting") return;
    const file = fileList[0];
    if (!file) return;
    setFiles([file]);
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
    if (!files.length || socketRef.current || status === "creating") return;

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
      const onLow = () => { cleanup(); resolve(); };
      const onClose = () => { cleanup(); reject(new Error("The direct connection closed while sending.")); };
      const onError = () => { cleanup(); reject(new Error("The data channel failed while sending.")); };
      const onCancel = () => { cleanup(); reject(new Error("Transfer cancelled.")); };
      const resetStallTimeout = () => {
        window.clearTimeout(stallTimeout);
        stallTimeout = window.setTimeout(() => { cleanup(); reject(new Error("The DataChannel buffer stopped draining.")); }, 5 * 60 * 1000);
      };
      const pollBuffer = () => {
        const amount = channel.bufferedAmount;
        if (amount <= BUFFER_LOW_WATER) { cleanup(); resolve(); return; }
        if (amount < lastAmount) { lastAmount = amount; resetStallTimeout(); }
        pollTimeout = window.setTimeout(pollBuffer, 5000);
      };
      cancelWaitRef.current = onCancel;
      channel.addEventListener("bufferedamountlow", onLow);
      channel.addEventListener("close", onClose);
      channel.addEventListener("error", onError);
      resetStallTimeout();
      pollTimeout = window.setTimeout(pollBuffer, 5000);
      if (channel.bufferedAmount <= BUFFER_LOW_WATER) { cleanup(); resolve(); }
    });
  }

  function waitForTransferControl(type: string, transferId: string, nextChunk?: number) {
    return new Promise<void>((resolve, reject) => {
      const timeout = window.setTimeout(() => { pendingControlRef.current = null; reject(new Error("The receiver did not respond. The transfer timed out.")); }, 5 * 60 * 1000);
      const finish = (callback: () => void) => { window.clearTimeout(timeout); callback(); };
      pendingControlRef.current = { type, transferId, nextChunk, resolve: () => finish(resolve), reject: (reason) => finish(() => reject(reason)) };
    });
  }

  function cancelLocalTransfer() {
    if (transferCancelledRef.current && transferStatusRef.current === "idle") return;
    console.info("[DropHut sender] Transfer cancelled", { transferId: transferIdRef.current });
    transferCancelledRef.current = true;
    cancelWaitRef.current?.();
    pendingControlRef.current?.reject(new Error("Transfer cancelled.")); pendingControlRef.current = null;
    if (transferTimeoutRef.current !== null) { window.clearTimeout(transferTimeoutRef.current); transferTimeoutRef.current = null; }
    transferIdRef.current = null; transferFileSizeRef.current = null; fileEndSentRef.current = false; transferStatusRef.current = "idle"; setTransferStatus("idle"); setTransferredBytes(0); setError("");
    setStatus("connected"); statusRef.current = "connected";
  }

  function prepareAnotherTransfer() {
    transferStatusRef.current = "idle";
    transferIdRef.current = null;
    transferFileSizeRef.current = null;
    fileEndSentRef.current = false;
    transferCancelledRef.current = false;
    setTransferStatus("idle");
    setTransferredBytes(0);
    setError("");
    setStatus("connected");
    statusRef.current = "connected";
  }

  function handleNextFileChange(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (file) { setFiles([file]); prepareAnotherTransfer(); }
    event.target.value = "";
  }

  function cancelTransfer() {
    const transferId = transferIdRef.current; const channel = dataChannelRef.current;
    if (transferId && channel?.readyState === "open") { try { channel.send(JSON.stringify({ type: "file-cancel", transferId })); } catch { /* Local cancellation still proceeds. */ } }
    cancelLocalTransfer();
  }

  async function sendFile() {
    const file = files[0]; const channel = dataChannelRef.current;
    if (!file || !channel || channel.readyState !== "open" || transferStatusRef.current === "preparing" || transferStatusRef.current === "sending" || transferStatusRef.current === "sent") return;
    const peerLimit = peerConnectionRef.current?.sctp?.maxMessageSize;
    if (peerLimit && peerLimit > 0 && CHUNK_SIZE > peerLimit) { setError("The connection cannot carry the selected chunk size."); return; }
    const transferId = String(++transferSequenceRef.current);
    transferIdRef.current = transferId; transferFileSizeRef.current = file.size; fileEndSentRef.current = false; transferCancelledRef.current = false; transferStartedAtRef.current = performance.now(); maxBufferedAmountRef.current = channel.bufferedAmount; backpressureWaitsRef.current = 0; lastProgressUpdateRef.current = 0; setError(""); setTransferredBytes(0);
    transferStatusRef.current = "preparing"; setTransferStatus("preparing"); setStatus("transfer"); statusRef.current = "transfer";
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE); const batchSize = 32;
    console.info("[DropHut sender] Transfer started", { transferId, name: file.name, mimeType: file.type || "application/octet-stream", fileSize: file.size, totalChunks, chunkSize: CHUNK_SIZE });
    try {
      channel.send(JSON.stringify({ type: "file-start", transferId, name: file.name, mimeType: file.type || "application/octet-stream", size: file.size, totalChunks, chunkSize: CHUNK_SIZE }));
      await waitForTransferControl("receiver-ready", transferId);
      console.info("[DropHut sender] Receiver ready", { transferId });
      if (transferCancelledRef.current) return;
      transferStatusRef.current = "sending"; setTransferStatus("sending");
      for (let index = 0; index < totalChunks; index += 1) {
        if (transferCancelledRef.current) return;
        if (channel.bufferedAmount > BUFFER_HIGH_WATER) {
          backpressureWaitsRef.current += 1;
          await waitForBufferDrain(channel);
        }
        if (transferCancelledRef.current) return;
        if (dataChannelRef.current !== channel || channel.readyState !== "open") throw new Error("The direct connection closed before the file could be sent.");
        const start = index * CHUNK_SIZE;
        const chunk = file.slice(start, Math.min(start + CHUNK_SIZE, file.size));
        if (transferCancelledRef.current) return;
        channel.send(chunk);
        maxBufferedAmountRef.current = Math.max(maxBufferedAmountRef.current, channel.bufferedAmount);
        const nextChunk = index + 1;
        if (nextChunk % batchSize === 0 || nextChunk === totalChunks) {
          await waitForTransferControl("chunk-ack", transferId, nextChunk);
          const now = performance.now();
          if (nextChunk === totalChunks || now - lastProgressUpdateRef.current >= 250) {
            setTransferredBytes(Math.min(file.size, nextChunk * CHUNK_SIZE));
            lastProgressUpdateRef.current = now;
          }
        }
      }
      if (transferCancelledRef.current) return;
      if (dataChannelRef.current !== channel || channel.readyState !== "open") throw new Error("The direct connection closed before the file could be completed.");
      channel.send(JSON.stringify({ type: "file-end", transferId, totalChunks, size: file.size }));
      fileEndSentRef.current = true;
      console.info("[DropHut sender] Sent file-end", { transferId, totalChunks, size: file.size });
      transferTimeoutRef.current = window.setTimeout(() => { if (transferStatusRef.current === "sending") { transferStatusRef.current = "error"; setTransferStatus("error"); setError("The receiver did not confirm the completed file."); } }, 5 * 60 * 1000);
    } catch (reason) {
      if (transferCancelledRef.current) return;
      console.error("[DropHut sender] Transfer failed", { transferId, reason });
      if (dataChannelRef.current === channel && channel.readyState === "open") {
        try { channel.send(JSON.stringify({ type: "file-cancel", transferId })); } catch { /* Connection may already be closed. */ }
      }
      transferStatusRef.current = "error"; setTransferStatus("error"); setError(reason instanceof Error ? reason.message : "Could not send the file.");
    }
  }

  function showTransferNotice() {
    setStatus("transfer");
    statusRef.current = "transfer";
    void sendFile();
  }

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
          <>
            <div className="drop-zone" onDragOver={(event) => event.preventDefault()} onDrop={handleDrop}>
              <div className="upload-icon" aria-hidden="true">↑</div>
              <strong>Drop files here</strong>
              <span>or <label htmlFor="file-picker" className="browse-link">browse</label></span>
              <input id="file-picker" className="visually-hidden" type="file" onChange={handleFileChange} />
            </div>
            {files.length > 0 && (
              <div className="file-panel">
                <div className="section-heading"><h2>File</h2><span>1 file selected</span></div>
                <ul className="file-list">
                  {files.map((file, index) => (
                    <li className="file-row" key={file.name + file.lastModified + index}>
                      <span className="file-icon" aria-hidden="true">↗</span>
                      <span className="file-name">{file.name}</span>
                      <span className="file-size">{formatFileSize(file.size)}</span>
                    </li>
                  ))}
                </ul>
                <button className="button button-primary button-full" onClick={createPipe} disabled={status === "creating"}>
                  {status === "creating" ? "Creating your pipe…" : "Create Pipe"}
                </button>
              </div>
            )}
          </>
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

        {status === "connected" && (
          <div className="pipe-card connected-card">
            <div className="connected-label"><span className="status-dot" />Connected ✓</div>
            <p className="file-count large-count">{files.length} {files.length === 1 ? "file" : "files"} ready</p>
            <ul className="compact-file-list">
              {files.map((file, index) => <li key={file.name + file.lastModified + index}>{file.name}<span>{formatFileSize(file.size)}</span></li>)}
            </ul>
            <button className="button button-primary button-full" onClick={showTransferNotice}>Send</button>
          </div>
        )}

        {status === "transfer" && (
          <div className="pipe-card transfer-card">
            <div className="connected-label"><span className="status-dot" />Connected ✓</div>
            <h2>{transferStatus === "sending" ? "Sending file…" : transferStatus === "sent" ? "File sent ✓" : transferStatus === "error" ? "Send failed" : "Ready to send"}</h2>
            <p>{files[0]?.name} · {formatFileSize(files[0]?.size ?? 0)}</p>
            {(transferStatus === "preparing" || transferStatus === "sending") && <p>{formatFileSize(transferredBytes)} / {formatFileSize(files[0]?.size ?? 0)} · {files[0]?.size ? Math.floor(transferredBytes / files[0].size * 100) : 0}%</p>}
            {transferStatus === "preparing" && <p>Waiting for receiver to get ready…</p>}
            {(transferStatus === "preparing" || transferStatus === "sending") && <button className="button button-secondary button-full" onClick={cancelTransfer}>Cancel transfer</button>}
            {transferStatus === "idle" && <button className="button button-primary button-full" onClick={sendFile}>Send file</button>}
            {transferStatus === "sent" && <><p>The receiver confirmed the file.</p><button className="button button-primary button-full" onClick={prepareAnotherTransfer}>Send another</button><label className="button button-secondary button-full" htmlFor="next-file-picker">Choose another file</label><input id="next-file-picker" className="visually-hidden" type="file" onChange={handleNextFileChange} /></>}
            <button className="button button-secondary button-full" onClick={() => { setStatus("connected"); statusRef.current = "connected"; }}>Back</button>
          </div>
        )}

        {error && <p className="error-message" role="alert">{error}</p>}
        {signalingStatus === "reconnecting" && (
          <p className="signaling-note" role="status">
            {status === "connected" || status === "transfer" ? "Signaling reconnecting; the direct connection is still active." : "Signaling reconnecting; keeping this pipe session available."}
          </p>
        )}
      </section>
      <p className="privacy-note">Files are sent directly to the receiver. One file is sent in 16 KB chunks.</p>
    </main>
  );
}

export default CreatePipe;
