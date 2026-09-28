import { useEffect, useRef, useState, type ChangeEvent } from "react";

function formatFileSize(bytes: number) {
  if (bytes < 1024) return bytes + " B";
  const units = ["KB", "MB", "GB", "TB"];
  let size = bytes / 1024;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit += 1; }
  return size.toFixed(size >= 10 ? 0 : 1) + " " + units[unit];
}

type JoinPipeProps = {
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

type JoinStatus = "idle" | "joining" | "connecting" | "connected";
type WritableFile = { write(data: ArrayBuffer): Promise<void>; close(): Promise<void>; abort?: () => Promise<void> };
type SaveFileHandle = { createWritable(): Promise<WritableFile> };
type PickerWindow = Window & { showSaveFilePicker?: (options: { suggestedName: string }) => Promise<SaveFileHandle> };
type IncomingTransfer = { transferId: string; name: string; mimeType: string; size: number; totalChunks: number; chunkSize: number; chunks: ArrayBuffer[]; receivedBytes: number; writer: WritableFile | null; accepted: boolean };
type ReceivedFile = { name: string; mimeType: string; size: number; url?: string; savedToDisk: boolean };
const CHUNK_SIZE = 16 * 1024;
const ACK_BATCH_SIZE = 32;

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

function JoinPipe({ onHome }: JoinPipeProps) {
  const [code, setCode] = useState("");
  const [status, setStatus] = useState<JoinStatus>("idle");
  const [error, setError] = useState("");
  const [receivedFile, setReceivedFile] = useState<ReceivedFile | null>(null);
  const [incomingOffer, setIncomingOffer] = useState<{ name: string; mimeType: string; size: number } | null>(null);
  const [receiveStatus, setReceiveStatus] = useState<"waiting" | "offered" | "receiving" | "received" | "error">("waiting");
  const [receivedBytes, setReceivedBytes] = useState(0);
  const [receiveWarning, setReceiveWarning] = useState("");
  const socketRef = useRef<WebSocket | null>(null);
  const mountedRef = useRef(false);
  const peerConnectionRef = useRef<RTCPeerConnection | null>(null);
  const dataChannelRef = useRef<RTCDataChannel | null>(null);
  const incomingFileRef = useRef<IncomingTransfer | null>(null);
  const receivedFileUrlRef = useRef<string | null>(null);
  const retiredTransferIdsRef = useRef(new Set<string>());
  const incomingTimeoutRef = useRef<number | null>(null);
  const messageQueueRef = useRef<Promise<void>>(Promise.resolve());
  const pendingIceRef = useRef<RTCIceCandidateInit[]>([]);
  const responseTimeoutRef = useRef<number | null>(null);
  const connectionTimeoutRef = useRef<number | null>(null);
  const statusRef = useRef<JoinStatus>("idle");

  function clearTimers() {
    if (responseTimeoutRef.current !== null) {
      window.clearTimeout(responseTimeoutRef.current);
      responseTimeoutRef.current = null;
    }
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
  }

  function retireTransferId(transferId: string) {
    retiredTransferIdsRef.current.add(transferId);
    if (retiredTransferIdsRef.current.size > 128) {
      const oldest = retiredTransferIdsRef.current.values().next().value;
      if (oldest) retiredTransferIdsRef.current.delete(oldest);
    }
  }

  function clearIncomingTransfer(): Promise<void> {
    if (incomingTimeoutRef.current !== null) { window.clearTimeout(incomingTimeoutRef.current); incomingTimeoutRef.current = null; }
    const transfer = incomingFileRef.current;
    incomingFileRef.current = null;
    if (transfer) retireTransferId(transfer.transferId);
    if (!transfer) return Promise.resolve();
    transfer.chunks.length = 0;
    return transfer.writer?.abort ? transfer.writer.abort().catch(() => undefined) : Promise.resolve();
  }

  function armIncomingTimeout(channel: RTCDataChannel, transferId: string) {
    if (incomingTimeoutRef.current !== null) window.clearTimeout(incomingTimeoutRef.current);
    incomingTimeoutRef.current = window.setTimeout(() => {
      if (incomingFileRef.current?.transferId !== transferId) return;
      const transfer = incomingFileRef.current;
      retireTransferId(transferId);
      transfer.chunks.length = 0;
      incomingFileRef.current = null;
      incomingTimeoutRef.current = null;
      if (transfer.writer?.abort) void transfer.writer.abort().catch(() => undefined);
      setIncomingOffer(null);
      const message = "The file transfer timed out before it completed.";
      setReceiveStatus("error");
      setError(message);
      try { channel.send(JSON.stringify({ type: "file-error", transferId, message })); } catch { /* Connection may already be closed. */ }
    }, 5 * 60 * 1000);
  }

  function closePeerConnection() {
    clearIncomingTransfer();
    const channel = dataChannelRef.current;
    dataChannelRef.current = null;
    if (channel) {
      console.info("[DropHut receiver] Closing DataChannel", channel.readyState);
      channel.onopen = null;
      channel.onmessage = null;
      channel.onclose = null;
      channel.onerror = null;
      channel.close();
      console.info("[DropHut receiver] DataChannel state after close", channel.readyState);
    }
    const peer = peerConnectionRef.current;
    peerConnectionRef.current = null;
    if (peer) {
      console.info("[DropHut receiver] Closing RTCPeerConnection", peer.connectionState);
      peer.ondatachannel = null;
      peer.onicecandidate = null;
      peer.oniceconnectionstatechange = null;
      peer.onicegatheringstatechange = null;
      peer.onconnectionstatechange = null;
      peer.close();
      console.info("[DropHut receiver] RTCPeerConnection state after close", peer.connectionState);
    }
    pendingIceRef.current = [];
  }

  function fail(message: string) {
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
    setStatus("idle");
    statusRef.current = "idle";
  }

  function sendSignal(payload: SignalPayload) {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    try {
      socket.send(JSON.stringify(payload));
      return true;
    } catch {
      return false;
    }
  }

  function startConnectionTimeout() {
    if (connectionTimeoutRef.current !== null) return;
    connectionTimeoutRef.current = window.setTimeout(() => {
      fail("Could not establish a direct connection. Please try again on the same Wi-Fi network.");
    }, 25000);
  }

  function setConnected() {
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
    setError("");
    setStatus("connected");
    statusRef.current = "connected";
  }

  function supportsIncrementalSave() {
    return typeof (window as PickerWindow).showSaveFilePicker === "function";
  }

  function sendTransferControl(type: string, transferId: string, extra: Record<string, unknown> = {}) {
    const channel = dataChannelRef.current;
    if (channel?.readyState === "open") channel.send(JSON.stringify({ type, transferId, ...extra }));
  }

  async function chooseSaveDestination() {
    const transfer = incomingFileRef.current;
    const channel = dataChannelRef.current;
    const picker = (window as PickerWindow).showSaveFilePicker;
    if (!transfer || !channel || !picker || transfer.accepted) return;
    try {
      const handle = await picker.call(window, { suggestedName: transfer.name });
      const writer = await handle.createWritable();
      if (incomingFileRef.current !== transfer || channel.readyState !== "open") {
        if (writer.abort) await writer.abort();
        return;
      }
      transfer.writer = writer;
      transfer.accepted = true;
      console.info("[DropHut receiver] Destination ready", { transferId: transfer.transferId, name: transfer.name });
      setReceivedBytes(0);
      setReceiveStatus("receiving");
      setError("");
      armIncomingTimeout(channel, transfer.transferId);
      sendTransferControl("receiver-ready", transfer.transferId);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === "AbortError") return;
      const message = reason instanceof Error ? reason.message : "Could not open the destination file.";
      setError(message);
      setReceiveStatus("offered");
    }
  }

  function receiveAnotherFile() {
    if (receivedFileUrlRef.current) { URL.revokeObjectURL(receivedFileUrlRef.current); receivedFileUrlRef.current = null; }
    setReceivedFile(null);
    setReceivedBytes(0);
    setReceiveStatus("waiting");
    setError("");
    setReceiveWarning("");
  }

  async function cancelReceive() {
    const transfer = incomingFileRef.current;
    if (transfer) {
      await clearIncomingTransfer();
      sendTransferControl("file-cancelled", transfer.transferId);
    } else {
      await clearIncomingTransfer();
    }
    setReceivedBytes(0);
    setIncomingOffer(null);
    setReceiveStatus("waiting");
    setError("");
    setReceiveWarning("");
  }

  async function handleDataMessage(channel: RTCDataChannel, data: unknown) {
    if (dataChannelRef.current !== channel) return;
    const rejectTransfer = (message: string, transferId?: string) => {
      console.error("[DropHut receiver] Transfer failed", { transferId, message });
      void clearIncomingTransfer();
      setReceiveStatus("error");
      setError(message);
      try { channel.send(JSON.stringify({ type: "file-error", transferId, message })); } catch { /* Connection may already be closed. */ }
    };

    if (typeof data === "string") {
      let message: { type?: string; transferId?: string; name?: string; mimeType?: string; size?: number; totalChunks?: number; chunkSize?: number };
      try { message = JSON.parse(data); } catch { rejectTransfer("Received invalid transfer metadata."); return; }
      if (message.type === "file-cancel") {
        if (incomingFileRef.current?.transferId === message.transferId) {
          console.info("[DropHut receiver] Sender cancelled transfer", { transferId: message.transferId });
          await clearIncomingTransfer();
          setReceivedBytes(0);
          setIncomingOffer(null);
          setReceiveStatus("waiting");
          setError("");
          setReceiveWarning("");
          sendTransferControl("file-cancelled", message.transferId as string);
        }
        return;
      }
      if (message.type === "file-start") {
        if (typeof message.transferId === "string" && retiredTransferIdsRef.current.has(message.transferId)) {
          console.debug("[DropHut receiver] Ignored stale file-start", { transferId: message.transferId });
          return;
        }
        if (incomingFileRef.current) {
          if (incomingFileRef.current.transferId === message.transferId) return;
          try { channel.send(JSON.stringify({ type: "file-error", transferId: message.transferId, message: "Another transfer is already active." })); } catch { /* Connection may already be closed. */ }
          return;
        }
        const validSize = Number.isSafeInteger(message.size) && (message.size as number) >= 0;
        const validChunks = Number.isSafeInteger(message.totalChunks) && (message.totalChunks as number) >= 0;
        if (typeof message.transferId !== "string" || !message.transferId || typeof message.name !== "string" || !message.name || typeof message.mimeType !== "string" || !validSize || !validChunks || message.chunkSize !== CHUNK_SIZE || message.totalChunks !== Math.ceil((message.size as number) / CHUNK_SIZE)) {
          rejectTransfer("The offered file metadata is invalid.", message.transferId); return;
        }
        if (receivedFileUrlRef.current) { URL.revokeObjectURL(receivedFileUrlRef.current); receivedFileUrlRef.current = null; }
        setReceivedFile(null);
        const incremental = supportsIncrementalSave();
        console.info("[DropHut receiver] Transfer started", { transferId: message.transferId, name: message.name, size: message.size, totalChunks: message.totalChunks, incrementalSave: incremental });
        setIncomingOffer({ name: message.name, mimeType: message.mimeType, size: message.size as number });
        incomingFileRef.current = { transferId: message.transferId, name: message.name, mimeType: message.mimeType, size: message.size as number, totalChunks: message.totalChunks as number, chunkSize: CHUNK_SIZE, chunks: [], receivedBytes: 0, writer: null, accepted: !incremental };
        setReceivedBytes(0);
        setReceiveWarning(incremental ? "" : "This browser cannot save incrementally. The complete file will be held in memory; large transfers may exceed available memory.");
        setError("");
        if (incremental) {
          setReceiveStatus("offered");
        } else {
          setReceiveStatus("receiving");
          armIncomingTimeout(channel, message.transferId as string);
          console.info("[DropHut receiver] Receiver ready (memory fallback)", { transferId: message.transferId });
        sendTransferControl("receiver-ready", message.transferId);
        }
        return;
      }
      if (message.type === "file-end") {
        const transfer = incomingFileRef.current;
        if (!transfer || message.transferId !== transfer.transferId) {
          console.debug("[DropHut receiver] Ignored stale file-end", { transferId: message.transferId, activeTransferId: transfer?.transferId });
          return;
        }
        if (!transfer.accepted || message.totalChunks !== transfer.totalChunks || message.size !== transfer.size || transfer.chunks.length !== (transfer.writer ? 0 : transfer.totalChunks) || transfer.receivedBytes !== transfer.size) {
          rejectTransfer("The received file was incomplete or did not match its metadata.", message.transferId); return;
        }
        try {
          let url: string | undefined;
          if (transfer.writer) {
            await transfer.writer.close();
            if (incomingFileRef.current !== transfer) return;
          } else {
            const blob = new Blob(transfer.chunks, { type: transfer.mimeType });
            url = URL.createObjectURL(blob);
          }
          const { name, mimeType, size, transferId } = transfer;
          transfer.chunks.length = 0;
          if (incomingTimeoutRef.current !== null) { window.clearTimeout(incomingTimeoutRef.current); incomingTimeoutRef.current = null; }
          incomingFileRef.current = null;
          receivedFileUrlRef.current = url ?? null;
          retireTransferId(transferId);
          setReceivedFile({ name, mimeType, size, url, savedToDisk: !url });
          setIncomingOffer(null);
          setReceiveStatus("received");
          setError("");
          sendTransferControl("file-received", transferId, { size });
          console.info("[DropHut receiver] File transfer complete", { transferId, name, size, savedToDisk: !url });
        } catch (reason) {
          rejectTransfer(reason instanceof Error ? reason.message : "Could not finish writing the destination file.", transfer.transferId);
        }
        return;
      }
      if (message.transferId && message.transferId === incomingFileRef.current?.transferId) rejectTransfer("Unexpected transfer control message.", message.transferId);
      return;
    }

    if (!(data instanceof ArrayBuffer)) { rejectTransfer("Received an unsupported file chunk."); return; }
    const transfer = incomingFileRef.current;
    if (!transfer || !transfer.accepted) { rejectTransfer("Received file data before the receiver was ready."); return; }
    if (transfer.chunks.length >= transfer.totalChunks) { rejectTransfer("Received more chunks than expected.", transfer.transferId); return; }
    const chunkIndex = transfer.writer ? transfer.receivedBytes / transfer.chunkSize : transfer.chunks.length;
    const expectedBytes = chunkIndex === transfer.totalChunks - 1 ? transfer.size - chunkIndex * transfer.chunkSize : transfer.chunkSize;
    if (!Number.isInteger(chunkIndex) || data.byteLength !== expectedBytes) { rejectTransfer("A received file chunk had an invalid size or order.", transfer.transferId); return; }
    try {
      if (transfer.writer) await transfer.writer.write(data);
      else transfer.chunks.push(data);
      if (incomingFileRef.current !== transfer) return;
      transfer.receivedBytes += data.byteLength;
      armIncomingTimeout(channel, transfer.transferId);
      const nextChunk = chunkIndex + 1;
      if (nextChunk % ACK_BATCH_SIZE === 0 || nextChunk === transfer.totalChunks) {
        setReceivedBytes(transfer.receivedBytes);
        console.info("[DropHut receiver] Chunk batch written and acknowledged", { transferId: transfer.transferId, nextChunk, totalChunks: transfer.totalChunks, receivedBytes: transfer.receivedBytes });
        sendTransferControl("chunk-ack", transfer.transferId, { nextChunk });
      }
    } catch (reason) {
      rejectTransfer(reason instanceof Error ? reason.message : "Could not write a received file chunk.", transfer.transferId);
    }
  }

  function setupDataChannel(channel: RTCDataChannel) {
    dataChannelRef.current = channel;
    console.info("[DropHut receiver] Incoming DataChannel", channel.label, channel.readyState);
    channel.onopen = () => {
      console.info("[DropHut receiver] DataChannel state: open");
      if (dataChannelRef.current === channel) setConnected();
    };
    channel.binaryType = "arraybuffer";
    channel.onmessage = (event) => {
      messageQueueRef.current = messageQueueRef.current.then(() => handleDataMessage(channel, event.data)).catch((reason: unknown) => {
        const message = reason instanceof Error ? reason.message : "Could not process the file transfer.";
        setError(message);
        setReceiveStatus("error");
        clearIncomingTransfer();
      });
    };
    channel.onclose = () => {
      console.info("[DropHut receiver] DataChannel state: closed");
      if (dataChannelRef.current === channel) fail("The direct connection closed.");
    };
    channel.onerror = (event) => {
      console.error("[DropHut receiver] DataChannel error", event);
      if (dataChannelRef.current === channel) fail("The data channel encountered an error.");
    };
    if (channel.readyState === "open") {
      console.info("[DropHut receiver] DataChannel state: open");
      setConnected();
    }
  }

  function setupPeerConnection(pipeCode: string) {
    console.info("[DropHut receiver] Creating RTCPeerConnection");
    const peer = new RTCPeerConnection(ICE_CONFIGURATION);
    peerConnectionRef.current = peer;
    console.info("[DropHut receiver] RTCPeerConnection created");
    peer.ondatachannel = (event) => {
      console.info("[DropHut receiver] peer.ondatachannel fired", event.channel.label);
      setupDataChannel(event.channel);
    };
    peer.onicegatheringstatechange = () => {
      console.info("[DropHut receiver] ICE gathering state:", peer.iceGatheringState);
    };
    console.info("[DropHut receiver] RTCPeerConnection created", { iceGatheringState: peer.iceGatheringState, iceConnectionState: peer.iceConnectionState, connectionState: peer.connectionState });
    peer.onicecandidate = (event) => {
      if (event.candidate) {
        console.info("[DropHut receiver] Generated ICE candidate", event.candidate.toJSON());
        if (!sendSignal({ type: "webrtc-ice", code: pipeCode, candidate: event.candidate.toJSON() })) {
          fail("Lost the signaling connection while exchanging ICE candidates.");
        }
      } else {
        console.info("[DropHut receiver] ICE candidate gathering complete");
      }
    };
    peer.oniceconnectionstatechange = () => {
      console.info("[DropHut receiver] ICE connection state:", peer.iceConnectionState);
      if (peer.iceConnectionState === "failed") fail("ICE connection failed. Try both devices on the same Wi-Fi network.");
      if (peer.iceConnectionState === "disconnected") {
        if (statusRef.current === "connected") { setStatus("connecting"); statusRef.current = "connecting"; }
        startConnectionTimeout();
      }
      if (peer.iceConnectionState === "closed" && peerConnectionRef.current === peer) fail("The peer connection closed.");
    };
    peer.onconnectionstatechange = () => {
      console.info("[DropHut receiver] Peer connection state:", peer.connectionState);
      if (peer.connectionState === "failed") fail("Could not establish a direct peer connection.");
      if (peer.connectionState === "closed" && peerConnectionRef.current === peer) fail("The peer connection closed.");
      if (peer.connectionState === "disconnected") {
        if (statusRef.current === "connected") { setStatus("connecting"); statusRef.current = "connecting"; }
        startConnectionTimeout();
      }
      if (peer.connectionState === "connected" && dataChannelRef.current?.readyState === "open") setConnected();
    };
    return peer;
  }

  async function flushPendingIce(peer: RTCPeerConnection) {
    const candidates = pendingIceRef.current.splice(0);
    for (const candidate of candidates) {
      await peer.addIceCandidate(candidate);
    console.info("[DropHut receiver] Applied remote ICE candidate");
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

  async function handleOffer(pipeCode: string, offer: RTCSessionDescriptionInit) {
    if (peerConnectionRef.current) return;
    const peer = setupPeerConnection(pipeCode);
    if (statusRef.current === "joining") {
      setStatus("connecting");
      statusRef.current = "connecting";
      startConnectionTimeout();
    }

    try {
      console.info("[DropHut receiver] Received SDP offer", offer);
      await peer.setRemoteDescription(offer);
      console.debug("[DropHut receiver] Remote description set", peer.remoteDescription);
      await flushPendingIce(peer);
      console.info("[DropHut receiver] Creating SDP answer");
      const answer = await peer.createAnswer();
      console.debug("[DropHut receiver] SDP answer created", answer);
      await peer.setLocalDescription(answer);
      const local = peer.localDescription;
      console.debug("[DropHut receiver] Local description set", local);
      if (!local || !sendSignal({ type: "webrtc-answer", code: pipeCode, answer: { type: local.type, sdp: local.sdp } })) {
        throw new Error("Could not send the WebRTC answer.");
      }
      console.info("[DropHut receiver] Sent SDP answer through signaling WebSocket");
    } catch (reason) {
      fail(reason instanceof Error ? reason.message : "Could not answer the WebRTC offer.");
    }
  }

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      clearTimers();
      closePeerConnection();
      if (receivedFileUrlRef.current) { URL.revokeObjectURL(receivedFileUrlRef.current); receivedFileUrlRef.current = null; }
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
    // The cleanup reads only refs and stable module/browser APIs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function updateCode(event: ChangeEvent<HTMLInputElement>) {
    setCode(event.target.value.replace(/\D/g, "").slice(0, 6));
    setError("");
  }

  function joinPipe() {
    if (!/^\d{6}$/.test(code) || status === "joining" || socketRef.current) return;
    setError("");
    setStatus("joining");
    statusRef.current = "joining";

    let socket: WebSocket;
    try {
      socket = new WebSocket(getSignalingUrl());
    } catch {
      setError("Could not connect to the signaling server. Please try again.");
      setStatus("idle");
      statusRef.current = "idle";
      return;
    }
    socketRef.current = socket;
    responseTimeoutRef.current = window.setTimeout(() => fail("The signaling server did not respond. Please try again."), 5000);

    socket.onopen = () => {
      try {
        socket.send(JSON.stringify({ type: "join-pipe", code }));
      } catch {
        fail("Could not send the join request. Please try again.");
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
      if (message.code !== undefined && message.code !== code) return;

      if (message.type === "error") {
        fail(message.message || "The signaling server reported an error.");
      } else if (message.type === "peer-joined") {
        if (statusRef.current !== "joining") return;
        setStatus("connecting");
        statusRef.current = "connecting";
        startConnectionTimeout();
      } else if (message.type === "webrtc-offer" && message.offer) {
        void handleOffer(code, message.offer).catch((reason: unknown) => {
          fail(reason instanceof Error ? reason.message : "Could not handle the WebRTC offer.");
        });
      } else if (message.type === "webrtc-ice" && message.candidate) {
        console.info("[DropHut receiver] Received remote ICE candidate", message.candidate);
        void addIceCandidate(message.candidate).catch((reason: unknown) => {
          fail(reason instanceof Error ? reason.message : "Could not apply an ICE candidate.");
        });
      } else {
        fail("Received an unexpected signaling response.");
      }
    };

    socket.onerror = () => fail("Could not connect to the signaling server. Please try again.");
    socket.onclose = () => {
      if (socketRef.current === socket && statusRef.current !== "idle") {
        fail("The signaling connection closed.");
      }
    };
  }

  return (
    <main className="app-shell flow-shell">
      <header className="topbar">
        <button className="brand-button" onClick={onHome}><span className="brand-mark small" aria-hidden="true">D</span>Drophut</button>
        <button className="text-button" onClick={onHome}>← Home</button>
      </header>
      <section className="flow-content" aria-labelledby="join-title">
        <p className="eyebrow">RECEIVE FILES</p>
        {receivedFile ? (
          <div className="pipe-card receiver-card">
            <div className="connected-label"><span className="status-dot" />File received ✓</div>
            <h1 id="join-title">{receivedFile.name}</h1>
            <p className="pipe-hint">{formatFileSize(receivedFile.size)} · {receivedFile.mimeType}</p>
            {receivedFile.savedToDisk ? <p>Saved to the selected location.</p> : receivedFile.url && <a className="button button-primary button-full" href={receivedFile.url} download={receivedFile.name}>Download</a>}
            <button className="button button-secondary button-full" onClick={receiveAnotherFile}>Receive another file</button>
          </div>
        ) : status === "connected" ? (
          <div className="pipe-card receiver-card">
            <div className="connected-label"><span className="status-dot" />Connected ✓</div>
            <h1 id="join-title">{receiveStatus === "offered" ? "File ready to receive" : "Connected to pipe"}</h1>
            {incomingOffer && receiveStatus === "offered" ? <p className="pipe-hint">{incomingOffer.name} · {formatFileSize(incomingOffer.size)}</p> : null}
            <p className="pipe-hint">{receiveStatus === "receiving" ? "Receiving file…" : receiveStatus === "error" ? "Could not receive the file." : receiveStatus === "offered" ? "Choose where to save this file." : "Waiting for the sender to send a file."}</p>
            {receiveStatus === "receiving" && incomingOffer && <p>{formatFileSize(receivedBytes)} / {formatFileSize(incomingOffer.size)} · {incomingOffer.size ? Math.floor(receivedBytes / incomingOffer.size * 100) : 0}%</p>}
            {receiveWarning && <p className="transfer-note">{receiveWarning}</p>}
            {receiveStatus === "offered" && <button className="button button-primary button-full" onClick={() => { void chooseSaveDestination(); }}>Choose destination</button>}
            {(receiveStatus === "offered" || receiveStatus === "receiving") && <button className="button button-secondary button-full" onClick={() => { void cancelReceive(); }}>Cancel transfer</button>}
            {error && <p className="error-message" role="alert">{error}</p>}
          </div>
        ) : status === "connecting" ? (
          <div className="pipe-card receiver-card">
            <div className="waiting-line"><span className="status-dot pulse" />Connecting directly…</div>
            <p className="pipe-hint">Setting up a private connection to the sender.</p>
            {error && <p className="error-message" role="alert">{error}</p>}
          </div>
        ) : (
          <>
            <h1 id="join-title">Join a Pipe</h1>
            <p className="intro-copy">Enter the 6-digit code shared by the sender.</p>
            <div className="join-form">
              <label htmlFor="pipe-code">Pipe code</label>
              <input
                id="pipe-code"
                className="code-input"
                type="text"
                inputMode="numeric"
                autoComplete="one-time-code"
                maxLength={6}
                placeholder="000000"
                value={code}
                onChange={updateCode}
                disabled={status === "joining"}
              />
              <button className="button button-primary button-full" onClick={joinPipe} disabled={code.length !== 6 || status === "joining"}>
                {status === "joining" ? "Connecting…" : "Join Pipe"}
              </button>
            </div>
            {error && <p className="error-message" role="alert">{error}</p>}
          </>
        )}
      </section>
    </main>
  );
}

export default JoinPipe;
