import { reportCompletedFile } from "./statsClient";
import { useEffect, useRef, useState, type ChangeEvent } from "react";
import { assembleFile, deleteSessionBatches, loadCheckpoint, saveCheckpoint, type Checkpoint } from "./pipeCheckpoint";
import { PipeSignaling, readSession, type Signal, type SignalState } from "./pipeSignaling";

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

type JoinStatus = "idle" | "joining" | "connecting" | "connected";
type IncomingTransfer = Checkpoint & { key: string; chunks: ArrayBuffer[]; savedChunk: number };
type ReceivedFile = { name: string; mimeType: string; size: number; url: string };
type ReceivedTextMessage = { id: string; text: string };
type DownloadNotice = { name: string; url: string; openBlocked: boolean };
const CHUNK_SIZE = 16 * 1024;
const ACK_BATCH_SIZE = 8;
// Must match the sender frame: uint16 ID length, uint32 chunk index, UTF-8 transfer ID, then payload.
const CHUNK_FRAME_HEADER_BYTES = 6;

const ICE_CONFIGURATION: RTCConfiguration = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
};

function JoinPipe({ onHome }: JoinPipeProps) {
  const [code, setCode] = useState(() => readSession("receiver")?.code ?? "");
  const [status, setStatus] = useState<JoinStatus>(() => readSession("receiver") ? "joining" : "idle");
  const [pipeNotice, setPipeNotice] = useState("");
  const [error, setError] = useState("");
  const [receivedFile, setReceivedFile] = useState<ReceivedFile | null>(null);
  const [incomingOffer, setIncomingOffer] = useState<{ name: string; mimeType: string; size: number } | null>(null);
  const [receiveStatus, setReceiveStatus] = useState<"waiting" | "receiving" | "received" | "error">("waiting");
  const [receivedBytes, setReceivedBytes] = useState(0);
  const [receiveWarning, setReceiveWarning] = useState("");
  const [downloadNotice, setDownloadNotice] = useState<DownloadNotice | null>(null);
  const [receivedTexts, setReceivedTexts] = useState<ReceivedTextMessage[]>([]);
  const [signalingStatus, setSignalingStatus] = useState<SignalState>("connected");
  const signalingRef = useRef<PipeSignaling | null>(null);
  const peerConnectionRef = useRef<RTCPeerConnection | null>(null);
  const dataChannelRef = useRef<RTCDataChannel | null>(null);
  const incomingFileRef = useRef<IncomingTransfer | null>(null);
  const receivedFileUrlRef = useRef<string | null>(null);
  const downloadUrlTimersRef = useRef(new Map<string, number>());
  const messagesRef = useRef(Promise.resolve());
  const pendingIceRef = useRef<RTCIceCandidateInit[]>([]);
  const resetTimerRef = useRef(0);
  const connectionTimeoutRef = useRef<number | null>(null);
  const statusRef = useRef<JoinStatus>("idle");
  const expiredRef = useRef(false);
  const sessionCodeRef = useRef<string | null>(null);

  function clearTimers() {
    window.clearTimeout(resetTimerRef.current);
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
  }

  function checkpointKey(transferId: string) {
    const token = signalingRef.current?.session?.token;
    if (!token) throw new Error("The pipe session has ended.");
    return token + ":" + transferId;
  }

  function persisted(transfer: IncomingTransfer, state: Checkpoint["state"]): Checkpoint {
    const { transferId, name, size, mimeType, totalChunks, chunkSize, nextChunk } = transfer;
    return { transferId, name, size, mimeType, totalChunks, chunkSize, nextChunk, state };
  }

  function enqueueMessage(work: () => Promise<void>) {
    messagesRef.current = messagesRef.current.then(work).catch(() => {
      setReceiveStatus("error");
      setError("Could not update the receiver’s local storage.");
    });
  }

  function discardSession() {
    const token = signalingRef.current?.session?.token;
    incomingFileRef.current = null;
    if (token) enqueueMessage(() => deleteSessionBatches(token));
  }

  function sendTransferControl(type: string, transferId: string, extra: Record<string, unknown> = {}) {
    const channel = dataChannelRef.current;
    if (channel?.readyState === "open") {
      try { channel.send(JSON.stringify({ type, transferId, ...extra })); } catch { /* The sender will retry after reconnecting. */ }
    }
  }

  function closePeerConnection() {
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

  function recoverPeer() {
    const code = sessionCodeRef.current;
    if (!code) return;
    if (expiredRef.current) {
      fail("The direct connection closed after the pipe expired. Join a new pipe to continue.");
      return;
    }
    closePeerConnection();
    setStatus("connecting");
    statusRef.current = "connecting";
    window.clearTimeout(resetTimerRef.current);
    resetTimerRef.current = window.setTimeout(() => {
      if (!peerConnectionRef.current) signal({ type: "peer-reset", code });
    }, 1500);
    startConnectionTimeout();
  }

  function fail(message: string) {
    discardSession();
    signalingRef.current?.forget();
    sessionCodeRef.current = null;
    clearTimers();
    closePeerConnection();
    setError(message);
    setPipeNotice("");
    setIncomingOffer(null);
    setReceiveStatus("waiting");
    setStatus("idle");
    statusRef.current = "idle";
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

  function setConnected() {
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
    setError("");
    setStatus("connected");
    statusRef.current = "connected";
  }

  function openDownloadedFile() {
    if (!downloadNotice) return;
    try {
      const openedWindow = window.open(downloadNotice.url, "_blank");
      if (openedWindow) {
        openedWindow.opener = null;
        setDownloadNotice({ ...downloadNotice, openBlocked: false });
      } else {
        setDownloadNotice({ ...downloadNotice, openBlocked: true });
      }
    } catch {
      setDownloadNotice({ ...downloadNotice, openBlocked: true });
    }
  }

  function clearDownloadedUrls() {
    for (const [url, timer] of downloadUrlTimersRef.current) {
      window.clearTimeout(timer);
      URL.revokeObjectURL(url);
    }
    downloadUrlTimersRef.current.clear();
    receivedFileUrlRef.current = null;
  }

  function retainDownloadedUrl(url: string) {
    receivedFileUrlRef.current = url;
    const timer = window.setTimeout(() => {
      URL.revokeObjectURL(url);
      downloadUrlTimersRef.current.delete(url);
      if (receivedFileUrlRef.current === url) receivedFileUrlRef.current = null;
      setDownloadNotice((current) => current?.url === url ? null : current);
    }, 60_000);
    downloadUrlTimersRef.current.set(url, timer);
  }

  function receiveAnotherFile() {
    clearDownloadedUrls();
    setDownloadNotice(null);
    setReceivedFile(null);
    setReceivedBytes(0);
    setReceiveStatus("waiting");
    setError("");
    setReceiveWarning("");
  }

  async function cancelReceive() {
    const transfer = incomingFileRef.current;
    if (!transfer) return;
    await saveCheckpoint(transfer.key, persisted(transfer, "cancelled"));
    incomingFileRef.current = null;
    sendTransferControl("file-cancelled", transfer.transferId);
    setReceivedBytes(0);
    setIncomingOffer(null);
    setReceiveStatus("waiting");
    setError("");
    setReceiveWarning("");
  }

  async function completeIncomingTransfer(transfer: IncomingTransfer) {
    if (transfer.savedChunk !== transfer.totalChunks) {
      sendTransferControl("chunk-ack", transfer.transferId, { nextChunk: transfer.savedChunk });
      return;
    }
    const blob = await assembleFile(transfer.key, transfer.mimeType);
    if (blob.size !== transfer.size) throw new Error("Saved data does not match the offered file size.");
    await saveCheckpoint(transfer.key, persisted(transfer, "completed"));
    incomingFileRef.current = null;
    const { name, mimeType, size, transferId } = transfer;
    const url = URL.createObjectURL(blob);
    retainDownloadedUrl(url);
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    link.style.display = "none";
    document.body.appendChild(link);
    link.click();
    link.remove();
    setReceivedFile({ name, mimeType, size, url });
    setDownloadNotice({ name, url, openBlocked: false });
    setIncomingOffer(null);
    setReceiveStatus("received");
    acknowledgeCompletedFile(transferId, size);
  }

  function acknowledgeCompletedFile(transferId: string, size: number) {
    sendTransferControl("file-received", transferId, { size });
    reportCompletedFile(transferId);
  }

  async function handleDataMessage(channel: RTCDataChannel, data: unknown) {
    if (dataChannelRef.current !== channel) return;
    if (typeof data === "string") {
      const message = JSON.parse(data);
      if (message.type === "text-message") {
        const { id, text } = message;
        if (typeof id === "string" && id && typeof text === "string" && new TextEncoder().encode(text).byteLength <= 48 * 1024) {
          setReceivedTexts((current) => [...current.slice(-29), { id, text }]);
        }
        return;
      }
      const { transferId } = message;
      if (typeof transferId !== "string" || !transferId || transferId.length > 128) throw new Error("Invalid transfer ID.");
      const key = checkpointKey(transferId);
      if (message.type === "file-start") {
        const { name, mimeType, size, totalChunks, chunkSize } = message;
        if (typeof name !== "string" || !name || typeof mimeType !== "string" ||
            !Number.isSafeInteger(size) || size < 0 || totalChunks > 0xffffffff ||
            chunkSize !== CHUNK_SIZE || totalChunks !== Math.ceil(size / CHUNK_SIZE)) {
          throw new Error("The offered file metadata is invalid.");
        }
        const saved = await loadCheckpoint(key);
        if (saved && (saved.name !== name || saved.size !== size || saved.mimeType !== mimeType ||
            saved.totalChunks !== totalChunks || saved.chunkSize !== chunkSize)) {
          throw new Error("The offered file does not match the saved checkpoint.");
        }
        if (saved?.state === "cancelled") {
          sendTransferControl("file-cancelled", transferId);
          return;
        }
        const nextChunk = saved?.nextChunk ?? 0;
        const checkpoint: Checkpoint = { transferId, name, mimeType, size, totalChunks, chunkSize, nextChunk, state: "receiving" };
        if (!saved) await saveCheckpoint(key, checkpoint);
        if (dataChannelRef.current !== channel) return;
        if (saved?.state !== "completed") {
          incomingFileRef.current = { ...checkpoint, key, chunks: [], savedChunk: nextChunk };
          setReceivedFile(null);
          setIncomingOffer({ name, mimeType, size });
          setReceivedBytes(Math.min(size, nextChunk * chunkSize));
          setReceiveStatus("receiving");
          setReceiveWarning(nextChunk ? "Resuming from the last saved chunk." : "");
          setError("");
        }
        sendTransferControl("file-checkpoint", transferId, { nextChunk });
        return;
      }
      const transfer = incomingFileRef.current;
      if (message.type === "file-cancel") {
        if (transfer?.transferId === transferId) await cancelReceive();
        else {
          const saved = await loadCheckpoint(key);
          if (saved?.state === "receiving") await saveCheckpoint(key, { ...saved, state: "cancelled" });
          sendTransferControl("file-cancelled", transferId);
        }
        return;
      }
      if (message.type === "chunk-sync" && transfer?.transferId === transferId) {
        sendTransferControl("chunk-ack", transferId, { nextChunk: transfer.savedChunk });
      } else if (message.type === "file-end") {
        if (transfer?.transferId === transferId) {
          if (message.totalChunks !== transfer.totalChunks || message.size !== transfer.size) throw new Error("Invalid file completion metadata.");
          await completeIncomingTransfer(transfer);
        } else {
          const saved = await loadCheckpoint(key);
          if (saved?.state === "completed") acknowledgeCompletedFile(transferId, saved.size);
        }
      }
      return;
    }

    if (!(data instanceof ArrayBuffer) || data.byteLength < CHUNK_FRAME_HEADER_BYTES) throw new Error("Invalid file chunk.");
    const header = new DataView(data);
    const idLength = header.getUint16(0);
    const payloadOffset = CHUNK_FRAME_HEADER_BYTES + idLength;
    if (!idLength || payloadOffset > data.byteLength) throw new Error("Invalid chunk header.");
    const transferId = new TextDecoder("utf-8", { fatal: true }).decode(new Uint8Array(data, CHUNK_FRAME_HEADER_BYTES, idLength));
    const transfer = incomingFileRef.current;
    // Ignore messages already in flight after a cancellation.
    if (transfer?.transferId !== transferId) return;
    const index = header.getUint32(2);
    if (index < transfer.nextChunk) {
      sendTransferControl("chunk-ack", transferId, { nextChunk: transfer.savedChunk });
      return;
    }
    const bytes = Math.min(transfer.chunkSize, transfer.size - index * transfer.chunkSize);
    if (index !== transfer.nextChunk || index >= transfer.totalChunks || data.byteLength - payloadOffset !== bytes) {
      throw new Error("Received a file chunk out of sequence or with an invalid size.");
    }
    transfer.chunks.push(data.slice(payloadOffset));
    transfer.nextChunk += 1;
    if (transfer.chunks.length === ACK_BATCH_SIZE || transfer.nextChunk === transfer.totalChunks) {
      await saveCheckpoint(transfer.key, persisted(transfer, "receiving"), new Blob(transfer.chunks));
      transfer.savedChunk = transfer.nextChunk;
      transfer.chunks.length = 0;
      setReceivedBytes(Math.min(transfer.size, transfer.savedChunk * transfer.chunkSize));
      if (dataChannelRef.current === channel) sendTransferControl("chunk-ack", transferId, { nextChunk: transfer.savedChunk });
    }
  }

  function setupDataChannel(channel: RTCDataChannel) {
    dataChannelRef.current = channel;
    let readySent = false;
    console.info("[DropHut receiver] Incoming DataChannel", channel.label, channel.readyState);
    const announceReceiverReady = () => {
      if (readySent || dataChannelRef.current !== channel || channel.readyState !== "open") return;
      readySent = true;
      // No transfer exists yet; file-start will synchronously initialize incomingFileRef.
      console.info("[DropHut receiver] DataChannel open");
      setConnected();
      try {
        channel.send(JSON.stringify({ type: "receiver-ready" }));
        console.info("[DropHut receiver] Sent receiver-ready");
      } catch {
        readySent = false;
        recoverPeer();
      }
    };
    channel.onopen = announceReceiverReady;
    channel.binaryType = "arraybuffer";
    channel.onmessage = (event) => {
      enqueueMessage(async () => {
        try {
          await handleDataMessage(channel, event.data);
        } catch (reason) {
          if (dataChannelRef.current !== channel) return;
          const transfer = incomingFileRef.current;
          let transferId = transfer?.transferId;
          if (typeof event.data === "string") {
            try { transferId = JSON.parse(event.data).transferId ?? transferId; } catch { /* Invalid control message. */ }
          }
          const message = reason instanceof DOMException && reason.name === "QuotaExceededError"
            ? "The receiver ran out of local storage. Free up space and retry the file."
            : reason instanceof Error ? reason.message : "Could not save the file on this device.";
          if (transferId) sendTransferControl("file-error", transferId, { message });
          if (transfer && transfer.transferId === transferId) {
            incomingFileRef.current = null;
            await saveCheckpoint(transfer.key, persisted(transfer, "cancelled")).catch(() => {});
          }
          setError(message);
          setReceiveStatus("error");
        }
      });
    };
    channel.onclose = () => {
      console.info("[DropHut receiver] DataChannel state: closed");
      if (dataChannelRef.current === channel) recoverPeer();
    };
    channel.onerror = (event) => {
      console.error("[DropHut receiver] DataChannel error", event);
      if (dataChannelRef.current === channel) recoverPeer();
    };
    if (channel.readyState === "open") announceReceiverReady();
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
        signal({ type: "webrtc-ice", code: pipeCode, candidate: event.candidate.toJSON() });
      } else {
        console.info("[DropHut receiver] ICE candidate gathering complete");
      }
    };
    peer.oniceconnectionstatechange = () => {
      console.info("[DropHut receiver] ICE connection state:", peer.iceConnectionState);
      if (peer.iceConnectionState === "failed" && peerConnectionRef.current === peer) recoverPeer();
      if (peer.iceConnectionState === "disconnected") {
        if (statusRef.current === "connected") { setStatus("connecting"); statusRef.current = "connecting"; }
        startConnectionTimeout();
      }
      if (peer.iceConnectionState === "closed" && peerConnectionRef.current === peer) recoverPeer();
    };
    peer.onconnectionstatechange = () => {
      console.info("[DropHut receiver] Peer connection state:", peer.connectionState);
      if (peer.connectionState === "failed" && peerConnectionRef.current === peer) recoverPeer();
      if (peer.connectionState === "closed" && peerConnectionRef.current === peer) recoverPeer();
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
    window.clearTimeout(resetTimerRef.current);
    if (peerConnectionRef.current) closePeerConnection();
    const peer = setupPeerConnection(pipeCode);
    setStatus("connecting");
    statusRef.current = "connecting";
    startConnectionTimeout();

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
      if (peerConnectionRef.current !== peer || !local) return;
      signal({ type: "webrtc-answer", code: pipeCode, answer: { type: local.type, sdp: local.sdp } });
      console.info("[DropHut receiver] Sent SDP answer through signaling WebSocket");
    } catch (reason) {
      if (peerConnectionRef.current !== peer) return;
      fail(reason instanceof Error ? reason.message : "Could not answer the WebRTC offer.");
    }
  }

  function updateCode(event: ChangeEvent<HTMLInputElement>) {
    setCode(event.target.value.replace(/\D/g, "").slice(0, 6));
    setError("");
  }

  function handleSignal(message: Signal) {
    const pipeCode = sessionCodeRef.current;
    if (!pipeCode) return;
    if (message.type === "pipe-expired") {
      if (canKeepChannel()) {
        expiredRef.current = true;
        signalingRef.current?.retire();
        setSignalingStatus("connected");
        setPipeNotice("The pipe’s reconnect window expired. The current direct connection is still active.");
      } else {
        fail("This pipe expired. Join a new pipe to continue.");
      }
    } else if (message.type === "pipe-closed") {
      fail("The other device closed the pipe.");
    } else if (message.type === "error") {
      fail(message.message || "The signaling server reported an error.");
    } else if (message.type === "peer-away") {
      setPipeNotice("The sender’s signaling connection dropped. Their place is reserved while they reconnect.");
    } else if (message.type === "peer-ready") {
      setPipeNotice("");
    } else if (message.type === "pipe-joined" || message.type === "pipe-resumed") {
      if (!channelHealthy()) {
        setStatus("connecting");
        statusRef.current = "connecting";
        startConnectionTimeout();
      }
    } else if (message.type === "webrtc-offer" && message.offer) {
      void handleOffer(pipeCode, message.offer);
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
    const connection = new PipeSignaling("receiver", setSignalingStatus, handleSignal, () => !canKeepChannel(), checkPeer);
    signalingRef.current = connection;
    return connection;
  }

  useEffect(() => {
    const connection = startSignaling();
    void deleteSessionBatches("").catch(() => {});
    if (connection.session) {
      sessionCodeRef.current = connection.session.code;
      statusRef.current = "joining";
    }
    if (connection.session) connection.open();
    return () => {
      signalingRef.current?.stop();
      sessionCodeRef.current = null;
      clearTimers();
      closePeerConnection();
      clearDownloadedUrls();
    };
    // Socket callbacks use refs to read current connection and transfer state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function joinPipe() {
    if (!/^\d{6}$/.test(code) || signalingRef.current?.session) return;
    const connection = startSignaling();
    setError("");
    setStatus("joining");
    statusRef.current = "joining";
    sessionCodeRef.current = code;
    connection.register(code);
  }

  function leavePipe() {
    discardSession();
    const connection = signalingRef.current;
    if (connection?.session) connection.send({ type: "leave-pipe", code: connection.session.code });
    connection?.forget();
    onHome();
  }

  return (
    <main className="app-shell flow-shell">
      <header className="topbar">
        <button className="brand-button" onClick={leavePipe}><span className="brand-mark small" aria-hidden="true">D</span>Drophut</button>
        <button className="text-button" onClick={leavePipe}>← Home</button>
      </header>
      <section className="flow-content" aria-labelledby="join-title">
        <p className="eyebrow">RECEIVE FILES</p>
        {receivedFile ? (
          <div className="pipe-card receiver-card">
            <div className="connected-label"><span className="status-dot" />File received ✓</div>
            <h1 id="join-title">{receivedFile.name}</h1>
            <p className="pipe-hint">{formatFileSize(receivedFile.size)} · {receivedFile.mimeType}</p>
            <p>Your browser started the download.</p>
            <button className="button button-secondary button-full" onClick={receiveAnotherFile}>Receive another file</button>
          </div>
        ) : status === "connected" ? (
          <div className="pipe-card receiver-card">
            <div className="connected-label"><span className="status-dot" />Connected ✓</div>
            <h1 id="join-title">"Connected to pipe"</h1>
            {incomingOffer ? <p className="pipe-hint">{incomingOffer.name} · {formatFileSize(incomingOffer.size)}</p> : null}
            <p className="pipe-hint">{receiveStatus === "receiving" ? "Receiving file…" : receiveStatus === "error" ? "Could not receive the file." : "Waiting for the sender to send a file."}</p>
            {receiveStatus === "receiving" && incomingOffer && <p>{formatFileSize(receivedBytes)} / {formatFileSize(incomingOffer.size)} · {incomingOffer.size ? Math.floor(receivedBytes / incomingOffer.size * 100) : 0}%</p>}
            {receiveWarning && <p className="transfer-note">{receiveWarning}</p>}
            {receiveStatus === "receiving" && <button className="button button-secondary button-full" onClick={() => enqueueMessage(cancelReceive)}>Cancel transfer</button>}
            {error && <p className="error-message" role="alert">{error}</p>}
          </div>
        ) : status === "connecting" ? (
          <div className="pipe-card receiver-card">
            <div className="waiting-line"><span className="status-dot pulse" />Connecting directly…</div>
            <p className="pipe-hint">{receiveStatus === "receiving" ? "Transfer paused. Reconnecting to the sender…" : "Setting up a private connection to the sender."}</p>
            {receiveStatus === "receiving" && <button className="button button-secondary button-full" onClick={() => enqueueMessage(cancelReceive)}>Cancel transfer</button>}
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
      {receivedTexts.length > 0 && (
        <div className="received-text-list" aria-live="polite">
          <h2>Messages from sender</h2>
          {receivedTexts.map((message) => <article className="received-text-card" key={message.id}>{message.text}</article>)}
        </div>
      )}
      {signalingStatus === "retrying" && (
        <p className="signaling-note" role="status">
          {status === "connected" ? "Signaling reconnecting; the direct connection is still active." : "Signaling reconnecting; keeping this pipe session available."}
        </p>
      )}
        {pipeNotice && <p role="status">{pipeNotice}</p>}
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
      {downloadNotice && (
        <aside className="download-toast" role="status" aria-live="polite">
          <span>{downloadNotice.openBlocked ? "Download started. Your browser blocked opening the file." : `${downloadNotice.name} downloaded`}</span>
          <button type="button" onClick={openDownloadedFile}>Open</button>
        </aside>
      )}
    </main>
  );
}

export default JoinPipe;
