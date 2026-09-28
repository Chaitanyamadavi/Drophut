import { useEffect, useRef } from "react";

type TermsModalProps = {
  onAccept: () => void;
  onClose: () => void;
};

function TermsModal({ onAccept, onClose }: TermsModalProps) {
  const acceptButtonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    acceptButtonRef.current?.focus();
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") onClose();
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  return (
    <div
      className="terms-overlay"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <section className="terms-modal" role="dialog" aria-modal="true" aria-labelledby="terms-title" aria-describedby="terms-intro">
        <header className="terms-header">
          <div>
            <p className="terms-kicker">BEFORE YOU CONNECT</p>
            <h2 id="terms-title">Terms &amp; Privacy</h2>
          </div>
          <button className="terms-close" type="button" onClick={onClose} aria-label="Close terms and privacy">×</button>
        </header>
        <div className="terms-body">
          <p id="terms-intro">A few things to know before using Drophut:</p>
          <ul>
            <li>Drophut uses WebRTC to transfer file contents directly between connected devices through a DataChannel.</li>
            <li>The signaling server helps peers establish a connection and relays signaling information. It may observe network metadata needed to provide that service.</li>
            <li>File contents travel through the WebRTC DataChannel; they are not uploaded to the signaling server.</li>
            <li>Pipe and signaling state is temporary server-side memory and is not persisted.</li>
            <li>Connectivity depends on NAT, firewall, and network conditions; a direct connection is not always possible.</li>
            <li>You are responsible for the files and other data you choose to transfer.</li>
          </ul>
        </div>
        <footer className="terms-actions">
          <button className="button button-secondary" type="button" onClick={onClose}>Cancel</button>
          <button className="button button-primary" type="button" onClick={onAccept} ref={acceptButtonRef}>I Agree &amp; Continue</button>
        </footer>
      </section>
    </div>
  );
}

export default TermsModal;
