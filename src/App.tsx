import { useState } from "react";
import CreatePipe from "./components/CreatePipe";
import JoinPipe from "./components/JoinPipe";
import Home from "./components/Home";
import TermsModal from "./components/TermsModal";

type Page = "home" | "send" | "join";
const CONSENT_KEY = "drophut-terms-accepted";

function App() {
  const [page, setPage] = useState<Page>("home");
  const [pendingPage, setPendingPage] = useState<Page | null>(null);
  const [termsOpen, setTermsOpen] = useState(false);

  function enterFlow(nextPage: "send" | "join") {
    try {
      if (sessionStorage.getItem(CONSENT_KEY) === "accepted") {
        setPage(nextPage);
        return;
      }
    } catch {
      // If session storage is unavailable, require consent for this attempt.
    }
    setPendingPage(nextPage);
    setTermsOpen(true);
  }

  function acceptTerms() {
    try {
      sessionStorage.setItem(CONSENT_KEY, "accepted");
    } catch {
      // Consent still applies for this navigation if storage is unavailable.
    }
    if (pendingPage) setPage(pendingPage);
    setPendingPage(null);
    setTermsOpen(false);
  }

  function cancelTerms() {
    setPendingPage(null);
    setTermsOpen(false);
  }

  let content;
  if (page === "send") {
    content = <CreatePipe onHome={() => setPage("home")} />;
  } else if (page === "join") {
    content = <JoinPipe onHome={() => setPage("home")} />;
  } else {
    content = <Home onSend={() => enterFlow("send")} onJoin={() => enterFlow("join")} />;
  }

  return (
    <>
      {content}
      {termsOpen && <TermsModal onAccept={acceptTerms} onClose={cancelTerms} />}
    </>
  );
}

export default App;
