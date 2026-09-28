import { useState } from "react";
import CreatePipe from "./components/CreatePipe";
import JoinPipe from "./components/JoinPipe";
import Home from "./components/Home";

type Page = "home" | "send" | "join";

function App() {
  const [page, setPage] = useState<Page>("home");

  if (page === "send") {
    return <CreatePipe onHome={() => setPage("home")} />;
  }
  if (page === "join") {
    return <JoinPipe onHome={() => setPage("home")} />;
  }

  return <Home onSend={() => setPage("send")} onJoin={() => setPage("join")} />;
}

export default App;
