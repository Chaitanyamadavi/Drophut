import { useHomepageStats } from "./useHomepageStats";

type HomeProps = {
  onSend: () => void;
  onJoin: () => void;
};

function Home({ onSend, onJoin }: HomeProps) {
  const { stats, loading } = useHomepageStats();
  const date = stats && new Intl.DateTimeFormat("en-GB", {
    day: "numeric", month: "short", year: "numeric", timeZone: "UTC",
  }).format(new Date(stats.visitorsSince));

  return (
    <main className="app-shell home-shell">
      <header className="home-topbar">
        <div className="home-wordmark"><span className="brand-mark small" aria-hidden="true">D</span>Drophut</div>
        <a className="home-numbers-nav" href="#drophut-numbers">Our numbers <span aria-hidden="true">↗</span></a>
      </header>

      <section className="home-hero" aria-labelledby="home-title">
        <div className="home-hero-copy">
          <p className="home-kicker"><span aria-hidden="true" /> DEVICE TO DEVICE FILE SHARING</p>
          <h1 id="home-title">Your files.<br /><span>Right over there.</span></h1>
          <p className="home-hero-description">Open Drophut on two devices. Make a pipe, enter the code, and send your files directly.</p>
          <div className="home-hero-steps" aria-label="Three steps to share">
            <span>01 <strong>Pick files</strong></span>
            <span>02 <strong>Share code</strong></span>
            <span>03 <strong>Send</strong></span>
          </div>
        </div>

        <div className="home-transfer-panel">
          <div className="home-panel-top"><span>READY WHEN YOU ARE</span><span className="home-panel-signal" aria-hidden="true"><i /><i /><i /></span></div>
          <div className="home-panel-icon" aria-hidden="true">↗</div>
          <h2>Start a transfer.</h2>
          <p>Choose how you want to connect your devices.</p>
          <div className="home-actions">
            <button className="button button-primary button-large" onClick={onSend}>Send Files <span aria-hidden="true">→</span></button>
            <button className="button button-secondary button-large" onClick={onJoin}>Join a Pipe <span aria-hidden="true">→</span></button>
          </div>
          <div className="home-panel-bottom">No account needed <span aria-hidden="true">·</span> Just a six-digit code</div>
        </div>
      </section>

      <section className="home-numbers" id="drophut-numbers" aria-labelledby="home-numbers-title">
        <div className="home-numbers-inner">
          <p className="home-numbers-kicker">DROPHUT IN NUMBERS <span aria-hidden="true">↘</span></p>
          <h2 id="home-numbers-title">Good things <em>move.</em></h2>
          <p className="home-numbers-description">A growing number of files and people connecting across devices.</p>
          {stats ? (
            <div className="home-numbers-grid">
              <div className="home-number">
                <strong>{stats.filesTransferred.toLocaleString("en-GB")}{stats.filesTransferred === 1000 ? "+" : ""}</strong>
                <span>Files transferred</span>
                <p>Includes an estimated 1,000 earlier transfers. New completed files are counted individually.</p>
              </div>
              <div className="home-number">
                <strong>{stats.uniqueVisitors.toLocaleString("en-GB")}</strong>
                <span>Unique visitors</span>
                <p>Counted from {date} using anonymous browser IDs.</p>
              </div>
            </div>
          ) : (
            <p className="home-numbers-status" role="status">{loading ? "Loading the latest numbers…" : "Numbers are unavailable right now. File transfers still work."}</p>
          )}
        </div>
      </section>

      <p className="home-footnote">Peer-to-peer transfers, connected through signaling.</p>
    </main>
  );
}

export default Home;
