import { useHomepageStats } from "./useHomepageStats";

type StatsPageProps = { onHome: () => void };

function StatsPage({ onHome }: StatsPageProps) {
  const { stats, loading } = useHomepageStats();
  const date = stats && new Intl.DateTimeFormat("en-GB", {
    day: "numeric", month: "short", year: "numeric", timeZone: "UTC",
  }).format(new Date(stats.visitorsSince));

  return (
    <main className="app-shell stats-shell">
      <header className="stats-topbar">
        <button className="brand-button" type="button" onClick={onHome} aria-label="Back to Drophut home">
          <span className="brand-mark small" aria-hidden="true">D</span>Drophut
        </button>
        <button className="stats-back" type="button" onClick={onHome}>← Back to transfers</button>
      </header>

      <section className="stats-content" aria-labelledby="stats-title">
        <div className="stats-intro">
          <span className="stats-eyebrow"><span className="stats-live-dot" aria-hidden="true" /> DROPHUT IN NUMBERS</span>
          <h1 id="stats-title">Made for sharing.<br /><span>Built for everyone.</span></h1>
          <p>Files move directly between your devices. Here’s how Drophut has been used so far.</p>
        </div>

        {stats ? (
          <div className="stats-grid">
            <article className="stats-card stats-card-featured">
              <div className="stats-card-top"><span>01 / TRANSFERS</span><span aria-hidden="true">↗</span></div>
              <div>
                <p className="stats-number">{stats.filesTransferred.toLocaleString("en-GB")}{stats.filesTransferred === 1000 ? "+" : ""}</p>
                <h2>Files transferred</h2>
                <p className="stats-card-caption">Shared device to device, without storing the files on our server.</p>
              </div>
            </article>
            <article className="stats-card stats-card-visitors">
              <div className="stats-card-top"><span>02 / COMMUNITY</span><span aria-hidden="true">◎</span></div>
              <div>
                <p className="stats-number">{stats.uniqueVisitors.toLocaleString("en-GB")}</p>
                <h2>Unique visitors</h2>
                <p className="stats-card-caption">Counted from {date} across browsers visiting Drophut.</p>
              </div>
            </article>
          </div>
        ) : loading ? (
          <div className="stats-unavailable" role="status">Loading the latest numbers…</div>
        ) : (
          <div className="stats-unavailable" role="status">Stats are unavailable right now. You can still send and receive files.</div>
        )}

        <div className="stats-bottom">
          <p>Transfer total includes an estimated 1,000 earlier files. New completed transfers are counted separately.</p>
          <button className="stats-cta" type="button" onClick={onHome}>Start sharing <span aria-hidden="true">→</span></button>
        </div>
      </section>
    </main>
  );
}

export default StatsPage;
