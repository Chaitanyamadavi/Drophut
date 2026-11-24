import { useHomepageStats } from "./useHomepageStats";

type HomeProps = {
  onSend: () => void;
  onJoin: () => void;
};

function Home({ onSend, onJoin }: HomeProps) {
  const stats = useHomepageStats();
  const date = stats && new Intl.DateTimeFormat("en-GB", {
    day: "numeric", month: "short", year: "numeric", timeZone: "UTC",
  }).format(new Date(stats.visitorsSince));
  return (
    <main className="app-shell home-shell">
      <section className="home-content" aria-labelledby="home-title">
        <div className="brand-mark" aria-hidden="true">D</div>
        <h1 id="home-title">Drophut</h1>
        <p className="tagline">A simple way to share files between devices.</p>
        <div className="home-actions">
          <button className="button button-primary button-large" onClick={onSend}>Send Files</button>
          <button className="button button-secondary button-large" onClick={onJoin}>Join a Pipe</button>
        </div>
        <div className="home-stats">
          {stats && (
            <>
              <p className="home-stats-line">
                <span>{stats.filesTransferred.toLocaleString("en-GB")} files transferred</span>
                <span>{stats.uniqueVisitors.toLocaleString("en-GB")} {stats.uniqueVisitors === 1 ? "unique visitor" : "unique visitors"} since {date}</span>
              </p>
              <p className="home-stats-note">Includes an estimated 1,000 earlier transfers.</p>
            </>
          )}
        </div>
      </section>
      <p className="home-footnote">Peer-to-peer transfers, connected through signaling.</p>
    </main>
  );
}

export default Home;
