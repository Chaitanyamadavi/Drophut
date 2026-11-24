import { useHomepageStats } from "./useHomepageStats";

type HomeProps = {
  onSend: () => void;
  onJoin: () => void;
  onStats: () => void;
};

function Home({ onSend, onJoin, onStats }: HomeProps) {
  const { stats } = useHomepageStats();
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
        <button className="home-stats-link" type="button" onClick={onStats}>
          <span className="home-stats-link-icon" aria-hidden="true">↗</span>
          <span>{stats ? `${stats.filesTransferred.toLocaleString("en-GB")}+ files shared` : "See Drophut stats"}</span>
          <span className="home-stats-link-arrow" aria-hidden="true">→</span>
        </button>
      </section>
      <p className="home-footnote">Peer-to-peer transfers, connected through signaling.</p>
    </main>
  );
}

export default Home;
