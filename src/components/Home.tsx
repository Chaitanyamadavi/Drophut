type HomeProps = {
  onSend: () => void;
  onJoin: () => void;
};

function Home({ onSend, onJoin }: HomeProps) {
  return (
    <main className="app-shell home-shell">
      <section className="home-content" aria-labelledby="home-title">
        <div className="brand-mark" aria-hidden="true">D</div>
        <h1 id="home-title">Drophut</h1>
        <p className="tagline">Transfer anything, anywhere.</p>
        <div className="home-actions">
          <button className="button button-primary button-large" onClick={onSend}>Send Files</button>
          <button className="button button-secondary button-large" onClick={onJoin}>Join a Pipe</button>
        </div>
      </section>
      <p className="home-footnote">Private, direct sharing between your devices.</p>
    </main>
  );
}

export default Home;
