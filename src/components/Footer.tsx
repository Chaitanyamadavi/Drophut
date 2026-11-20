function Footer() {
  return (
    <footer className="site-footer">
      <div className="site-footer-inner">
        <p className="footer-credit">
          Made with <span className="footer-heart" aria-label="love">♥</span> by{" "}
          <a href="https://github.com/Chaitanyamadavi" target="_blank" rel="noreferrer">Chaitanya</a>
        </p>
        <nav className="footer-social" aria-label="Follow me">
          <span className="footer-social-label">Follow me</span>
          <a href="https://x.com/Chaitanyatwts" target="_blank" rel="noreferrer" aria-label="Follow on X (Twitter)">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M18.9 2H22l-6.78 7.75L23.2 22h-6.25l-4.9-7.4L5.57 22H2.45l7.25-8.29L1.8 2h6.4l4.43 6.76L18.9 2Zm-1.1 18h1.73L7.28 3.89H5.42L17.8 20Z" /></svg>
          </a>
          <a href="https://linkedin.com/in/chaitanyamadavi/" target="_blank" rel="noreferrer" aria-label="Connect on LinkedIn">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.45 2H3.55C2.69 2 2 2.68 2 3.52v16.96c0 .84.69 1.52 1.55 1.52h16.9c.86 0 1.55-.68 1.55-1.52V3.52c0-.84-.69-1.52-1.55-1.52ZM7.93 18.7H4.98V9.2h2.95v9.5ZM6.46 7.9a1.71 1.71 0 1 1 .02-3.42 1.71 1.71 0 0 1-.02 3.42ZM19.02 18.7h-2.95v-4.62c0-1.1-.02-2.51-1.53-2.51-1.53 0-1.77 1.2-1.77 2.43v4.7H9.82V9.2h2.83v1.3h.04c.4-.75 1.36-1.53 2.8-1.53 3 0 3.53 1.97 3.53 4.53v5.2Z" /></svg>
          </a>
          <a href="https://github.com/Chaitanyamadavi" target="_blank" rel="noreferrer" aria-label="View GitHub profile">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 .9a11.1 11.1 0 0 0-3.51 21.63c.56.1.76-.24.76-.54v-2.1c-3.1.68-3.76-1.31-3.76-1.31-.51-1.3-1.25-1.65-1.25-1.65-1.02-.7.08-.69.08-.69 1.13.08 1.73 1.16 1.73 1.16 1 .1.74 2.47 3.29 1.75.1-.73.4-1.23.72-1.51-2.48-.28-5.09-1.24-5.09-5.52 0-1.22.44-2.22 1.16-3-.12-.29-.5-1.42.11-2.96 0 0 .95-.3 3.05 1.15a10.6 10.6 0 0 1 5.55 0c2.1-1.45 3.04-1.15 3.04-1.15.61 1.54.23 2.67.11 2.96.73.78 1.16 1.78 1.16 3 0 4.29-2.61 5.23-5.1 5.51.4.35.76 1.03.76 2.08v3.08c0 .3.2.65.77.54A11.1 11.1 0 0 0 12 .9Z" /></svg>
          </a>
        </nav>
      </div>
    </footer>
  );
}

export default Footer;
