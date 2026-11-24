import { useEffect, useState } from "react";
import { registerVisitor, requestStats, type Stats } from "./statsClient";

export function useHomepageStats() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    function display(next: Stats | undefined) {
      if (!active || !next) return;
      setStats((current) => ({
        ...next,
        filesTransferred: Math.max(current?.filesTransferred ?? 0, next.filesTransferred),
        uniqueVisitors: Math.max(current?.uniqueVisitors ?? 0, next.uniqueVisitors),
      }));
    }
    void Promise.allSettled([
      requestStats().then(display),
      registerVisitor().then(display),
    ]).then(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, []);

  return { stats, loading };
}
