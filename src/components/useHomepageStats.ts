import { useEffect, useState } from "react";
import { registerVisitor, requestStats, type Stats } from "./statsClient";

export function useHomepageStats() {
  const [stats, setStats] = useState<Stats | null>(null);

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
    void requestStats().then(display).catch(() => {});
    void registerVisitor().then(display);
    return () => { active = false; };
  }, []);

  return stats;
}
