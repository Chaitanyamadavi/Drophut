export type Stats = { filesTransferred: number; uniqueVisitors: number; visitorsSince: string };

const VISITOR_KEY = "drophut-visitor-id";
const VISITOR_PATTERN = /^[a-f0-9]{32}$/;
const reports = new Set<string>();

function statsUrl(route: string) {
  const signaling = import.meta.env?.VITE_SIGNALING_URL ||
    (import.meta.env?.DEV ? "ws://localhost:8080" : "wss://drophut.onrender.com");
  const base = import.meta.env?.VITE_STATS_URL || signaling.replace(/^ws/, "http");
  return new URL("/api/stats" + route, base);
}

export async function requestStats(route = "", id?: string): Promise<Stats> {
  const response = await fetch(statsUrl(route), {
    signal: AbortSignal.timeout(5000),
    ...(id ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id }) } : {}),
  });
  if (!response.ok) throw new Error("Homepage stats unavailable");
  const stats: Stats = await response.json();
  if (!Number.isSafeInteger(stats?.filesTransferred) || stats.filesTransferred < 1000 ||
      !Number.isSafeInteger(stats.uniqueVisitors) || stats.uniqueVisitors < 0 ||
      !/^\d{4}-\d{2}-\d{2}$/.test(stats.visitorsSince) ||
      !Number.isFinite(Date.parse(stats.visitorsSince))) {
    throw new Error("Invalid homepage stats");
  }
  return stats;
}

function createVisitorId() {
  return Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

// Serialize first-time ID creation across tabs with an IndexedDB write transaction.
function getInitialVisitorId(): Promise<string | undefined> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("drophut-visitor", 1);
    request.onupgradeneeded = () => request.result.createObjectStore("identity");
    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      const database = request.result;
      const transaction = database.transaction("identity", "readwrite");
      const store = transaction.objectStore("identity");
      const read = store.get(VISITOR_KEY);
      let id: string | undefined;
      transaction.onabort = () => { database.close(); resolve(undefined); };
      transaction.oncomplete = () => { database.close(); resolve(id); };
      read.onsuccess = () => {
        id = typeof read.result === "string" && VISITOR_PATTERN.test(read.result) ? read.result : createVisitorId();
        store.put(id, VISITOR_KEY);
      };
    };
  });
}

export async function getVisitorId(): Promise<string | undefined> {
  try {
    const stored = localStorage.getItem(VISITOR_KEY);
    if (stored && VISITOR_PATTERN.test(stored)) return stored;
    const id = await getInitialVisitorId();
    if (!id) return;
    localStorage.setItem(VISITOR_KEY, id);
    return id;
  } catch {
    return undefined;
  }
}

let visitorRegistration: Promise<Stats | undefined> | undefined;

export function registerVisitor() {
  visitorRegistration ??= getVisitorId().then((id) => id ? requestStats("/visitors", id) : undefined)
    .catch(() => undefined);
  return visitorRegistration;
}

export function reportCompletedFile(transferId: string) {
  if (reports.has(transferId)) return;
  reports.add(transferId);
  void (async () => {
    try {
      for (const delay of [0, 1000, 3000]) {
        await new Promise<void>((resolve) => setTimeout(resolve, delay));
        try {
          await requestStats("/transfers", transferId);
          return;
        } catch {
          // Replay the same ID on the next attempt.
        }
      }
    } finally {
      reports.delete(transferId);
    }
  })();
}
