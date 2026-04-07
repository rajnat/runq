import type { SessionState } from "./types";

const sessionKey = "runq.ops-console.session";

export function loadSession(): SessionState | null {
  const raw = window.sessionStorage.getItem(sessionKey);
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as SessionState;
    if (!parsed.apiBaseUrl || !parsed.token || !parsed.principal?.role) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export function saveSession(session: SessionState): void {
  window.sessionStorage.setItem(sessionKey, JSON.stringify(session));
}

export function clearSession(): void {
  window.sessionStorage.removeItem(sessionKey);
}

export function normalizeBaseUrl(value: string): string {
  return value.trim().replace(/\/+$/, "");
}
