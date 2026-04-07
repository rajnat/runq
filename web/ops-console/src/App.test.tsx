import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { BrowserRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import App from "./App";

const authMeResponse = { role: "tenant", tenant_id: "tenant-api" };
const runsResponse = {
  runs: [
    {
      id: "run-failed-1",
      job_id: "job-1",
      job_name: "failed job",
      tenant_id: "tenant-api",
      queue: "default",
      kind: "http",
      schedule_type: "once",
      job_disabled: false,
      status: "FAILED",
      attempt: 1,
      scheduled_at: "2026-04-02T12:00:00Z",
      available_at: "2026-04-02T12:00:00Z",
      completed_at: "2026-04-02T12:02:00Z",
      lease_token: 0,
      worker_id: "worker-1",
    },
  ],
};
const runDetailResponse = {
  run: runsResponse.runs[0],
  events: [
    {
      event_type: "RUN_FAILED",
      event_time: "2026-04-02T12:02:00Z",
      actor_type: "worker",
      actor_id: "worker-1",
      payload: { error_code: "HTTP_500" },
    },
  ],
};

describe("ops console", () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    window.sessionStorage.clear();
    window.history.replaceState({}, "", "/");
  });

  afterEach(() => {
    cleanup();
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  test("signs in and loads tenant-scoped failed runs", async () => {
    globalThis.fetch = vi.fn(async (input) => {
      const url = String(input);
      if (url.endsWith("/v1/auth/me")) {
        return jsonResponse(authMeResponse);
      }
      if (url.includes("/v1/runs?")) {
        expect(url).toContain("tenant_id=tenant-api");
        expect(url).toContain("status=FAILED%2CTIMED_OUT");
        return jsonResponse(runsResponse);
      }
      throw new Error(`unexpected request ${url}`);
    }) as typeof fetch;

    renderApp();

    await userEvent.type(screen.getByLabelText(/bearer token/i), "tenant-token");
    await userEvent.click(screen.getByRole("button", { name: /sign in/i }));

    expect(await screen.findByText("failed job")).toBeInTheDocument();
    expect(screen.getByText(/tenant scope/i)).toHaveTextContent("tenant-api");
  });

  test("renders run detail and event history", async () => {
    primeSession();
    globalThis.fetch = vi.fn(async (input) => {
      const url = String(input);
      if (url.includes("/v1/runs/run-failed-1")) {
        return jsonResponse(runDetailResponse);
      }
      throw new Error(`unexpected request ${url}`);
    }) as typeof fetch;

    window.history.pushState({}, "", "/runs/run-failed-1");
    renderApp();

    expect(await screen.findByRole("heading", { name: "failed job" })).toBeInTheDocument();
    expect(screen.getByText("RUN_FAILED")).toBeInTheDocument();
  });

  test("requeues a run from the detail page", async () => {
    primeSession();
    globalThis.fetch = vi.fn(async (input, init) => {
      const url = String(input);
      if (url.includes("/v1/runs/run-failed-1/requeue")) {
        expect(init?.method).toBe("POST");
        return jsonResponse({
          run_id: "run-failed-2",
          status: "accepted",
          job_id: "job-1",
          source: "manual_requeue",
          from_run: "run-failed-1",
        });
      }
      if (url.includes("/v1/runs/run-failed-2")) {
        return jsonResponse({
          run: { ...runDetailResponse.run, id: "run-failed-2", status: "PENDING", completed_at: undefined },
          events: [
            {
              event_type: "RUN_CREATED",
              event_time: "2026-04-02T12:03:00Z",
              actor_type: "api",
              payload: { source_run_id: "run-failed-1" },
            },
          ],
        });
      }
      if (url.includes("/v1/runs/run-failed-1")) {
        return jsonResponse(runDetailResponse);
      }
      throw new Error(`unexpected request ${url}`);
    }) as typeof fetch;

    window.history.pushState({}, "", "/runs/run-failed-1");
    renderApp();

    await screen.findByRole("heading", { name: "failed job" });
    await userEvent.click(screen.getByRole("button", { name: /requeue run/i }));
    await userEvent.click(screen.getAllByRole("button", { name: /^requeue run$/i })[1]);

    await waitFor(() => {
      expect(window.location.pathname).toBe("/runs/run-failed-2");
    });
  });

  test("cancels the owning job from the detail page", async () => {
    primeSession();
    let detailCalls = 0;
    globalThis.fetch = vi.fn(async (input, init) => {
      const url = String(input);
      if (url.includes("/v1/jobs/job-1/cancel")) {
        expect(init?.method).toBe("POST");
        return jsonResponse({ job_id: "job-1", status: "cancel_requested", canceled_runs: 1 });
      }
      if (url.includes("/v1/runs/run-failed-1")) {
        detailCalls += 1;
        return jsonResponse({
          ...runDetailResponse,
          run: {
            ...runDetailResponse.run,
            job_disabled: detailCalls > 1,
          },
        });
      }
      throw new Error(`unexpected request ${url}`);
    }) as typeof fetch;

    window.history.pushState({}, "", "/runs/run-failed-1");
    renderApp();

    await screen.findByRole("heading", { name: "failed job" });
    await userEvent.click(screen.getByRole("button", { name: /cancel job/i }));
    await userEvent.click(screen.getAllByRole("button", { name: /^cancel job$/i })[1]);

    await waitFor(() => {
      expect(screen.getByText("No")).toBeInTheDocument();
    });
  });
});

function renderApp() {
  return render(
    <BrowserRouter>
      <App />
    </BrowserRouter>,
  );
}

function primeSession() {
  window.sessionStorage.setItem(
    "runq.ops-console.session",
    JSON.stringify({
      apiBaseUrl: "http://localhost:8080",
      token: "tenant-token",
      principal: authMeResponse,
    }),
  );
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}
