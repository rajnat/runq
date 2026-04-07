import { FormEvent, useEffect, useMemo, useState } from "react";
import { Link, Route, Routes, useLocation, useNavigate, useParams } from "react-router-dom";
import { APIError, RunqClient } from "./api";
import { clearSession, loadSession, normalizeBaseUrl, saveSession } from "./session";
import type { GetRunResponse, Run, SessionState } from "./types";

type Banner = {
  tone: "error" | "success";
  message: string;
};

type Filters = {
  tenantId: string;
  queue: string;
  includeCanceled: boolean;
  jobId: string;
  workerId: string;
  runIdQuery: string;
};

const defaultStatuses = ["FAILED", "TIMED_OUT"];

export default function App() {
  const [session, setSession] = useState<SessionState | null>(() => loadSession());
  const [banner, setBanner] = useState<Banner | null>(null);

  const handleSignedIn = (nextSession: SessionState) => {
    saveSession(nextSession);
    setSession(nextSession);
    setBanner(null);
  };

  const handleSignOut = () => {
    clearSession();
    setSession(null);
    setBanner(null);
  };

  if (!session) {
    return <SignInScreen onSignedIn={handleSignedIn} />;
  }

  return (
    <div className="shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">runq</p>
          <h1>Failed-work console</h1>
        </div>
        <div className="session-pill">
          <span>{session.principal.role}</span>
          {session.principal.tenant_id ? <span>{session.principal.tenant_id}</span> : null}
          <button className="ghost-button" onClick={handleSignOut}>
            Sign out
          </button>
        </div>
      </header>
      {banner ? <BannerView banner={banner} onDismiss={() => setBanner(null)} /> : null}
      <Routes>
        <Route
          path="/"
          element={<FailedRunsPage session={session} onBanner={setBanner} />}
        />
        <Route
          path="/runs/:runId"
          element={<RunDetailPage session={session} onBanner={setBanner} />}
        />
      </Routes>
    </div>
  );
}

function SignInScreen(props: { onSignedIn: (session: SessionState) => void }) {
  const [apiBaseUrl, setAPIBaseUrl] = useState("http://localhost:8080");
  const [token, setToken] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);

    const session = {
      apiBaseUrl: normalizeBaseUrl(apiBaseUrl),
      token: token.trim(),
    };

    try {
      const client = new RunqClient(session);
      const principal = await client.authMe();
      if (principal.role === "worker") {
        throw new APIError(403, "worker principals cannot use the failed-work console");
      }
      props.onSignedIn({
        ...session,
        principal,
      });
    } catch (err) {
      setError(asMessage(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="signin-layout">
      <div className="signin-panel">
        <p className="eyebrow">runq ops console</p>
        <h1>Connect to the API</h1>
        <p className="supporting">
          Sign in with an existing bearer token. The console will scope itself automatically for
          admin and tenant principals.
        </p>
        <form className="form-grid" onSubmit={handleSubmit}>
          <label>
            API base URL
            <input
              value={apiBaseUrl}
              onChange={(event) => setAPIBaseUrl(event.target.value)}
              placeholder="http://localhost:8080"
            />
          </label>
          <label>
            Bearer token
            <input
              value={token}
              onChange={(event) => setToken(event.target.value)}
              placeholder="admin-token"
              type="password"
            />
          </label>
          {error ? <div className="banner error">{error}</div> : null}
          <button disabled={submitting || !token.trim()} type="submit">
            {submitting ? "Connecting..." : "Sign in"}
          </button>
        </form>
      </div>
    </div>
  );
}

function FailedRunsPage(props: { session: SessionState; onBanner: (banner: Banner | null) => void }) {
  const client = useMemo(() => new RunqClient(props.session), [props.session]);
  const location = useLocation();
  const [filters, setFilters] = useState<Filters>({
    tenantId: props.session.principal.tenant_id ?? "",
    queue: "",
    includeCanceled: false,
    jobId: "",
    workerId: "",
    runIdQuery: "",
  });
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (location.state && typeof location.state === "object" && "message" in location.state) {
      const nextBanner = location.state as { message?: string };
      if (nextBanner.message) {
        props.onBanner({ tone: "success", message: nextBanner.message });
      }
      window.history.replaceState({}, document.title);
    }
  }, [location.state, props]);

  useEffect(() => {
    const statuses = filters.includeCanceled ? [...defaultStatuses, "CANCELED"] : defaultStatuses;
    setLoading(true);
    setError(null);
    client
      .listRuns({
        tenantId: props.session.principal.role === "tenant" ? props.session.principal.tenant_id : filters.tenantId,
        queue: filters.queue,
        statuses,
        jobId: filters.jobId,
        workerId: filters.workerId,
      })
      .then((response) => {
        setRuns(response.runs);
      })
      .catch((err) => {
        setError(asMessage(err));
      })
      .finally(() => {
        setLoading(false);
      });
  }, [client, filters.includeCanceled, filters.jobId, filters.queue, filters.tenantId, filters.workerId, props.session.principal.role, props.session.principal.tenant_id]);

  const visibleRuns = runs.filter((run) => {
    if (!filters.runIdQuery.trim()) {
      return true;
    }
    const query = filters.runIdQuery.trim().toLowerCase();
    return run.id.toLowerCase().includes(query);
  });

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">workflow</p>
            <h2>Failed and timed-out runs</h2>
          </div>
          <Link className="ghost-button" to="/">
            Refresh view
          </Link>
        </div>
        <div className="filters">
          {props.session.principal.role === "tenant" ? (
            <div className="locked-filter">
              Tenant scope: <strong>{props.session.principal.tenant_id}</strong>
            </div>
          ) : (
            <label>
              Tenant
              <input
                value={filters.tenantId}
                onChange={(event) => setFilters((current) => ({ ...current, tenantId: event.target.value }))}
                placeholder="tenant-id"
              />
            </label>
          )}
          <label>
            Queue
            <input
              value={filters.queue}
              onChange={(event) => setFilters((current) => ({ ...current, queue: event.target.value }))}
              placeholder="default"
            />
          </label>
          <label>
            Job ID
            <input
              value={filters.jobId}
              onChange={(event) => setFilters((current) => ({ ...current, jobId: event.target.value }))}
              placeholder="job-..."
            />
          </label>
          <label>
            Worker ID
            <input
              value={filters.workerId}
              onChange={(event) => setFilters((current) => ({ ...current, workerId: event.target.value }))}
              placeholder="worker-..."
            />
          </label>
          <label>
            Run ID search
            <input
              value={filters.runIdQuery}
              onChange={(event) => setFilters((current) => ({ ...current, runIdQuery: event.target.value }))}
              placeholder="run-..."
            />
          </label>
          <label className="checkbox">
            <input
              checked={filters.includeCanceled}
              onChange={(event) => setFilters((current) => ({ ...current, includeCanceled: event.target.checked }))}
              type="checkbox"
            />
            Include canceled
          </label>
        </div>
        {error ? <div className="banner error">{error}</div> : null}
        {loading ? <div className="empty-state">Loading failed runs...</div> : null}
        {!loading && visibleRuns.length === 0 ? (
          <div className="empty-state">No failed runs matched the current filters.</div>
        ) : null}
        {!loading && visibleRuns.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Run ID</th>
                  <th>Job</th>
                  <th>Tenant</th>
                  <th>Queue</th>
                  <th>Kind</th>
                  <th>Status</th>
                  <th>Attempt</th>
                  <th>Scheduled</th>
                  <th>Completed</th>
                  <th>Worker</th>
                </tr>
              </thead>
              <tbody>
                {visibleRuns.map((run) => (
                  <tr key={run.id}>
                    <td>
                      <Link to={`/runs/${run.id}`}>{run.id}</Link>
                    </td>
                    <td>{run.job_name ?? run.job_id}</td>
                    <td>{run.tenant_id}</td>
                    <td>{run.queue ?? "-"}</td>
                    <td>{run.kind ?? "-"}</td>
                    <td><StatusPill status={run.status} /></td>
                    <td>{run.attempt}</td>
                    <td>{formatTimestamp(run.scheduled_at)}</td>
                    <td>{formatTimestamp(run.completed_at)}</td>
                    <td>{run.worker_id ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </div>
  );
}

function RunDetailPage(props: { session: SessionState; onBanner: (banner: Banner | null) => void }) {
  const { runId = "" } = useParams();
  const navigate = useNavigate();
  const client = useMemo(() => new RunqClient(props.session), [props.session]);
  const [response, setResponse] = useState<GetRunResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [confirmAction, setConfirmAction] = useState<"requeue" | "cancel" | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(null);
    client
      .getRun(runId)
      .then((nextResponse) => setResponse(nextResponse))
      .catch((err) => setError(asMessage(err)))
      .finally(() => setLoading(false));
  }, [client, runId]);

  async function handleConfirmedAction() {
    if (!response) {
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      if (confirmAction === "requeue") {
        const requeue = await client.requeueRun(response.run.id);
        props.onBanner({ tone: "success", message: `Run requeued as ${requeue.run_id}.` });
        navigate(`/runs/${requeue.run_id}`);
      }
      if (confirmAction === "cancel") {
        await client.cancelJob(response.run.job_id);
        props.onBanner({ tone: "success", message: `Job ${response.run.job_id} canceled.` });
        const nextResponse = await client.getRun(response.run.id);
        setResponse(nextResponse);
      }
      setConfirmAction(null);
    } catch (err) {
      setError(asMessage(err));
    } finally {
      setSubmitting(false);
    }
  }

  if (loading) {
    return <div className="page"><section className="panel"><div className="empty-state">Loading run detail...</div></section></div>;
  }

  if (error) {
    return <div className="page"><section className="panel"><div className="banner error">{error}</div></section></div>;
  }

  if (!response) {
    return null;
  }

  const { run, events } = response;
  const requeueable =
    ["FAILED", "TIMED_OUT", "CANCELED"].includes(run.status) &&
    (!run.job_disabled || run.schedule_type === "once");
  const cancelable = !run.job_disabled;

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <div>
            <Link className="crumb" to="/">
              Back to failed runs
            </Link>
            <h2>{run.job_name ?? run.job_id}</h2>
            <p className="supporting">{run.id}</p>
          </div>
          <div className="action-row">
            <button disabled={!requeueable} onClick={() => setConfirmAction("requeue")}>
              Requeue run
            </button>
            <button className="ghost-button" disabled={!cancelable} onClick={() => setConfirmAction("cancel")}>
              Cancel job
            </button>
          </div>
        </div>
        {error ? <div className="banner error">{error}</div> : null}
        <div className="detail-grid">
          <KeyValue label="Tenant" value={run.tenant_id} />
          <KeyValue label="Queue" value={run.queue ?? "-"} />
          <KeyValue label="Kind" value={run.kind ?? "-"} />
          <KeyValue label="Status" value={run.status} />
          <KeyValue label="Attempt" value={String(run.attempt)} />
          <KeyValue label="Schedule" value={run.schedule_type ?? "-"} />
          <KeyValue label="Worker" value={run.worker_id ?? "-"} />
          <KeyValue label="Job active" value={run.job_disabled ? "No" : "Yes"} />
        </div>
        <div className="json-grid">
          <JSONPanel title="Result" value={run.result ?? {}} />
          <JSONPanel title="Error" value={{ code: run.error_code, message: run.error_message }} />
        </div>
        <section className="events">
          <h3>Run timeline</h3>
          <ul>
            {events.map((event) => (
              <li key={`${event.event_type}-${event.event_time}`}>
                <div className="event-header">
                  <strong>{event.event_type}</strong>
                  <span>{formatTimestamp(event.event_time)}</span>
                </div>
                <p className="supporting">
                  actor: {event.actor_type}
                  {event.actor_id ? ` / ${event.actor_id}` : ""}
                </p>
                <pre>{JSON.stringify(event.payload, null, 2)}</pre>
              </li>
            ))}
          </ul>
        </section>
      </section>
      {confirmAction ? (
        <ConfirmModal
          busy={submitting}
          confirmLabel={confirmAction === "requeue" ? "Requeue run" : "Cancel job"}
          message={
            confirmAction === "requeue"
              ? "Create a fresh pending run from this terminal run?"
              : "Disable the job and cancel any pending or running work?"
          }
          onCancel={() => setConfirmAction(null)}
          onConfirm={handleConfirmedAction}
        />
      ) : null}
    </div>
  );
}

function BannerView(props: { banner: Banner; onDismiss: () => void }) {
  return (
    <div className={`banner ${props.banner.tone}`}>
      <span>{props.banner.message}</span>
      <button className="ghost-button" onClick={props.onDismiss}>
        Dismiss
      </button>
    </div>
  );
}

function ConfirmModal(props: {
  busy: boolean;
  message: string;
  confirmLabel: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="modal-backdrop" role="dialog" aria-modal="true">
      <div className="modal">
        <h3>Confirm action</h3>
        <p>{props.message}</p>
        <div className="action-row">
          <button className="ghost-button" onClick={props.onCancel}>
            Keep current state
          </button>
          <button disabled={props.busy} onClick={props.onConfirm}>
            {props.busy ? "Working..." : props.confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

function KeyValue(props: { label: string; value: string }) {
  return (
    <div className="kv">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

function JSONPanel(props: { title: string; value: unknown }) {
  return (
    <section className="json-panel">
      <h3>{props.title}</h3>
      <pre>{JSON.stringify(props.value, null, 2)}</pre>
    </section>
  );
}

function StatusPill(props: { status: string }) {
  return <span className={`status-pill status-${props.status.toLowerCase()}`}>{props.status}</span>;
}

function asMessage(error: unknown): string {
  if (error instanceof APIError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return "Something went wrong.";
}

function formatTimestamp(value?: string): string {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString();
}
