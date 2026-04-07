export type PrincipalRole = "admin" | "tenant" | "worker";

export type AuthMeResponse = {
  role: PrincipalRole;
  tenant_id?: string;
  worker_name?: string;
};

export type Run = {
  id: string;
  job_id: string;
  job_name?: string;
  tenant_id: string;
  queue?: string;
  kind?: string;
  schedule_type?: string;
  job_disabled: boolean;
  status: string;
  attempt: number;
  scheduled_at: string;
  available_at: string;
  started_at?: string;
  completed_at?: string;
  worker_id?: string;
  lease_token: number;
  lease_expires_at?: string;
  last_heartbeat_at?: string;
  result?: Record<string, unknown>;
  error_code?: string;
  error_message?: string;
};

export type RunEvent = {
  event_type: string;
  event_time: string;
  actor_type: string;
  actor_id?: string;
  payload: Record<string, unknown>;
};

export type GetRunResponse = {
  run: Run;
  events: RunEvent[];
};

export type ListRunsResponse = {
  runs: Run[];
};

export type CancelJobResponse = {
  job_id: string;
  status: string;
  canceled_runs: number;
};

export type RequeueRunResponse = {
  run_id: string;
  status: string;
  job_id: string;
  source: string;
  from_run: string;
};

export type SessionState = {
  apiBaseUrl: string;
  token: string;
  principal: AuthMeResponse;
};
