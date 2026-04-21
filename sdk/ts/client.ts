export type AuthMeResponse = {
  role: string;
  tenant_id?: string;
  worker_name?: string;
};

export type PaginationMeta = {
  limit: number;
  offset: number;
  returned: number;
  has_more: boolean;
  next_offset?: number;
  next_cursor?: string;
};

export type Job = {
  id: string;
  name: string;
  tenant_id: string;
  queue: string;
  kind: string;
  created_at: string;
  updated_at: string;
};

export type ListJobsResponse = {
  jobs: Job[];
  pagination: PaginationMeta;
};

export type CreateJobResponse = {
  job_id: string;
  run_id?: string;
  status: string;
};

export class RunqClient {
  constructor(
    readonly baseUrl: string = "http://localhost:8080",
    readonly token?: string,
  ) {}

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const headers = new Headers(init?.headers || {});
    if (this.token) headers.set("Authorization", `Bearer ${this.token}`);
    if (init?.body) headers.set("Content-Type", "application/json");
    const resp = await fetch(`${this.baseUrl.replace(/\/$/, "")}${path}`, {
      ...init,
      headers,
    });
    if (!resp.ok) {
      throw new Error(`request failed: status=${resp.status} body=${await resp.text()}`);
    }
    return (await resp.json()) as T;
  }

  authMe(): Promise<AuthMeResponse> {
    return this.request<AuthMeResponse>("/v1/auth/me");
  }

  listJobs(params: Record<string, string> = {}): Promise<ListJobsResponse> {
    const query = new URLSearchParams(params).toString();
    return this.request<ListJobsResponse>(`/v1/jobs${query ? `?${query}` : ""}`);
  }

  createJob(payload: Record<string, unknown>): Promise<CreateJobResponse> {
    return this.request<CreateJobResponse>("/v1/jobs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  }

  getJob(jobId: string): Promise<{ job: Job }> {
    return this.request<{ job: Job }>(`/v1/jobs/${jobId}`);
  }

  lookupJobByDedupeKey(tenantId: string, dedupeKey: string): Promise<{ job: Job }> {
    const q = new URLSearchParams({ tenant_id: tenantId, dedupe_key: dedupeKey }).toString();
    return this.request<{ job: Job }>(`/v1/jobs/lookup?${q}`);
  }

  listRuns(params: Record<string, string> = {}): Promise<unknown> {
    const query = new URLSearchParams(params).toString();
    return this.request(`/v1/runs${query ? `?${query}` : ""}`);
  }

  listWorkers(params: Record<string, string> = {}): Promise<unknown> {
    const query = new URLSearchParams(params).toString();
    return this.request(`/v1/workers${query ? `?${query}` : ""}`);
  }

  upsertQuota(tenantId: string, payload: Record<string, unknown>): Promise<unknown> {
    return this.request(`/v1/tenants/${tenantId}/quota`, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
  }
}
