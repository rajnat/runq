import type {
  AuthMeResponse,
  CancelJobResponse,
  GetRunResponse,
  ListRunsResponse,
  RequeueRunResponse,
  SessionState,
} from "./types";

export class APIError extends Error {
  status: number;
  code?: string;

  constructor(status: number, message: string, code?: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

type RequestOptions = {
  method?: string;
  body?: unknown;
};

export class RunqClient {
  private readonly baseUrl: string;
  private readonly token: string;

  constructor(session: Pick<SessionState, "apiBaseUrl" | "token">) {
    this.baseUrl = session.apiBaseUrl;
    this.token = session.token;
  }

  authMe(): Promise<AuthMeResponse> {
    return this.request<AuthMeResponse>("/v1/auth/me");
  }

  listRuns(params: {
    tenantId?: string;
    queue?: string;
    statuses?: string[];
    workerId?: string;
    jobId?: string;
  }): Promise<ListRunsResponse> {
    const search = new URLSearchParams();
    if (params.tenantId) {
      search.set("tenant_id", params.tenantId);
    }
    if (params.queue) {
      search.set("queue", params.queue);
    }
    if (params.statuses && params.statuses.length > 0) {
      search.set("status", params.statuses.join(","));
    }
    if (params.workerId) {
      search.set("worker_id", params.workerId);
    }
    if (params.jobId) {
      search.set("job_id", params.jobId);
    }
    const suffix = search.toString();
    return this.request<ListRunsResponse>(suffix ? `/v1/runs?${suffix}` : "/v1/runs");
  }

  getRun(runId: string): Promise<GetRunResponse> {
    return this.request<GetRunResponse>(`/v1/runs/${runId}`);
  }

  requeueRun(runId: string): Promise<RequeueRunResponse> {
    return this.request<RequeueRunResponse>(`/v1/runs/${runId}/requeue`, { method: "POST" });
  }

  cancelJob(jobId: string): Promise<CancelJobResponse> {
    return this.request<CancelJobResponse>(`/v1/jobs/${jobId}/cancel`, { method: "POST" });
  }

  private async request<T>(path: string, options: RequestOptions = {}): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: options.method ?? "GET",
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });

    if (!response.ok) {
      const fallbackMessage = `request failed with status ${response.status}`;
      try {
        const payload = (await response.json()) as { error?: { message?: string; code?: string } };
        throw new APIError(
          response.status,
          payload.error?.message ?? fallbackMessage,
          payload.error?.code,
        );
      } catch (error) {
        if (error instanceof APIError) {
          throw error;
        }
        throw new APIError(response.status, fallbackMessage);
      }
    }

    return (await response.json()) as T;
  }
}
