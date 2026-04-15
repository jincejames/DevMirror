/** API client -- plain fetch wrappers for every backend endpoint. */

import type {
  ConfigIn,
  ConfigOut,
  ConfigListResponse,
  ValidationResult,
  StreamSearchResponse,
  ScanResponse,
  ManifestResponse,
  ManifestData,
  ProvisionStartResponse,
  TaskStatusResponse,
  DrStatusResponse,
  DrListResponse,
  CleanupResponse,
  RefreshStartResponse,
} from './types';

const BASE = '/api';

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export async function createConfig(data: ConfigIn): Promise<ConfigOut> {
  return request<ConfigOut>(`${BASE}/configs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
}

export async function listConfigs(): Promise<ConfigListResponse> {
  return request<ConfigListResponse>(`${BASE}/configs`);
}

export async function getConfig(drId: string): Promise<ConfigOut> {
  return request<ConfigOut>(`${BASE}/configs/${encodeURIComponent(drId)}`);
}

export async function updateConfig(drId: string, data: ConfigIn): Promise<ConfigOut> {
  return request<ConfigOut>(`${BASE}/configs/${encodeURIComponent(drId)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
}

export async function deleteConfig(drId: string): Promise<void> {
  const res = await fetch(`${BASE}/configs/${encodeURIComponent(drId)}`, {
    method: 'DELETE',
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `HTTP ${res.status}`);
  }
}

export async function revalidateConfig(drId: string): Promise<ValidationResult> {
  return request<ValidationResult>(
    `${BASE}/configs/${encodeURIComponent(drId)}/validate`,
    { method: 'POST' },
  );
}

export async function exportYaml(drId: string): Promise<Blob> {
  const res = await fetch(
    `${BASE}/configs/${encodeURIComponent(drId)}/yaml`,
  );
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `HTTP ${res.status}`);
  }
  return res.blob();
}

export async function searchStreams(query: string): Promise<StreamSearchResponse> {
  return request<StreamSearchResponse>(
    `${BASE}/streams/search?q=${encodeURIComponent(query)}`,
  );
}

// ---- Stage 2 API functions ----

export async function scanConfig(drId: string): Promise<ScanResponse> {
  return request<ScanResponse>(
    `${BASE}/configs/${encodeURIComponent(drId)}/scan`,
    { method: 'POST' },
  );
}

export async function getManifest(drId: string): Promise<ManifestResponse> {
  const raw = await request<{ dr_id: string; manifest: Record<string, unknown>; scanned_at: string | null }>(
    `${BASE}/configs/${encodeURIComponent(drId)}/manifest`,
  );
  // The backend stores manifest as {scan_result: {objects, ...}} -- unwrap for the frontend
  const sr = (raw.manifest?.scan_result ?? raw.manifest ?? {}) as Record<string, unknown>;
  return {
    dr_id: raw.dr_id,
    manifest: {
      dr_id: (sr.dr_id as string) ?? raw.dr_id,
      streams_scanned: ((sr.streams_scanned as Array<Record<string, string>>) ?? []).map(s => s.name ?? s),
      objects: ((sr.objects as Array<Record<string, unknown>>) ?? []).map(o => ({
        fqn: (o.fqn as string) ?? '',
        object_type: (o.type as string) ?? (o.object_type as string) ?? 'table',
        access_mode: (o.access_mode as string) ?? 'READ_ONLY',
        estimated_size_mb: o.estimated_size_gb ? Number(o.estimated_size_gb) * 1024 : null,
        clone_strategy: (o.clone_strategy as string) ?? 'shallow_clone',
      })),
      total_schemas: ((sr.schemas_required as string[]) ?? []).length,
      review_required: (sr.review_required as boolean) ?? false,
      lineage_row_limit_hit: (sr.lineage_row_limit_hit as boolean) ?? false,
    },
    scanned_at: raw.scanned_at,
  };
}

export async function updateManifest(drId: string, manifest: ManifestData): Promise<ManifestResponse> {
  return request<ManifestResponse>(
    `${BASE}/configs/${encodeURIComponent(drId)}/manifest`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(manifest),
    },
  );
}

export async function startProvision(drId: string): Promise<ProvisionStartResponse> {
  return request<ProvisionStartResponse>(
    `${BASE}/configs/${encodeURIComponent(drId)}/provision`,
    { method: 'POST' },
  );
}

export async function getTaskStatus(taskId: string): Promise<TaskStatusResponse> {
  return request<TaskStatusResponse>(
    `${BASE}/tasks/${encodeURIComponent(taskId)}`,
  );
}

export async function getDrStatus(drId: string): Promise<DrStatusResponse> {
  return request<DrStatusResponse>(
    `${BASE}/drs/${encodeURIComponent(drId)}/status`,
  );
}

export async function listDrs(): Promise<DrListResponse> {
  return request<DrListResponse>(`${BASE}/drs`);
}

export async function cleanupDr(drId: string): Promise<CleanupResponse> {
  return request<CleanupResponse>(
    `${BASE}/drs/${encodeURIComponent(drId)}/cleanup`,
    { method: 'POST' },
  );
}

export async function refreshDr(
  drId: string,
  body?: { mode?: string; selected_objects?: string[] },
): Promise<RefreshStartResponse> {
  return request<RefreshStartResponse>(
    `${BASE}/drs/${encodeURIComponent(drId)}/refresh`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    },
  );
}

export async function reprovisionDr(drId: string): Promise<ProvisionStartResponse> {
  return request<ProvisionStartResponse>(
    `${BASE}/drs/${encodeURIComponent(drId)}/reprovision`,
    { method: 'POST' },
  );
}
