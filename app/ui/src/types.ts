/** TypeScript interfaces matching backend Pydantic models. */

export interface UserInfo {
  email: string;
  role: 'admin' | 'user';
  display_name: string;
}

export interface ConfigIn {
  dr_id: string;
  description?: string | null;
  streams: string[];
  additional_objects?: string[] | null;
  target_catalog?: string | null;
  qa_enabled: boolean;
  data_revision_mode: string;
  data_revision_version?: number | null;
  data_revision_timestamp?: string | null;
  developers: string[];
  qa_users?: string[] | null;
  expiration_date: string;
  notification_days_before: number;
  notification_recipients?: string[] | null;
}

export interface FieldError {
  loc: string[];
  msg: string;
}

export interface ValidationResult {
  status: string;
  errors: FieldError[];
}

export interface ConfigOut {
  dr_id: string;
  description: string | null;
  status: string;
  config: ConfigIn;
  validation_errors: FieldError[];
  created_at: string;
  created_by: string;
  updated_at: string | null;
  expiration_date: string;
}

export interface ConfigListItem {
  dr_id: string;
  description: string | null;
  status: string;
  created_at: string;
  created_by: string;
  expiration_date: string;
}

export interface ConfigListResponse {
  configs: ConfigListItem[];
  total: number;
}

export interface StreamSearchResult {
  name: string;
  type: string;
}

export interface StreamSearchResponse {
  results: StreamSearchResult[];
}

// ---- Stage 2 types ----

export interface ScanResponse {
  dr_id: string;
  status: string;
  manifest: ManifestData;
}

export interface ManifestResponse {
  dr_id: string;
  manifest: ManifestData;
  scanned_at: string | null;
}

export interface ManifestData {
  dr_id: string;
  streams_scanned: string[];
  objects: ManifestObject[];
  total_schemas: number;
  review_required: boolean;
  lineage_row_limit_hit?: boolean;
}

export interface ManifestObject {
  fqn: string;
  object_type: string;
  access_mode: string;
  estimated_size_mb: number | null;
  clone_strategy: string;
  source_stream?: string;
}

export interface ProvisionStartResponse {
  dr_id: string;
  task_id: string;
  status: string;
  message: string;
}

export interface TaskStatusResponse {
  task_id: string;
  dr_id: string;
  task_type: string;
  status: string;
  progress: string;
  result: Record<string, unknown> | null;
  error: string | null;
  started_at: string;
  completed_at: string | null;
}

export interface DrStatusResponse {
  dr_id: string;
  status: string;
  description: string | null;
  expiration_date: string;
  created_at: string;
  created_by: string;
  last_refreshed_at: string | null;
  objects: DrObject[];
  total_objects: number;
  object_breakdown: Record<string, number>;
  recent_audit: AuditEntry[];
}

export interface DrObject {
  source_fqn: string;
  target_fqn: string;
  status: string;
  clone_strategy: string;
  [key: string]: unknown;
}

export interface AuditEntry {
  action: string;
  timestamp: string;
  details?: string;
  [key: string]: unknown;
}

export interface DrListItem {
  dr_id: string;
  status: string;
  description: string | null;
  expiration_date: string;
  created_at: string;
  created_by: string;
  total_objects: number;
}

export interface DrListResponse {
  drs: DrListItem[];
  total: number;
}

export interface CleanupResponse {
  dr_id: string;
  final_status: string;
  objects_dropped: number;
  schemas_dropped: number;
  revokes_succeeded: number;
}

export interface RefreshStartResponse {
  dr_id: string;
  task_id: string;
  status: string;
  message: string;
}

export interface ModifyDrRequest {
  new_expiration_date?: string | null;
  add_developers?: string[] | null;
  remove_developers?: string[] | null;
  add_qa_users?: string[] | null;
  remove_qa_users?: string[] | null;
}

export interface ModifyDrResponse {
  dr_id: string;
  status: string;
  message: string;
}
