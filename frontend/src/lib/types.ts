export type Department = {
  id: string;
  name: string;
  code: string;
  domain: string;
  ingestion_tier: 1 | 2 | 3;
  connection_type: 'webhook' | 'polling' | 'snapshot';
  status: 'active' | 'degraded' | 'offline' | 'circuit_open';
  poll_interval_minutes: number | null;
  last_sync_at: string | null;
  records_synced: number;
  created_at: string;
};

export type Business = {
  id: string;
  ubid: string;
  business_name: string;
  pan: string | null;
  gstin: string | null;
  registered_address: string | null;
  owner_name: string | null;
  status: 'active' | 'pending_ubid' | 'suspended';
  sws_id: string | null;
  created_at: string;
  updated_at: string;
};

export type PropagationEvent = {
  id: string;
  ubid: string;
  event_type: string;
  source_system: string;
  destination_system: string;
  payload_hash: string | null;
  idempotency_key: string;
  outcome: 'success' | 'failure' | 'duplicate' | 'pending' | 'dlq' | 'conflict' | 'held';
  direction: 'sws_to_dept' | 'dept_to_sws';
  conflict_flag: boolean;
  resolution_applied: string | null;
  retry_count: number;
  error_message: string | null;
  propagation_ms: number | null;
  created_at: string;
};

export type Conflict = {
  id: string;
  ubid: string;
  field_name: string;
  sws_value: string | null;
  dept_value: string | null;
  source_department_id: string | null;
  resolution_policy: 'sws_wins' | 'last_write_wins' | 'manual_review';
  status: 'open' | 'resolved' | 'pending_review';
  winning_value: string | null;
  resolved_by: string | null;
  resolved_at: string | null;
  resolution_reason: string | null;
  propagation_event_id: string | null;
  created_at: string;
};

export type SchemaMapping = {
  id: string;
  department_id: string;
  sws_field: string;
  dept_field: string;
  confidence_score: number;
  status: 'auto_mapped' | 'pending_review' | 'confirmed' | 'rejected';
  version: number;
  reviewed_by: string | null;
  reviewed_at: string | null;
  created_at: string;
};

export type DlqMessage = {
  id: string;
  propagation_event_id: string | null;
  ubid: string;
  event_type: string;
  source_system: string;
  destination_system: string;
  payload: string;
  created_at: string;
};

export type AuditRecord = {
  id: string;
  ubid: string;
  event_type: string;
  source_system: string;
  destination_system: string;
  payload_hash: string;
  idempotency_key: string;
  outcome: string;
  created_at: string;
};