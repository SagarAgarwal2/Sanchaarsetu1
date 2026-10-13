-- Initialize extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Tables
CREATE TABLE IF NOT EXISTS audit (
  id SERIAL PRIMARY KEY,
  ubid TEXT,
  event_type TEXT,
  source_system TEXT,
  destination_system TEXT,
  payload_hash TEXT,
  idempotency_key TEXT,
  outcome TEXT,
  previous_hash TEXT,
  chained_hash TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_ubid ON audit(ubid);

CREATE TABLE IF NOT EXISTS departments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  code TEXT UNIQUE NOT NULL,
  domain TEXT NOT NULL,
  ingestion_tier INTEGER NOT NULL DEFAULT 1 CHECK (ingestion_tier IN (1, 2, 3)),
  connection_type TEXT NOT NULL DEFAULT 'webhook' CHECK (connection_type IN ('webhook', 'polling', 'snapshot')),
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'degraded', 'offline', 'circuit_open')),
  poll_interval_minutes INTEGER,
  last_sync_at TIMESTAMPTZ,
  records_synced INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS businesses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ubid TEXT UNIQUE NOT NULL,
  business_name TEXT NOT NULL,
  pan TEXT,
  gstin TEXT,
  registered_address TEXT,
  owner_name TEXT,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'pending_ubid', 'suspended')),
  sws_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS business_departments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID NOT NULL REFERENCES businesses(id),
  department_id UUID NOT NULL REFERENCES departments(id),
  dept_record_id TEXT,
  link_status TEXT NOT NULL DEFAULT 'confirmed' CHECK (link_status IN ('candidate', 'confirmed', 'rejected')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(business_id, department_id)
);

CREATE TABLE IF NOT EXISTS propagation_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ubid TEXT NOT NULL,
  event_type TEXT NOT NULL,
  source_system TEXT NOT NULL,
  destination_system TEXT NOT NULL,
  payload_hash TEXT,
  idempotency_key TEXT UNIQUE NOT NULL,
  outcome TEXT NOT NULL DEFAULT 'pending' CHECK (outcome IN ('success', 'failure', 'duplicate', 'pending', 'dlq', 'conflict', 'held')),
  direction TEXT NOT NULL DEFAULT 'dept_to_sws' CHECK (direction IN ('sws_to_dept', 'dept_to_sws')),
  conflict_flag BOOLEAN NOT NULL DEFAULT FALSE,
  resolution_applied TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  propagation_ms INTEGER,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conflicts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ubid TEXT NOT NULL,
  field_name TEXT NOT NULL,
  sws_value TEXT,
  dept_value TEXT,
  source_department_id UUID REFERENCES departments(id),
  resolution_policy TEXT NOT NULL DEFAULT 'sws_wins' CHECK (resolution_policy IN ('sws_wins', 'last_write_wins', 'manual_review')),
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved', 'pending_review')),
  winning_value TEXT,
  resolved_by TEXT,
  resolved_at TIMESTAMPTZ,
  propagation_event_id UUID REFERENCES propagation_events(id),
  resolution_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS schema_mappings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  department_id UUID NOT NULL REFERENCES departments(id),
  sws_field TEXT NOT NULL,
  dept_field TEXT NOT NULL,
  confidence_score NUMERIC(4,3) NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'pending_review' CHECK (status IN ('auto_mapped', 'pending_review', 'confirmed', 'rejected')),
  version INTEGER NOT NULL DEFAULT 1,
  reviewed_by TEXT,
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(department_id, sws_field, dept_field, version)
);

CREATE INDEX IF NOT EXISTS idx_propagation_events_ubid ON propagation_events(ubid);
CREATE INDEX IF NOT EXISTS idx_propagation_events_created_at ON propagation_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_propagation_events_outcome ON propagation_events(outcome);
CREATE INDEX IF NOT EXISTS idx_conflicts_status ON conflicts(status);
CREATE INDEX IF NOT EXISTS idx_conflicts_ubid ON conflicts(ubid);
CREATE INDEX IF NOT EXISTS idx_schema_mappings_dept ON schema_mappings(department_id);
CREATE INDEX IF NOT EXISTS idx_schema_mappings_status ON schema_mappings(status);

-- Seed demo data so the operator dashboard is populated on first run
INSERT INTO departments (name, code, domain, ingestion_tier, connection_type, status, poll_interval_minutes, last_sync_at, records_synced) VALUES
  ('Department of Factories & Boilers', 'DFB', 'factories', 1, 'webhook', 'active', NULL, now() - interval '2 minutes', 12847),
  ('Shop & Establishment Authority', 'SEA', 'shop_establishments', 1, 'webhook', 'active', NULL, now() - interval '5 minutes', 34291),
  ('Department of Labour', 'DOL', 'labour', 2, 'polling', 'active', 5, now() - interval '4 minutes', 8934),
  ('Karnataka Fire & Emergency Services', 'KFES', 'fire_safety', 2, 'polling', 'degraded', 10, now() - interval '18 minutes', 5612),
  ('BBMP (Commercial Properties)', 'BBMP', 'municipal', 3, 'snapshot', 'active', NULL, now() - interval '1 hour', 21043),
  ('Commercial Taxes Department', 'CTD', 'taxation', 1, 'webhook', 'active', NULL, now() - interval '1 minute', 67821),
  ('Karnataka Pollution Control Board', 'KPCB', 'environment', 2, 'polling', 'active', 15, now() - interval '12 minutes', 3291),
  ('Directorate of Industrial Safety', 'DIS', 'industrial_safety', 3, 'snapshot', 'offline', NULL, now() - interval '3 hours', 1847)
ON CONFLICT (code) DO NOTHING;

INSERT INTO businesses (ubid, business_name, pan, gstin, registered_address, owner_name, status, sws_id) VALUES
  ('UBID-KA-2024-001847', 'M/s Bangalore Textiles Pvt Ltd', 'AABCB1234F', '29AABCB1234F1Z5', '14, Industrial Area, Peenya, Bangalore - 560058', 'Ramesh Kumar Jain', 'active', 'SWS-2024-78234'),
  ('UBID-KA-2024-002341', 'Sri Venkateshwara Engineering Works', 'BCDVE5678G', '29BCDVE5678G1Z2', '8/A, KIADB Industrial Estate, Bommasandra, Bangalore - 560099', 'Venkata Reddy', 'active', 'SWS-2024-81923'),
  ('UBID-KA-2024-003892', 'Kaveri Food Products LLP', 'AAECK7890H', '29AAECK7890H1Z8', '23, Rajajinagar Industrial Area, Bangalore - 560010', 'Meenakshi Sundaram', 'active', 'SWS-2024-90012'),
  ('UBID-KA-2024-005123', 'Karnataka Chemicals & Pharma Ltd', 'AAFKC2345J', '29AAFKC2345J1Z1', '45, Whitefield Industrial Zone, Bangalore - 560066', 'Dr. Suresh Patel', 'active', 'SWS-2024-95678'),
  ('UBID-KA-2024-006789', 'Green Valley Waste Management', 'AAGVW8901K', '29AAGVW8901K1Z4', '12, Electronic City Phase 2, Bangalore - 560100', 'Anitha Krishnamurthy', 'active', 'SWS-2024-98345'),
  ('UBID-KA-2024-008234', 'Deccan Auto Components', 'AAHDA4567L', NULL, '67, Tumkur Road Industrial Corridor, Bangalore - 560073', 'Mohammed Farooq', 'pending_ubid', 'SWS-2024-99012')
ON CONFLICT (ubid) DO NOTHING;

INSERT INTO propagation_events (ubid, event_type, source_system, destination_system, payload_hash, idempotency_key, outcome, conflict_flag, resolution_applied, retry_count, propagation_ms, direction, error_message) VALUES
  ('UBID-KA-2024-001847', 'address_update', 'SWS', 'DFB', 'sha256:a3f4b2c1d8e9f0a1', 'UBID-KA-2024-001847:address_update:1746230400', 'success', false, NULL, 0, 234, 'sws_to_dept', NULL),
  ('UBID-KA-2024-001847', 'address_update', 'SWS', 'SEA', 'sha256:a3f4b2c1d8e9f0a1', 'UBID-KA-2024-001847:address_update:1746230401', 'success', false, NULL, 0, 189, 'sws_to_dept', NULL),
  ('UBID-KA-2024-001847', 'address_update', 'SWS', 'DOL', 'sha256:a3f4b2c1d8e9f0a1', 'UBID-KA-2024-001847:address_update:1746230402', 'success', false, NULL, 0, 412, 'sws_to_dept', NULL),
  ('UBID-KA-2024-001847', 'address_update', 'SWS', 'KFES', 'sha256:a3f4b2c1d8e9f0a1', 'UBID-KA-2024-001847:address_update:1746230403', 'failure', false, NULL, 3, NULL, 'sws_to_dept', 'Connection timeout to KFES API after 30s'),
  ('UBID-KA-2024-002341', 'signatory_update', 'SEA', 'SWS', 'sha256:b4c5d6e7f8a9b0c1', 'UBID-KA-2024-002341:signatory_update:1746231000', 'conflict', true, 'manual_review', 0, NULL, 'dept_to_sws', NULL),
  ('UBID-KA-2024-003892', 'license_renewal', 'SWS', 'DFB', 'sha256:c5d6e7f8a9b0c1d2', 'UBID-KA-2024-003892:license_renewal:1746231600', 'success', false, NULL, 0, 156, 'sws_to_dept', NULL),
  ('UBID-KA-2024-003892', 'gstin_update', 'CTD', 'SWS', 'sha256:d6e7f8a9b0c1d2e3', 'UBID-KA-2024-004111:gstin_update:1746232200', 'success', false, NULL, 0, 98, 'dept_to_sws', NULL),
  ('UBID-KA-2024-005123', 'compliance_status', 'KPCB', 'SWS', 'sha256:e7f8a9b0c1d2e3f4', 'UBID-KA-2024-005123:compliance_status:1746232800', 'success', false, NULL, 0, 723, 'dept_to_sws', NULL),
  ('UBID-KA-2024-006789', 'noc_issued', 'KFES', 'SWS', 'sha256:f8a9b0c1d2e3f4a5', 'UBID-KA-2024-006789:noc_issued:1746233400', 'duplicate', false, NULL, 0, NULL, 'dept_to_sws', NULL),
  ('UBID-KA-2024-001847', 'owner_update', 'SWS', 'CTD', 'sha256:a9b0c1d2e3f4a5b6', 'UBID-KA-2024-001847:owner_update:1746234000', 'success', false, NULL, 0, 301, 'sws_to_dept', NULL),
  ('UBID-KA-2024-002341', 'address_update', 'DFB', 'SWS', 'sha256:b0c1d2e3f4a5b6c7', 'UBID-KA-2024-002341:address_update:1746234600', 'conflict', true, 'sws_wins', 0, 445, 'dept_to_sws', NULL),
  ('UBID-KA-2024-008234', 'registration_new', 'SWS', 'SEA', 'sha256:c1d2e3f4a5b6c7d8', 'UBID-KA-2024-008234:registration_new:1746235200', 'pending', false, NULL, 0, NULL, 'sws_to_dept', NULL),
  ('UBID-KA-2024-003892', 'address_update', 'SWS', 'BBMP', 'sha256:d2e3f4a5b6c7d8e9', 'UBID-KA-2024-003892:address_update:1746235800', 'success', false, NULL, 0, 1823, 'sws_to_dept', NULL),
  ('UBID-KA-2024-005123', 'license_new', 'SWS', 'DIS', 'sha256:e3f4a5b6c7d8e9f0', 'UBID-KA-2024-004567:license_new:1746236400', 'failure', false, NULL, 4, NULL, 'sws_to_dept', 'Connection timeout to DIS API after 30s'),
  ('UBID-KA-2024-005123', 'signatory_update', 'SWS', 'DFB', 'sha256:f4a5b6c7d8e9f0a1', 'UBID-KA-2024-005123:signatory_update:1746237000', 'success', false, NULL, 0, 267, 'sws_to_dept', NULL)
ON CONFLICT (idempotency_key) DO NOTHING;

INSERT INTO conflicts (ubid, field_name, sws_value, dept_value, resolution_policy, status, winning_value, resolved_by, resolved_at, resolution_reason) VALUES
  ('UBID-KA-2024-002341', 'signatory_name', 'Venkata Reddy', 'V. Reddy & Sons', 'manual_review', 'pending_review', NULL, NULL, NULL, NULL),
  ('UBID-KA-2024-002341', 'registered_address', '8/A KIADB Industrial Estate, Bommasandra', 'Plot 8A, KIADB Bommasandra, Bangalore 560099', 'sws_wins', 'resolved', '8/A KIADB Industrial Estate, Bommasandra', 'system:sws_wins_policy', now() - interval '1 hour', 'SWS value accepted for field "registered_address" via SWS-wins policy (auto-resolution).'),
  ('UBID-KA-2024-008234', 'pan', 'AAHDA4567L', 'AAHDA4567M', 'manual_review', 'pending_review', NULL, NULL, NULL, NULL),
  ('UBID-KA-2024-006789', 'employee_count', '42', '45', 'last_write_wins', 'resolved', '45', 'system:last_write_wins_policy', now() - interval '30 minutes', 'Last write value accepted')
ON CONFLICT DO NOTHING;

WITH dfb AS (SELECT id FROM departments WHERE code = 'DFB')
INSERT INTO schema_mappings (department_id, sws_field, dept_field, confidence_score, status, version, reviewed_by, reviewed_at)
SELECT dfb.id, sws_field, dept_field, confidence_score, status, 1, reviewed_by, reviewed_at
FROM dfb, (VALUES
  ('business_name', 'factory_name', 0.962, 'confirmed', 'admin', now() - interval '2 days'),
  ('owner_name', 'occupier_name', 0.891, 'confirmed', 'admin', now() - interval '2 days'),
  ('registered_address', 'factory_address', 0.934, 'confirmed', 'admin', now() - interval '2 days'),
  ('gstin', 'gst_number', 0.978, 'auto_mapped', NULL, NULL),
  ('pan', 'pan_card_no', 0.956, 'auto_mapped', NULL, NULL),
  ('employee_count', 'total_workers', 0.823, 'confirmed', 'admin', now() - interval '2 days'),
  ('license_expiry', 'certificate_validity', 0.712, 'pending_review', NULL, NULL)
) AS m(sws_field, dept_field, confidence_score, status, reviewed_by, reviewed_at)
ON CONFLICT DO NOTHING;

WITH dol AS (SELECT id FROM departments WHERE code = 'DOL')
INSERT INTO schema_mappings (department_id, sws_field, dept_field, confidence_score, status, version, reviewed_by, reviewed_at)
SELECT dol.id, sws_field, dept_field, confidence_score, status, 1, reviewed_by, reviewed_at
FROM dol, (VALUES
  ('business_name', 'establishment_name', 0.887, 'confirmed', 'admin', now() - interval '1 day'),
  ('owner_name', 'employer_name', 0.901, 'confirmed', 'admin', now() - interval '1 day'),
  ('registered_address', 'employer_address', 0.923, 'confirmed', 'admin', now() - interval '1 day'),
  ('employee_count', 'workforce_strength', 0.798, 'pending_review', NULL, NULL),
  ('pan', 'proprietor_pan', 0.934, 'auto_mapped', NULL, NULL)
) AS m(sws_field, dept_field, confidence_score, status, reviewed_by, reviewed_at)
ON CONFLICT DO NOTHING;

WITH bbmp AS (SELECT id FROM departments WHERE code = 'BBMP')
INSERT INTO schema_mappings (department_id, sws_field, dept_field, confidence_score, status, version, reviewed_by, reviewed_at)
SELECT bbmp.id, sws_field, dept_field, confidence_score, status, 1, reviewed_by, reviewed_at
FROM bbmp, (VALUES
  ('business_name', 'trade_name', 0.756, 'pending_review', NULL, NULL),
  ('registered_address', 'property_address', 0.869, 'confirmed', 'admin', now() - interval '3 days'),
  ('owner_name', 'applicant_full_name', 0.634, 'pending_review', NULL, NULL),
  ('gstin', 'taxpayer_gstin', 0.991, 'auto_mapped', NULL, NULL)
) AS m(sws_field, dept_field, confidence_score, status, reviewed_by, reviewed_at)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS mapping_registry (
  id SERIAL PRIMARY KEY,
  department TEXT NOT NULL,
  source_field TEXT NOT NULL,
  target_field TEXT NOT NULL,
  confidence FLOAT,
  version INTEGER DEFAULT 1,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS change_events (
  id SERIAL PRIMARY KEY,
  department TEXT,
  event_type TEXT,
  payload TEXT,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conflict_log (
  id SERIAL PRIMARY KEY,
  ubid TEXT,
  source1 TEXT,
  source2 TEXT,
  policy TEXT,
  resolution TEXT,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dlq_messages (
  id SERIAL PRIMARY KEY,
  propagation_event_id UUID,
  ubid TEXT NOT NULL,
  event_type TEXT,
  source_system TEXT,
  destination_system TEXT,
  payload JSONB,
  created_at TIMESTAMP DEFAULT now(),
  replayed_at TIMESTAMP,
  UNIQUE(propagation_event_id)
);

CREATE INDEX IF NOT EXISTS idx_dlq_messages_created_at ON dlq_messages(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_messages_replayed_at ON dlq_messages(replayed_at);

-- ---------------------------------------------------------------------------
-- Field registry: every field of every connected system, with the metadata the
-- matcher uses. system_code is 'SWS' or a department code.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_fields (
  id SERIAL PRIMARY KEY,
  system_code TEXT NOT NULL,
  field_name TEXT NOT NULL,
  description TEXT,
  data_type TEXT,
  sample_values TEXT[],
  UNIQUE(system_code, field_name)
);

-- Fields that could not be translated; values are deliberately not stored.
CREATE TABLE IF NOT EXISTS unmapped_fields (
  id SERIAL PRIMARY KEY,
  department TEXT NOT NULL,
  direction TEXT NOT NULL,
  field_name TEXT NOT NULL,
  reason TEXT NOT NULL,
  ubid TEXT,
  propagation_event_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_unmapped_fields_dept ON unmapped_fields(department, field_name);

-- What each simulated destination has received, keyed by idempotency key.
CREATE TABLE IF NOT EXISTS destination_receipts (
  idempotency_key TEXT NOT NULL,
  destination TEXT NOT NULL,
  ubid TEXT,
  times_received INTEGER NOT NULL DEFAULT 1,
  first_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (idempotency_key, destination)
);

INSERT INTO schema_fields (system_code, field_name, description, data_type) VALUES
  ('SWS', 'business_name', 'Registered name of the business', 'text'),
  ('SWS', 'registered_address', 'Registered office or premises address', 'text'),
  ('SWS', 'owner_name', 'Name of the proprietor, partner or director', 'text'),
  ('SWS', 'signatory_name', 'Name of the authorised signatory', 'text'),
  ('SWS', 'signatory_pan', 'PAN of the authorised signatory', 'identifier'),
  ('SWS', 'pan', 'Permanent account number of the business', 'identifier'),
  ('SWS', 'gstin', 'GST identification number', 'identifier'),
  ('SWS', 'employee_count', 'Total number of persons employed', 'integer'),
  ('SWS', 'nature_of_business', 'Activity or line of business', 'text'),
  ('SWS', 'registration_date', 'Date of registration or commencement', 'date'),
  ('SWS', 'license_expiry', 'Date until which the licence is valid', 'date'),
  -- Factories: names as used in the seeded mappings
  ('DFB', 'factory_name', 'Name of the factory', 'text'),
  ('DFB', 'occupier_name', 'Name of the occupier of the factory', 'text'),
  ('DFB', 'factory_address', 'Full postal address of the factory', 'text'),
  ('DFB', 'gst_number', 'GST registration number', 'identifier'),
  ('DFB', 'pan_card_no', 'PAN of the occupier or company', 'identifier'),
  ('DFB', 'total_workers', 'Maximum number of workers employed', 'integer'),
  ('DFB', 'certificate_validity', 'Licence valid up to', 'date'),
  ('DFB', 'manager_name', 'Name of the manager', 'text'),
  -- Shops and establishments: labels from Form A, Karnataka Shops and Commercial Establishments Rules
  ('SEA', 'establishment_name', 'Name of the establishment', 'text'),
  ('SEA', 'postal_address', 'Postal address of the establishment', 'text'),
  ('SEA', 'proprietor_name', 'Name of the proprietor, managing partner or director', 'text'),
  ('SEA', 'manager_name', 'Name of the head of the unit, authorised signatory or manager', 'text'),
  ('SEA', 'nature_of_business', 'Nature of business', 'text'),
  ('SEA', 'commencement_date', 'Date of commencement of business', 'date'),
  ('SEA', 'employees_total', 'Number of employees, total', 'integer'),
  -- Labour
  ('DOL', 'establishment_name', 'Name of the establishment', 'text'),
  ('DOL', 'employer_name', 'Name of the employer', 'text'),
  ('DOL', 'employer_address', 'Address of the establishment', 'text'),
  ('DOL', 'workforce_strength', 'Number of workers', 'integer'),
  ('DOL', 'proprietor_pan', 'PAN of the employer', 'identifier'),
  -- Municipal trade licence: names follow the BBMP trade-licence data
  ('BBMP', 'trade_name', 'Trade name', 'text'),
  ('BBMP', 'property_address', 'Address of the premises', 'text'),
  ('BBMP', 'applicant_full_name', 'Name of the applicant', 'text'),
  ('BBMP', 'taxpayer_gstin', 'GSTIN of the trader', 'identifier'),
  ('BBMP', 'trade_type', 'Major trade category', 'text'),
  ('BBMP', 'licence_valid_till', 'Licence valid till', 'date'),
  -- Commercial taxes
  ('CTD', 'legal_name', 'Legal name of the business', 'text'),
  ('CTD', 'principal_place_of_business', 'Address of the principal place of business', 'text'),
  ('CTD', 'gstin', 'GSTIN', 'identifier'),
  ('CTD', 'pan', 'PAN', 'identifier'),
  ('CTD', 'authorised_signatory', 'Name of the authorised signatory', 'text'),
  -- Pollution control board
  ('KPCB', 'unit_name', 'Name of the industry or unit', 'text'),
  ('KPCB', 'unit_address', 'Location of the unit', 'text'),
  ('KPCB', 'occupier_name', 'Name of the occupier', 'text'),
  ('KPCB', 'industry_category', 'Category of the industry', 'text'),
  ('KPCB', 'consent_validity', 'Consent valid up to', 'date'),
  -- Fire services
  ('KFES', 'building_name', 'Name of the building or premises', 'text'),
  ('KFES', 'premises_address', 'Address of the premises', 'text'),
  ('KFES', 'owner_name', 'Name of the owner or occupier', 'text'),
  ('KFES', 'noc_valid_upto', 'No-objection certificate valid up to', 'date'),
  -- Industrial safety
  ('DIS', 'unit_name', 'Name of the unit', 'text'),
  ('DIS', 'address', 'Address of the unit', 'text'),
  ('DIS', 'occupier', 'Name of the occupier', 'text'),
  ('DIS', 'workers_employed', 'Number of workers employed', 'integer')
ON CONFLICT (system_code, field_name) DO NOTHING;

-- Demo identifier links: each seeded business is linked to its record in every department.
INSERT INTO business_departments (business_id, department_id, dept_record_id, link_status)
SELECT b.id, d.id, d.code || '-' || lpad(row_number() OVER (PARTITION BY d.id ORDER BY b.ubid)::text, 5, '0'), 'confirmed'
FROM businesses b CROSS JOIN departments d
ON CONFLICT (business_id, department_id) DO NOTHING;
