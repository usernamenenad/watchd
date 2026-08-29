-- Development-only database roles and a minimal projection table.
-- This script runs only when the compose volume is initialized.

CREATE ROLE watchd_app LOGIN PASSWORD 'watchd_app';
CREATE ROLE watchd_replicator LOGIN REPLICATION PASSWORD 'watchd_replicator';

CREATE TABLE tenant_permissions_projection (
    tenant_id UUID NOT NULL,
    user_id UUID NOT NULL,
    permissions JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, user_id)
);

ALTER TABLE tenant_permissions_projection REPLICA IDENTITY DEFAULT;

CREATE PUBLICATION watchd_publication FOR TABLE tenant_permissions_projection;

GRANT CONNECT ON DATABASE watchd TO watchd_app, watchd_replicator;
GRANT USAGE ON SCHEMA public TO watchd_app, watchd_replicator;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_permissions_projection TO watchd_app;
GRANT SELECT ON tenant_permissions_projection TO watchd_replicator;

