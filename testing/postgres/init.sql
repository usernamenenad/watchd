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

-- A second projection covering PostgreSQL type classes beyond text/jsonb, so
-- the value-encoding contract (docs/semantics.md, issue #32) can be verified
-- end to end: every column still crosses pgoutput as its PostgreSQL text
-- representation, whatever its underlying type.
CREATE TYPE value_encoding_status AS ENUM ('active', 'inactive');

CREATE TABLE value_encoding_projection (
    tenant_id UUID NOT NULL,
    item_id UUID NOT NULL,
    status value_encoding_status NOT NULL,
    tags TEXT[] NOT NULL,
    payload BYTEA,
    metadata JSONB,
    is_active BOOLEAN NOT NULL,
    quantity INTEGER NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (tenant_id, item_id)
);

ALTER TABLE value_encoding_projection REPLICA IDENTITY DEFAULT;

-- A wide matrix of PostgreSQL type classes, one column each, so the value
-- encoding contract can be checked against measured output rather than
-- assumed from a SQL `column::text` cast (which is known to disagree with
-- the wire representation for at least boolean). TIMESTAMPTZ is
-- deliberately excluded: its text output depends on the replication
-- session's TimeZone setting, not a fixed per-type contract, so it is not a
-- meaningful fit for a deterministic CI fixture.
CREATE DOMAIN value_encoding_nonempty_text AS TEXT CHECK (VALUE <> '');

CREATE TABLE type_matrix_projection (
    tenant_id UUID NOT NULL,
    item_id UUID NOT NULL,
    smallint_val SMALLINT NOT NULL,
    bigint_val BIGINT NOT NULL,
    numeric_val NUMERIC(12, 4) NOT NULL,
    real_val REAL NOT NULL,
    double_val DOUBLE PRECISION NOT NULL,
    bool_val BOOLEAN NOT NULL,
    timestamp_val TIMESTAMP NOT NULL,
    date_val DATE NOT NULL,
    time_val TIME NOT NULL,
    interval_val INTERVAL NOT NULL,
    inet_val INET NOT NULL,
    cidr_val CIDR NOT NULL,
    int_array_val INTEGER[] NOT NULL,
    char_val CHAR(8) NOT NULL,
    domain_val value_encoding_nonempty_text NOT NULL,
    PRIMARY KEY (tenant_id, item_id)
);

ALTER TABLE type_matrix_projection REPLICA IDENTITY DEFAULT;

CREATE PUBLICATION watchd_publication
    FOR TABLE tenant_permissions_projection, value_encoding_projection, type_matrix_projection
    WITH (publish = 'insert, update, delete');

GRANT CONNECT ON DATABASE watchd TO watchd_app, watchd_replicator;
GRANT USAGE ON SCHEMA public TO watchd_app, watchd_replicator;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_permissions_projection, value_encoding_projection, type_matrix_projection TO watchd_app;
GRANT SELECT ON tenant_permissions_projection, value_encoding_projection, type_matrix_projection TO watchd_replicator;
