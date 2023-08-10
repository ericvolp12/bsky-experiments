CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    initiator_did TEXT NOT NULL,
    target_did TEXT NOT NULL,
    event_type TEXT NOT NULL,
    post_uri TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expired_at TIMESTAMPTZ,
    concluded_at TIMESTAMPTZ,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    results JSONB
);
CREATE TABLE point_assignments (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL,
    actor_did TEXT NOT NULL,
    points INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, actor_did)
);
CREATE INDEX point_assignments_actor_did_idx ON point_assignments (actor_did);
