ALTER TABLE urls ADD COLUMN click_count BIGINT NOT NULL DEFAULT 0;

CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox_events(id) WHERE published_at IS NULL;
