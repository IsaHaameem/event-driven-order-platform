CREATE TABLE analytics_events (
    id           UUID PRIMARY KEY,
    event_type   VARCHAR(50) NOT NULL,
    order_id     UUID NOT NULL,
    customer_id  UUID,
    status       VARCHAR(20),
    quantity     INTEGER,
    occurred_at  TIMESTAMP NOT NULL,
    received_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_analytics_events_event_type ON analytics_events (event_type);
CREATE INDEX idx_analytics_events_occurred_at ON analytics_events (occurred_at);
CREATE INDEX idx_analytics_events_order_id ON analytics_events (order_id);