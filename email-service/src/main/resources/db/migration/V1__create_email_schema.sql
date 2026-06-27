CREATE TABLE email_log (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL,
    recipient   VARCHAR(255) NOT NULL,
    subject     VARCHAR(255) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_email_log_order_id ON email_log (order_id);

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    event_type   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);