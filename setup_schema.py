"""
Run this once to create the customerio schema, all workspace tables, and combined views.
"""
from sqlalchemy import text
from connection import engine

DDL = """
-- Schema
CREATE SCHEMA IF NOT EXISTS customerio;

-- ─────────────────────────────────────────
-- CUSTOMERS
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customerio.customers_sd (
    id          TEXT PRIMARY KEY,
    email       TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    attributes  JSONB,
    workspace   TEXT DEFAULT 'sd'
);
CREATE TABLE IF NOT EXISTS customerio.customers_bf (
    id          TEXT PRIMARY KEY,
    email       TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    attributes  JSONB,
    workspace   TEXT DEFAULT 'bf'
);
CREATE TABLE IF NOT EXISTS customerio.customers_bld (
    id          TEXT PRIMARY KEY,
    email       TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    attributes  JSONB,
    workspace   TEXT DEFAULT 'bld'
);

-- ─────────────────────────────────────────
-- SEGMENTS
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customerio.segments_sd (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    description TEXT,
    type        TEXT,
    created_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'sd'
);
CREATE TABLE IF NOT EXISTS customerio.segments_bf (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    description TEXT,
    type        TEXT,
    created_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'bf'
);
CREATE TABLE IF NOT EXISTS customerio.segments_bld (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    description TEXT,
    type        TEXT,
    created_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'bld'
);

-- ─────────────────────────────────────────
-- CAMPAIGNS
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customerio.campaigns_sd (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    active      BOOLEAN,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'sd'
);
CREATE TABLE IF NOT EXISTS customerio.campaigns_bf (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    active      BOOLEAN,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'bf'
);
CREATE TABLE IF NOT EXISTS customerio.campaigns_bld (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    active      BOOLEAN,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'bld'
);

-- ─────────────────────────────────────────
-- NEWSLETTERS
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customerio.newsletters_sd (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'sd'
);
CREATE TABLE IF NOT EXISTS customerio.newsletters_bf (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'bf'
);
CREATE TABLE IF NOT EXISTS customerio.newsletters_bld (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    workspace   TEXT DEFAULT 'bld'
);

-- ─────────────────────────────────────────
-- MESSAGES (incremental, one row per send)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customerio.messages_sd (
    message_id    TEXT PRIMARY KEY,
    campaign_id   INTEGER,
    campaign_name TEXT,
    customer_id   TEXT,
    email         TEXT,
    subject       TEXT,
    sent_at       TIMESTAMP,
    delivered_at  TIMESTAMP,
    opened_at     TIMESTAMP,
    clicked_at    TIMESTAMP,
    bounced_at    TIMESTAMP,
    workspace     TEXT DEFAULT 'sd'
);
CREATE TABLE IF NOT EXISTS customerio.messages_bf (
    message_id    TEXT PRIMARY KEY,
    campaign_id   INTEGER,
    campaign_name TEXT,
    customer_id   TEXT,
    email         TEXT,
    subject       TEXT,
    sent_at       TIMESTAMP,
    delivered_at  TIMESTAMP,
    opened_at     TIMESTAMP,
    clicked_at    TIMESTAMP,
    bounced_at    TIMESTAMP,
    workspace     TEXT DEFAULT 'bf'
);
CREATE TABLE IF NOT EXISTS customerio.messages_bld (
    message_id    TEXT PRIMARY KEY,
    campaign_id   INTEGER,
    campaign_name TEXT,
    customer_id   TEXT,
    email         TEXT,
    subject       TEXT,
    sent_at       TIMESTAMP,
    delivered_at  TIMESTAMP,
    opened_at     TIMESTAMP,
    clicked_at    TIMESTAMP,
    bounced_at    TIMESTAMP,
    workspace     TEXT DEFAULT 'bld'
);

-- ─────────────────────────────────────────
-- CAMPAIGN METRICS
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customerio.campaign_metrics_sd (
    campaign_id     INTEGER,
    campaign_name   TEXT,
    metric_date     DATE,
    sent            INTEGER DEFAULT 0,
    delivered       INTEGER DEFAULT 0,
    opened          INTEGER DEFAULT 0,
    clicked         INTEGER DEFAULT 0,
    bounced         INTEGER DEFAULT 0,
    unsubscribed    INTEGER DEFAULT 0,
    converted       INTEGER DEFAULT 0,
    workspace       TEXT DEFAULT 'sd',
    PRIMARY KEY (campaign_id, metric_date)
);
CREATE TABLE IF NOT EXISTS customerio.campaign_metrics_bf (
    campaign_id     INTEGER,
    campaign_name   TEXT,
    metric_date     DATE,
    sent            INTEGER DEFAULT 0,
    delivered       INTEGER DEFAULT 0,
    opened          INTEGER DEFAULT 0,
    clicked         INTEGER DEFAULT 0,
    bounced         INTEGER DEFAULT 0,
    unsubscribed    INTEGER DEFAULT 0,
    converted       INTEGER DEFAULT 0,
    workspace       TEXT DEFAULT 'bf',
    PRIMARY KEY (campaign_id, metric_date)
);
CREATE TABLE IF NOT EXISTS customerio.campaign_metrics_bld (
    campaign_id     INTEGER,
    campaign_name   TEXT,
    metric_date     DATE,
    sent            INTEGER DEFAULT 0,
    delivered       INTEGER DEFAULT 0,
    opened          INTEGER DEFAULT 0,
    clicked         INTEGER DEFAULT 0,
    bounced         INTEGER DEFAULT 0,
    unsubscribed    INTEGER DEFAULT 0,
    converted       INTEGER DEFAULT 0,
    workspace       TEXT DEFAULT 'bld',
    PRIMARY KEY (campaign_id, metric_date)
);

-- ─────────────────────────────────────────
-- MESSAGES — add newsletter columns if not present
-- ─────────────────────────────────────────
ALTER TABLE customerio.messages_sd  ADD COLUMN IF NOT EXISTS newsletter_id   INTEGER;
ALTER TABLE customerio.messages_sd  ADD COLUMN IF NOT EXISTS newsletter_name TEXT;
ALTER TABLE customerio.messages_bf  ADD COLUMN IF NOT EXISTS newsletter_id   INTEGER;
ALTER TABLE customerio.messages_bf  ADD COLUMN IF NOT EXISTS newsletter_name TEXT;
ALTER TABLE customerio.messages_bld ADD COLUMN IF NOT EXISTS newsletter_id   INTEGER;
ALTER TABLE customerio.messages_bld ADD COLUMN IF NOT EXISTS newsletter_name TEXT;

-- ─────────────────────────────────────────
-- NEWSLETTER METRICS
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customerio.newsletter_metrics_sd (
    newsletter_id   INTEGER,
    newsletter_name TEXT,
    metric_date     DATE,
    sent            INTEGER DEFAULT 0,
    delivered       INTEGER DEFAULT 0,
    opened          INTEGER DEFAULT 0,
    clicked         INTEGER DEFAULT 0,
    bounced         INTEGER DEFAULT 0,
    unsubscribed    INTEGER DEFAULT 0,
    converted       INTEGER DEFAULT 0,
    workspace       TEXT DEFAULT 'sd',
    PRIMARY KEY (newsletter_id, metric_date)
);
CREATE TABLE IF NOT EXISTS customerio.newsletter_metrics_bf (
    newsletter_id   INTEGER,
    newsletter_name TEXT,
    metric_date     DATE,
    sent            INTEGER DEFAULT 0,
    delivered       INTEGER DEFAULT 0,
    opened          INTEGER DEFAULT 0,
    clicked         INTEGER DEFAULT 0,
    bounced         INTEGER DEFAULT 0,
    unsubscribed    INTEGER DEFAULT 0,
    converted       INTEGER DEFAULT 0,
    workspace       TEXT DEFAULT 'bf',
    PRIMARY KEY (newsletter_id, metric_date)
);
CREATE TABLE IF NOT EXISTS customerio.newsletter_metrics_bld (
    newsletter_id   INTEGER,
    newsletter_name TEXT,
    metric_date     DATE,
    sent            INTEGER DEFAULT 0,
    delivered       INTEGER DEFAULT 0,
    opened          INTEGER DEFAULT 0,
    clicked         INTEGER DEFAULT 0,
    bounced         INTEGER DEFAULT 0,
    unsubscribed    INTEGER DEFAULT 0,
    converted       INTEGER DEFAULT 0,
    workspace       TEXT DEFAULT 'bld',
    PRIMARY KEY (newsletter_id, metric_date)
);

-- ─────────────────────────────────────────
-- COMBINED VIEWS
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW customerio.v_customers AS
    SELECT * FROM customerio.customers_sd
    UNION ALL
    SELECT * FROM customerio.customers_bf
    UNION ALL
    SELECT * FROM customerio.customers_bld;

CREATE OR REPLACE VIEW customerio.v_segments AS
    SELECT * FROM customerio.segments_sd
    UNION ALL
    SELECT * FROM customerio.segments_bf
    UNION ALL
    SELECT * FROM customerio.segments_bld;

CREATE OR REPLACE VIEW customerio.v_campaigns AS
    SELECT * FROM customerio.campaigns_sd
    UNION ALL
    SELECT * FROM customerio.campaigns_bf
    UNION ALL
    SELECT * FROM customerio.campaigns_bld;

CREATE OR REPLACE VIEW customerio.v_newsletters AS
    SELECT * FROM customerio.newsletters_sd
    UNION ALL
    SELECT * FROM customerio.newsletters_bf
    UNION ALL
    SELECT * FROM customerio.newsletters_bld;

CREATE OR REPLACE VIEW customerio.v_campaign_metrics AS
    SELECT * FROM customerio.campaign_metrics_sd
    UNION ALL
    SELECT * FROM customerio.campaign_metrics_bf
    UNION ALL
    SELECT * FROM customerio.campaign_metrics_bld;

CREATE OR REPLACE VIEW customerio.v_messages AS
    SELECT * FROM customerio.messages_sd
    UNION ALL
    SELECT * FROM customerio.messages_bf
    UNION ALL
    SELECT * FROM customerio.messages_bld;

CREATE OR REPLACE VIEW customerio.v_newsletter_metrics AS
    SELECT * FROM customerio.newsletter_metrics_sd
    UNION ALL
    SELECT * FROM customerio.newsletter_metrics_bf
    UNION ALL
    SELECT * FROM customerio.newsletter_metrics_bld;
"""


def setup():
    with engine.begin() as conn:
        for statement in DDL.split(';'):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
    print("Schema, tables, and views created successfully.")


if __name__ == '__main__':
    setup()
