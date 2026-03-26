"""
Customer.io full-load pipeline.
Fetches customers, segments, campaigns, and newsletters for all 3 workspaces,
loads each into workspace-specific tables in the customerio schema,
and combined views (v_customers, v_segments, v_campaigns, v_newsletters) are
always up to date via UNION ALL.

Run once manually or schedule every ~10 minutes on Heroku.
"""

import csv
import io
import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert

from connection import engine

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

US_BASE = 'https://api.customer.io/v1'
SCHEMA = 'customerio'

WORKSPACES = {
    'sd':  os.getenv('CIO_API_KEY_SD'),
    'bf':  os.getenv('CIO_API_KEY_BF'),
    'bld': os.getenv('CIO_API_KEY_BLD'),
}


# ─────────────────────────────────────────
# Workspace client (handles EU region + auth on redirects)
# ─────────────────────────────────────────

class WorkspaceClient:
    """
    Wraps requests.Session to:
    - Preserve the Authorization header on cross-domain redirects (CIO EU region)
    - Auto-detect the correct regional base URL (US vs EU)
    """

    def __init__(self, api_key):
        self._api_key = api_key
        self._session = self._make_session()
        self.base_url = self._detect_base_url()

    def _make_session(self):
        s = requests.Session()
        s.headers.update({
            'Authorization': f'Bearer {self._api_key}',
            'Content-Type': 'application/json',
        })
        api_key = self._api_key

        def rebuild_auth(prepared, response):
            prepared.headers['Authorization'] = f'Bearer {api_key}'

        s.rebuild_auth = rebuild_auth
        return s

    def _detect_base_url(self):
        """Probe /segments to detect region, then normalise to current (non-beta) URL."""
        try:
            resp = self._session.get(f'{US_BASE}/segments', allow_redirects=False, timeout=10)
            if resp.is_redirect:
                location = resp.headers.get('Location', '')
                base = location.rsplit('/segments', 1)[0]
                # beta-api-eu.customer.io is the deprecated redirect target;
                # the correct current EU base is api-eu.customer.io/v1
                base = base.replace('beta-api-eu.customer.io', 'api-eu.customer.io')
                base = base.replace('beta-api.customer.io', 'api.customer.io')
                log.info(f'  Detected EU region → {base}')
                return base
        except Exception:
            pass
        return US_BASE

    def get(self, path, params=None):
        resp = self._session.get(f'{self.base_url}{path}', params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def post(self, path, body=None, params=None):
        resp = self._session.post(f'{self.base_url}{path}', json=body or {}, params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()


# ─────────────────────────────────────────
# Extractors
# ─────────────────────────────────────────

def fetch_customers(client):
    """
    Fetch all customers via the async Exports API:
      1. POST /v1/exports/customers  → start export job
      2. Poll GET /v1/exports/{id}   → wait for 'completed'
      3. GET download_url            → download CSV, parse rows
    """
    # 1. Start export — filters is required; match all people with an email
    body = {
        'filters': {
            'and': [
                {'attribute': {'field': 'email', 'operator': 'exists'}}
            ]
        }
    }
    resp = client.post('/exports/customers', body=body)
    export_id = resp['export']['id']
    log.info(f'  Customer export started (id={export_id}), polling...')

    # 2. Poll — max 10 min (120 × 5 s); CIO status is "done" when complete
    for _ in range(120):
        time.sleep(5)
        status = client.get(f'/exports/{export_id}')
        export = status.get('export', {})
        state = export.get('status')
        log.info(f'  Export {export_id} status: {state}')
        if state == 'done':
            break
        if export.get('failed') or state == 'failed':
            raise RuntimeError(f'Customer export failed: {export}')
    else:
        raise TimeoutError('Customer export did not complete within 10 minutes')

    # 3. Get pre-signed download URL from /exports/{id}/download
    dl_meta = client.get(f'/exports/{export_id}/download')
    download_url = dl_meta.get('url')
    if not download_url:
        raise RuntimeError(f'No download URL in response: {dl_meta}')

    # 4. Download CSV (pre-signed GCS URL — no auth header needed)
    dl = requests.get(download_url, timeout=120)
    dl.raise_for_status()

    reader = csv.DictReader(io.StringIO(dl.text))
    return list(reader)


def fetch_segments(client):
    """GET /v1/segments."""
    data = client.get('/segments')
    return data.get('segments', [])


def fetch_campaigns(client):
    """GET /v1/campaigns — paginated."""
    results, cursor = [], None
    while True:
        params = {'start': cursor} if cursor else {}
        data = client.get('/campaigns', params=params)
        results.extend(data.get('campaigns', []))
        cursor = data.get('next')
        if not cursor:
            break
        time.sleep(0.15)
    return results


def fetch_newsletters(client):
    """GET /v1/newsletters."""
    data = client.get('/newsletters')
    return data.get('newsletters', [])


def fetch_campaign_metrics(client, campaigns: list) -> list:
    """
    For each campaign fetch daily metrics for the last 30 days.
    GET /v1/campaigns/{id}/metrics?period=days&steps=30
    Response: {"start": "...", "metric": {"series": {"sent": [...], "opened": [...], ...}}}
    Each array has one value per day starting from `start`.
    """
    from datetime import timedelta
    rows = []
    for campaign in campaigns:
        cid = campaign.get('id')
        cname = campaign.get('name', '')
        try:
            data = client.get(f'/campaigns/{cid}/metrics', params={'period': 'days', 'steps': 30})
            start_str = data.get('start', '')[:10]  # "2026-03-22 ..." → "2026-03-22"
            series = data.get('metric', {}).get('series', {})
            if not series or not start_str:
                continue
            start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
            n = len(series.get('sent', []))
            for i in range(n):
                rows.append({
                    'campaign_id':   cid,
                    'campaign_name': cname,
                    'metric_date':   start_date + timedelta(days=i),
                    'sent':          series.get('sent',         [0]*n)[i],
                    'delivered':     series.get('delivered',    [0]*n)[i],
                    'opened':        series.get('opened',       [0]*n)[i],
                    'clicked':       series.get('clicked',      [0]*n)[i],
                    'bounced':       series.get('bounced',      [0]*n)[i],
                    'unsubscribed':  series.get('unsubscribed', [0]*n)[i],
                    'converted':     series.get('converted',    [0]*n)[i],
                })
            time.sleep(0.15)
        except Exception as e:
            log.warning(f'  Could not fetch metrics for campaign {cid} ({cname}): {e}')
    return rows


# ─────────────────────────────────────────
# Transformers
# ─────────────────────────────────────────

def _ts(epoch):
    """Convert Unix epoch int to datetime, safely."""
    try:
        return datetime.utcfromtimestamp(int(epoch)) if epoch else None
    except (TypeError, ValueError, OSError):
        return None


KNOWN_CUSTOMER_COLS = {'id', 'email', 'created_at', 'updated_at'}

def transform_customers(raw: list, workspace: str) -> pd.DataFrame:
    # Export returns flat CSV rows; collect extra columns as attributes JSONB
    rows = []
    for c in raw:
        cid = c.get('id', '').strip()
        if not cid:
            continue  # skip rows with no id
        attributes = {k: v for k, v in c.items() if k not in KNOWN_CUSTOMER_COLS and v != ''}
        rows.append({
            'id':         cid,
            'email':      c.get('email'),
            'created_at': _ts(c.get('created_at')),
            'updated_at': _ts(c.get('updated_at')),
            'attributes': json.dumps(attributes),
            'workspace':  workspace,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=['id', 'email', 'created_at', 'updated_at', 'attributes', 'workspace']
    )


def transform_campaign_metrics(raw: list, workspace: str) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame(columns=[
            'campaign_id', 'campaign_name', 'metric_date',
            'sent', 'delivered', 'opened', 'clicked',
            'bounced', 'unsubscribed', 'converted', 'workspace'
        ])
    df = pd.DataFrame(raw)
    df['workspace'] = workspace
    return df


def transform_segments(raw: list, workspace: str) -> pd.DataFrame:
    rows = [
        {
            'id':          s.get('id'),
            'name':        s.get('name'),
            'description': s.get('description', ''),
            'type':        s.get('type'),
            'created_at':  _ts(s.get('created_at')),
            'workspace':   workspace,
        }
        for s in raw
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=['id', 'name', 'description', 'type', 'created_at', 'workspace']
    )


def transform_campaigns(raw: list, workspace: str) -> pd.DataFrame:
    rows = [
        {
            'id':         c.get('id'),
            'name':       c.get('name'),
            'active':     c.get('active'),
            'created_at': _ts(c.get('created')),
            'updated_at': _ts(c.get('updated')),
            'workspace':  workspace,
        }
        for c in raw
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=['id', 'name', 'active', 'created_at', 'updated_at', 'workspace']
    )


def transform_newsletters(raw: list, workspace: str) -> pd.DataFrame:
    rows = [
        {
            'id':         n.get('id'),
            'name':       n.get('name'),
            'created_at': _ts(n.get('created_at')),
            'updated_at': _ts(n.get('updated_at')),
            'workspace':  workspace,
        }
        for n in raw
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=['id', 'name', 'created_at', 'updated_at', 'workspace']
    )


# ─────────────────────────────────────────
# Loader
# ─────────────────────────────────────────

def load(df: pd.DataFrame, table: str, dtype: dict = None):
    if df.empty:
        log.info(f'  No data for {SCHEMA}.{table} — skipping.')
        return

    # TRUNCATE + INSERT in a single transaction so a failed insert doesn't leave an empty table
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE {SCHEMA}.{table}'))
        df.to_sql(
            table,
            conn,
            schema=SCHEMA,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500,
            dtype=dtype or {},
        )
    log.info(f'  Loaded {len(df):,} rows → {SCHEMA}.{table}')


# ─────────────────────────────────────────
# Messages (incremental)
# ─────────────────────────────────────────

def get_message_watermark(table: str) -> int:
    """Return MAX(sent_at) from the messages table as a Unix timestamp, or 0 if empty."""
    with engine.connect() as conn:
        row = conn.execute(text(
            f"SELECT EXTRACT(EPOCH FROM MAX(sent_at))::BIGINT FROM {SCHEMA}.{table}"
        )).fetchone()
    ts = row[0] if row and row[0] else 0
    return int(ts)


def fetch_messages(client, campaigns: list, since_ts: int) -> list:
    """
    Fetch all messages newer than since_ts in a single paginated stream
    (no per-campaign filter — per-campaign filtering cuts off historical pages).
    Messages are returned newest-first; pagination stops at the watermark.
    campaign_name is resolved from a local lookup built from the campaigns list.
    """
    campaign_names = {c['id']: c.get('name', '') for c in campaigns}
    rows, cursor = [], None

    while True:
        params = {'limit': 1000}
        if cursor:
            params['start'] = cursor
        data = client.get('/messages', params=params)
        messages = data.get('messages', [])

        stop = False
        for m in messages:
            created = m.get('created', 0)
            if created > 0 and created <= since_ts:
                stop = True
                break
            cid = m.get('campaign_id')
            if not cid:
                continue  # skip transactional / non-campaign messages
            metrics = m.get('metrics', {})
            rows.append({
                'message_id':    m['id'],
                'campaign_id':   cid,
                'campaign_name': campaign_names.get(cid, ''),
                'customer_id':   m.get('customer_id'),
                'email':         m.get('customer_identifiers', {}).get('email'),
                'subject':       m.get('subject'),
                'sent_at':       _ts(metrics.get('sent')),
                'delivered_at':  _ts(metrics.get('delivered')),
                'opened_at':     _ts(metrics.get('opened')),
                'clicked_at':    _ts(metrics.get('clicked')),
                'bounced_at':    _ts(metrics.get('bounced')),
            })

        cursor = data.get('next')
        if stop or not cursor or not messages:
            break
        time.sleep(0.15)

    return rows


def upsert_messages(rows: list, table: str):
    """Insert new messages; update open/click/bounce timestamps if changed."""
    if not rows:
        log.info(f'  No new messages for {SCHEMA}.{table}')
        return

    meta = MetaData()
    tbl = Table(table, meta, schema=SCHEMA, autoload_with=engine)
    stmt = pg_insert(tbl).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=['message_id'],
        set_={
            'delivered_at': stmt.excluded.delivered_at,
            'opened_at':    stmt.excluded.opened_at,
            'clicked_at':   stmt.excluded.clicked_at,
            'bounced_at':   stmt.excluded.bounced_at,
        }
    )
    with engine.begin() as conn:
        conn.execute(stmt)
    log.info(f'  Upserted {len(rows):,} messages → {SCHEMA}.{table}')


# ─────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────

ENTITIES = [
    {
        'name':      'customers',
        'fetch':     fetch_customers,
        'transform': transform_customers,
        'dtype':     {'attributes': JSONB()},
    },
    {
        'name':      'segments',
        'fetch':     fetch_segments,
        'transform': transform_segments,
    },
    {
        'name':      'campaigns',
        'fetch':     fetch_campaigns,
        'transform': transform_campaigns,
    },
    {
        'name':      'newsletters',
        'fetch':     fetch_newsletters,
        'transform': transform_newsletters,
    },
]


def run():
    log.info('══════ Customer.io pipeline started ══════')
    start_time = time.time()

    for ws, api_key in WORKSPACES.items():
        if not api_key:
            log.warning(f'No API key for workspace "{ws}" — skipping.')
            continue

        log.info(f'── Workspace: {ws.upper()} ──')

        client = WorkspaceClient(api_key)
        fetched_campaigns = []

        for entity in ENTITIES:
            name = entity['name']
            table = f"{name}_{ws}"
            try:
                raw = entity['fetch'](client)
                log.info(f'  Fetched {len(raw):,} {name}')
                df = entity['transform'](raw, ws)
                load(df, table, dtype=entity.get('dtype'))
                if name == 'campaigns':
                    fetched_campaigns = raw
            except requests.HTTPError as e:
                body = e.response.text if e.response is not None else ''
                log.error(f'  HTTP error for {name} [{ws}]: {e} — {body}')
            except Exception as e:
                log.error(f'  Unexpected error for {name} [{ws}]: {e}', exc_info=True)

        # Campaign metrics (depends on campaigns being fetched first)
        try:
            metrics_raw = fetch_campaign_metrics(client, fetched_campaigns)
            log.info(f'  Fetched {len(metrics_raw):,} campaign metric rows')
            df = transform_campaign_metrics(metrics_raw, ws)
            load(df, f'campaign_metrics_{ws}')
        except Exception as e:
            log.error(f'  Unexpected error for campaign_metrics [{ws}]: {e}', exc_info=True)

        # Messages — incremental: only fetch since last run's max sent_at
        try:
            msg_table = f'messages_{ws}'
            watermark = get_message_watermark(msg_table)
            if watermark == 0:
                log.info(f'  Messages table empty — full backfill (this may take a while)...')
            else:
                log.info(f'  Fetching messages since {datetime.utcfromtimestamp(watermark)} UTC')
            msg_rows = fetch_messages(client, fetched_campaigns, since_ts=watermark)
            upsert_messages(msg_rows, msg_table)
        except Exception as e:
            log.error(f'  Unexpected error for messages [{ws}]: {e}', exc_info=True)

    elapsed = time.time() - start_time
    log.info(f'══════ Pipeline complete in {elapsed:.1f}s ══════')


if __name__ == '__main__':
    run()
