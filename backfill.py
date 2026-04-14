"""
Full historical backfill — run once.

Fetches as far back as the CIO API allows (capped at ~March 1, 2026):
  - Campaign metrics    (all history)
  - Newsletter metrics  (all history)
  - Messages            (all history — campaign + newsletter sends)

Safe to re-run: everything uses upsert, no data is wiped.
"""

import logging
import time

from pipeline import (
    WORKSPACES,
    WorkspaceClient,
    fetch_campaign_metrics,
    fetch_newsletter_metrics,
    fetch_messages,
    upsert_metrics,
    upsert_messages,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# Request 365 steps — API will return as far back as it has data (currently March 1, 2026)
MAX_STEPS = 365


def get_all_campaigns(client):
    results, cursor = [], None
    while True:
        params = {'start': cursor} if cursor else {}
        data = client.get('/campaigns', params=params)
        results.extend(data.get('campaigns', []))
        cursor = data.get('next')
        if not cursor:
            break
    return results


def get_all_newsletters(client):
    results, cursor = [], None
    while True:
        params = {'start': cursor} if cursor else {}
        data = client.get('/newsletters', params=params)
        results.extend(data.get('newsletters', []))
        cursor = data.get('next')
        if not cursor:
            break
    return results


def backfill():
    log.info('══════ Full backfill started ══════\n')

    for ws, api_key in WORKSPACES.items():
        if not api_key:
            continue

        log.info(f'── Workspace: {ws.upper()} ──')
        client = WorkspaceClient(api_key)

        campaigns   = get_all_campaigns(client)
        newsletters = get_all_newsletters(client)
        log.info(f'  {len(campaigns)} campaigns, {len(newsletters)} newsletters')

        # Campaign metrics — full history
        log.info('  Backfilling campaign metrics...')
        rows = fetch_campaign_metrics(client, campaigns, steps=MAX_STEPS)
        upsert_metrics(rows, f'campaign_metrics_{ws}', ['campaign_id', 'metric_date'])

        # Newsletter metrics — full history
        log.info('  Backfilling newsletter metrics...')
        rows = fetch_newsletter_metrics(client, newsletters, steps=MAX_STEPS)
        upsert_metrics(rows, f'newsletter_metrics_{ws}', ['newsletter_id', 'metric_date'])

        # Messages — full history (since_ts=0 fetches everything the API has)
        log.info('  Backfilling messages (this may take a few minutes)...')
        rows = fetch_messages(client, campaigns, newsletters, since_ts=0)
        upsert_messages(rows, f'messages_{ws}')

        log.info('')

    log.info('══════ Backfill complete ══════')


if __name__ == '__main__':
    backfill()
