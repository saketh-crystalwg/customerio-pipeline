"""
One-time YTD message backfill.
Fetches all messages from Jan 1, 2026 onwards for all campaigns/workspaces
and upserts them into the messages tables.

Safe to re-run — uses upsert so no duplicates.
"""

import logging
from datetime import datetime, timezone

from dotenv import load_dotenv

from pipeline import WORKSPACES, WorkspaceClient, fetch_messages, upsert_messages

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

YTD_START = int(datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())


def get_all_campaigns(client):
    """Fetch all campaigns, handling pagination."""
    campaigns, cursor = [], None
    while True:
        params = {'start': cursor} if cursor else {}
        data = client.get('/campaigns', params=params)
        campaigns.extend(data.get('campaigns', []))
        cursor = data.get('next')
        if not cursor:
            break
    return campaigns


def backfill():
    log.info('══════ YTD message backfill started ══════')
    log.info(f'Fetching messages from {datetime.utcfromtimestamp(YTD_START).date()} onwards\n')

    for ws, api_key in WORKSPACES.items():
        if not api_key:
            log.warning(f'No API key for {ws}, skipping.')
            continue

        log.info(f'── Workspace: {ws.upper()} ──')
        client = WorkspaceClient(api_key)

        campaigns = get_all_campaigns(client)
        log.info(f'  Found {len(campaigns)} campaigns')

        rows = fetch_messages(client, campaigns, since_ts=YTD_START)
        upsert_messages(rows, f'messages_{ws}')

    log.info('\n══════ Backfill complete ══════')


if __name__ == '__main__':
    backfill()
