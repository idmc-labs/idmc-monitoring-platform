from datetime import date

from dateutil.parser import parse
from sqlalchemy import create_engine, desc, asc

from feeds.collectors.gdacs import GDACSFeed
from feeds.connectors import session, engine
from feeds.tables import GdacsTable


def runner():
    # data intersection is checked against following fields
    print(f'Fetching existing GDACS entries', end='\n\n')
    latest_100_entries = set([
        (each.publisheddate, each.id) for each in
        session.query(GdacsTable).
            order_by(desc(GdacsTable.c.publisheddate)).
            with_entities(GdacsTable.c.publisheddate, GdacsTable.c.id)
    ])

    print('Collecting the feeds from GDACS rss feed...', end='\n\n')
    feeds = GDACSFeed().get_feeds()
    new_feeds = list(filter(lambda feed: (feed['publisheddate'], feed['id']) not in latest_100_entries, feeds))
    print(f'Found {len(new_feeds)} new gdacs rss feeds', end='\n\n')

    engine.execute(GdacsTable.insert(), new_feeds)
