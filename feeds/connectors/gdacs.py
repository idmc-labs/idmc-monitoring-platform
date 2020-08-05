import json

from sqlalchemy import create_engine, desc, asc

from feeds.collectors.gdacs import GDACSFeed
from feeds.connectors import session
from feeds.tables.gdacs import Gdacs


def runner():
    limit = 100
    # data intersection is checked against following fields
    fields = ('publisheddate', 'id')
    print(f'Fetching latest {limit} GDACS entries')
    latest_100_entries = set([
        tuple([getattr(each, field, '') for field in fields]) for each in
        session.query(Gdacs).
            order_by(desc(Gdacs.publisheddate)).
            with_entities(Gdacs.publisheddate, Gdacs.id).
            limit(limit)
    ])

    feeds = GDACSFeed().get_feeds()
    new_feeds = list(filter(lambda feed: tuple([feed[field] for field in fields]) not in latest_100_entries, feeds))
