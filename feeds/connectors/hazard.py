from feeds.collectors.hazard_monitoring import HazardMonitoringFeed
from feeds.connectors import session, engine
from feeds.tables import HazardTable


def runner():
    print(f'Fetching existing HAZARD entries', end='\n\n')
    existing_entries = set([
        # data intersection is checked against following fields
        (each.pub_date, each.event_id) for each in
        session.query(HazardTable).
            with_entities(HazardTable.c.pub_date, HazardTable.c.event_id)
    ])

    print('Collecting the feeds for HAZARD_MONITORING (from GDACS) rss feed...', end='\n\n')
    feeds = HazardMonitoringFeed().get_feeds()
    new_feeds = list(filter(lambda feed: (feed['pub_date'], feed['event_id']) not in existing_entries, feeds))
    print(f'Found {len(new_feeds)} new gdacs rss feeds for hazard monitoring', end='\n\n')

    engine.execute(HazardTable.insert(), new_feeds)
