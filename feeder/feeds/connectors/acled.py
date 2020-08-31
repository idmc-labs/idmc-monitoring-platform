from sqlalchemy import desc

from feeds.collectors.acled import ACLEDFeed
from feeds.connectors import session, engine
from feeds.tables import AcledTable


def runner():
    print(f'Fetching existing ACLED entries', end='\n\n')
    existing_entries = set([
        # data intersection is checked against following fields
        int(each.data_id) for each in
        session.query(AcledTable).
            order_by(desc(AcledTable.c.event_date)).
            with_entities(AcledTable.c.data_id).
            filter(AcledTable.c.data_id!=None)
    ])
    last_entry = session.query(AcledTable).\
        order_by(desc(AcledTable.c.event_date)).\
        with_entities(AcledTable.c.event_date).\
        filter(AcledTable.c.event_date!=None).limit(1)[0].event_date
    print(f'The last stored data in the ACLED is from {last_entry}', end='\n\n')
    print('Collecting the feeds for ACLED api...', end='\n\n')
    print('This can be network intensive in the case that the last entry was distant', end='\n\n')

    temp = []
    page = 1
    while True:
        print(f'page={page}')
        feeds = ACLEDFeed(page=page).get_feeds()
        new_feeds = list(filter(lambda feed: int(feed['data_id']) not in existing_entries, feeds))
        if not new_feeds:
            break
        temp += new_feeds
        page += 1

    print(f'\nFound {len(temp)} new feeds from acled', end='\n\n')
    engine.execute(AcledTable.insert(), temp)
