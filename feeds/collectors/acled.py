import csv
from datetime import datetime
import uuid
import json
from lxml import etree
import re
import requests
from typing import List

import pygeohash as geohash
from dateutil.parser import parse


class ACLEDFeed():
    """
    https://acleddata.com/acleddatanew/wp-content/uploads/dlm_uploads/2019/01/API-User-Guide2020.pdf
    """
    
    URL = 'https://api.acleddata.com/acled/read?terms=accept&page={page}'
    # geohash precision
    GEOHASH_PRECISION = 9

    # keys
    DATA_ID = 'data_id'
    LATITUDE_KEY = 'latitude'
    LONGITUDE_KEY = 'longitude'

    # keys transformation to match the database fields
    KEY_TRANSFORM = dict()

    def __init__(self, page=1):
        self.url = self.URL.format(page=page)

    def fetch_content(self):
        r = requests.get(self.url)
        return r.content.decode()

    def filter_duplicate_items(self, items: List) -> List:
        new_items = []
        seen = set()
        for item in items:
            if item[self.DATA_ID] in seen:
                continue
            else:
                seen.add(item[self.DATA_ID])
                new_items.append(item)
        return new_items

    def annotate_geohash(self, item: dict) -> dict:
        return {'location_id': geohash.encode(float(item[self.LATITUDE_KEY]), float(item[self.LONGITUDE_KEY]), precision=self.GEOHASH_PRECISION)}

    def annotate_gwno(self, item: dict) -> dict:
        # todo
        return {'gwno': None}

    def annotate_uuid(self, *a) -> dict:
        return {'uuid_acled': str(uuid.uuid4())}

    def annotate_ally_actors(self, item: dict) -> dict:
        # todo check me
        return {
            'ally_actor_1': item['assoc_actor_1'],
            'ally_actor_2': item['assoc_actor_2'],
        }

    def annotate_extra_features(self, items: List) -> List:
        annotate_funcs = [
            self.annotate_ally_actors,
            self.annotate_geohash,
            self.annotate_gwno,
            self.annotate_uuid
        ]
        for func in annotate_funcs:
            items = list(map(lambda item: {**item, **func(item)}, items))
        return items

    def get_feeds(self):
        content = self.fetch_content()
        try:
            data = json.loads(content)['data']
        except Exception as e:
            print(f'Exception from the ACLED api content at {self.url}', e, 'content=', content)
            return []
        items = self.filter_duplicate_items(data)
        items = self.annotate_extra_features(items)
        return items


if __name__ == '__main__':
    feeds = ACLEDFeed().get_feeds()[0]
    print(json.dumps(feeds))
