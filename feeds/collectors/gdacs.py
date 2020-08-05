from datetime import datetime
import json
import re
import requests
from typing import List

from dateutil.parser import parse

from feeds import XMLToJSONParser


class GDACSFeed():
    URL = "http://dev.gdacs.org/xml/rss_7d.xml"
    ITEM_KEY = 'item'
    # in order to fetch the attributes of a given xml element
    ATTRIBUTES_REQUIRED = [
        'resource'
    ]
    # some of features have their original element xml stored
    KEEP_ORIGINAL_XML = [
        'population',
        'severity',
        'vulnerability',
        'resources',
    ]
    # to transform dates into ISO-8601 format
    DATE_FEATURES = [
        'fromdate',
        'todate',
        'pubDate',
    ]

    # keys
    PUBLISH_DATE = 'pubDate'
    EVENT_ID = 'eventid'

    # key transformation to map to respective database field names
    # src -> destination
    KEY_TRANSFORM = {
        'alertlevel': 'gdacs_alertlevel',
        'cap': 'gdacs_cap',
        'country': 'gdacs_country',
        'episodeid': 'gdacs_episodeid',
        'eventid': 'gdacs_eventid',
        'eventname': 'gdacs_eventname',
        'eventtype': 'gdacs_eventtype',
        'fromdate': 'gdacs_fromdate',
        'glide': 'gdacs_glide',
        'gtslink': 'gdacs_gtslink',
        'population': 'gdacs_population',
        'resources': 'gdacs_resources',
        'severity': 'gdacs_severity',
        'todate': 'gdacs_todate',
        'version': 'gdacs_version',
        'vulnerability': 'gdacs_vulnerability',
        'year': 'gdacs_year',
        'pubDate': 'publisheddate',
        'description': 'content',
        'guid': 'id',
        'link': 'linkurl',
    }

    def get_items(self, json_feeds):
        """
        returns data points from the given feed        
        """
        items = []
        for each in json_feeds['rss'][0]['channel']:
            if self.ITEM_KEY in each.keys():
                # item found
                item_dict = dict()
                for values in each['item']:
                    item_dict.update(values)
                items.append(item_dict)
        return items

    def fetch_geojson_resource(self, items):
        # todo (async)
        for item in items:
            item['gdacs_geojson'] = ''
            item['gdacs_geojson_link'] = ''
            urls = re.findall('(?:(?:http):\/\/)?[\w/\-?=%.]+\.geojson', item['resources'])
            if urls:
                # todo verification remains
                # in FMW, geojson feature is added into item
                item['gdacs_geojson'] = (geojson := requests.get(urls[0]).content) \
                    if isinstance(geojson, str) else geojson.decode()
                item['gdacs_geojson_link'] = urls[0]

    def filter_duplicate_items(self, items):
        new_items = []
        seen = set()
        for item in items:
            if (item[self.PUBLISH_DATE], item[self.EVENT_ID]) in seen:
                continue
            else:
                seen.add((item[self.PUBLISH_DATE], item[self.EVENT_ID]))
                new_items.append(item)
        return new_items

    def map_features(self, items: List):
        """
        simply change the key names from one to another
        """
        def func(item):
            for src, dest in self.KEY_TRANSFORM.items():
                item[dest] = item.pop(src)
            return item
        return list(map(func, items))

    @classmethod
    def format_string_to_ISO_date(cls, date: str) -> str:
        return parse(date).date()

    def format_date_features(self, items: List) -> List:
        return list(map(lambda item: {
                            **item, 
                            **{date_field: self.format_string_to_ISO_date(item[date_field]) 
                                for date_field in self.DATE_FEATURES}
                        }, 
                        items))

    def get_feeds(self):
        json_feeds = XMLToJSONParser(url=self.URL)(self.ATTRIBUTES_REQUIRED, self.KEEP_ORIGINAL_XML)
        items = self.get_items(json_feeds)
        self.fetch_geojson_resource(items)
        items = self.filter_duplicate_items(items)
        items = self.format_date_features(items)
        # finally
        items = self.map_features(items)
        return items


if __name__ == '__main__':
    feeds = GDACSFeed().get_feeds()[0]
    print(json.dumps(feeds))
