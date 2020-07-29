import json
from lxml import etree
import requests
from typing import List


class XMLToJSONParser():
    def __init__(self, url):
        self.url = url

    def fetch_content(self):
        response = requests.get(self.url)
        return response.content

    def get_json_data(self, xml_element, attributes_required: List) -> dict:
        """
        returns a json from xml element
        with 
        """
        try:
            tag = etree.QName(xml_element).localname
        except ValueError:
            # print(xml_element.tag)
            return {}
        if len(xml_element) == 0:
            # there are no children
            d = {tag: xml_element.text}
            
            if tag in attributes_required:
                # append the attributes as well
                for k, v in dict(xml_element.attrib).items():
                    d[k] = v
            return d
        else:
            d = {
                tag: [
                    self.get_json_data(child, attributes_required)
                    for child in xml_element
                ]
            }
            if tag in attributes_required:
                # append the attributes as well
                for k, v in dict(xml_element.attrib).items():
                    d[k] = v
            return d

    def get_features(self, attributes_required):
        """
        returns the json {tag: text} attributes obtained from the gdacs rss feed
        """
        content = self.fetch_content()
        root = etree.fromstring(content)
        return self.get_json_data(root, attributes_required)
    
    def __call__(self, attributes_required=list()):
        return self.get_features(attributes_required)


class GDACSFeed():
    URL = "http://dev.gdacs.org/xml/rss.xml"
    ITEM_KEY = 'item'
    ITEM_FIELDS = [
        'title', 'description', 'link', 'pubDate', 'fromDate', 'toDate', 'durationinweek', 'year', 
        'point', 'bbox', 'cap', 'eventtype', 'alertlevel', 'alertscore', 'episodeid', 'episodealertlevel', 
        'episodealertscore', 'eventid', 'eventname', 'severity',  'population', 'vulnerability', 
        'iso3', 'country', 'glide', 'mapimage', 'maplink', 'gtsimage', 'gtslink', 'version',
    ]
    ATTRIBUTES_REQUIRED = [
        'resource'
    ]

    # keys
    PUBLISH_DATE = 'pubDate'
    EVENT_ID = 'eventid'

    def get_items(self, json_feeds):
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
        # todo
        for item in items:
            item['geojson_data'] = ''
            if (resource_link := item['resources'][0]['url']).endswith('.geojson'):
                # todo get from the geojson link
                # in FMW, geojson feature is added into item
                item['geojson_data'] = requests.get(resource_link)

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

    def get_feeds(self):
        json_feeds = XMLToJSONParser(url=self.URL)(self.ATTRIBUTES_REQUIRED)
        items = self.get_items(json_feeds)
        self.fetch_geojson_resource(items)
        items = self.filter_duplicate_items(items)
        return items


if __name__ == '__main__':
    feeds = GDACSFeed().get_feeds()
    print("gdacs feed", json.dumps(feeds))
