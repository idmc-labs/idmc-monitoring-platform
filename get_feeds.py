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


class XMLToJSONParser():
    def __init__(self, url):
        self.url = url

    def fetch_content(self):
        response = requests.get(self.url)
        return response.content

    def get_json_data(self, xml_element, attributes_required: List, keep_original_xml=list()) -> dict:
        """
        returns a json from xml element
        with 
        """
        try:
            tag = etree.QName(xml_element).localname
        except ValueError:
            return {}
        if tag in keep_original_xml:
            return {tag: etree.tostring(xml_element).decode()}
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
                    self.get_json_data(child, attributes_required, keep_original_xml)
                    for child in xml_element
                ]
            }
            if tag in attributes_required:
                # append the attributes as well
                for k, v in dict(xml_element.attrib).items():
                    d[k] = v
            return d

    def get_features(self, attributes_required, keep_original_xml):
        """
        returns the json {tag: text} attributes obtained from the gdacs rss feed
        """
        content = self.fetch_content()
        root = etree.fromstring(content)
        return self.get_json_data(root, attributes_required, keep_original_xml)
    
    def __call__(self, attributes_required=list(), keep_original_xml=list()):
        return self.get_features(attributes_required, keep_original_xml)


class GDACSFeed():
    URL = "http://dev.gdacs.org/xml/rss.xml"
    ITEM_KEY = 'item'
    ATTRIBUTES_REQUIRED = [
        'resource'
    ]
    KEEP_ORIGINAL_XML = [
        'population',
        'severity',
        'vulnerability',
        'resources',
    ]
    DATE_FEATURES = [
        'fromdate',
        'todate',
        'pubDate',
    ]

    # keys
    PUBLISH_DATE = 'pubDate'
    EVENT_ID = 'eventid'

    # key transformation to store into the database
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
                # todo get from the geojson link
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
        return parse(date).strftime('%Y-%m-%d')

    def format_date_features(self, items: List) -> List:
        return list(map(lambda item: {
                            **item, 
                            **{date_field: self.format_string_to_ISO_date(item[date_field]) 
                                for date_field in self.DATE_FEATURES}}, 
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


class HazardMonitoringFeed(GDACSFeed):
    """
    It uses the same response as from GDACS
    And few features are annotated into it
    """

    IDMC_HAZARD_TRANSLATOR = 'https://raw.githubusercontent.com/idmc-labs/idmc-monitoring-platform/master/hazard_monitoring/IDMC_Hazard_Types_Translator.csv'

    # gdacs keys
    EVENT_TYPE = 'eventtype'
    GDACS_EVENT_DESCRIPTION = 'description'
    # idmc keys
    IDMC_GDACS_EVENT_TYPE = 'GDACS_eventtype'
    IDMC_HAZARD_ID = 'IDMC_id'
    OUTPUT_IDMC_HAZARD_ID = 'hazard_type_id'
    IDMC_HAZARD_TYPE = 'IDMC_type'
    OUTPUT_IDMC_HAZARD_TYPE = 'hazard_type'
    # new keys
    DISPLACEMENT_MENTIONED = 'displacement_mentioned'

    def fetch_hazard_types(self):
        r = requests.get(self.IDMC_HAZARD_TRANSLATOR)
        reader = csv.reader([each.decode() for each in r.iter_lines()])
        headers = next(reader)
        hazard_types = []
        for line in reader:
            hazard_types.append(dict(zip(headers, line)))
        return hazard_types

    def map_idmc_hazard(self, items: List, hazard_types: List):
        for item in items:
            idmc_hazard = list(filter(lambda x: x[self.IDMC_GDACS_EVENT_TYPE] == item[self.EVENT_TYPE], hazard_types))
            item[self.IDMC_HAZARD_ID] = None
            item[self.IDMC_HAZARD_TYPE] = None
            if idmc_hazard:
                item[self.OUTPUT_IDMC_HAZARD_ID] = idmc_hazard[0][self.IDMC_HAZARD_ID]
                item[self.OUTPUT_IDMC_HAZARD_TYPE] = idmc_hazard[0][self.IDMC_HAZARD_TYPE]

    def is_displacement_mentioned(self, item: dict) -> bool:
        return bool(re.search('displace|destro|idp', item[self.GDACS_EVENT_DESCRIPTION]))

    def get_closest_neighbor(self, item: dict) -> dict:
        """
        NeighborFinder using WD (I dont know whats WD)
        attributes annotated : OBJECTID, ISO3, ISO2, Short_name,lat, log, name, region, iso

        gdac_country = countries affected (one or more)
        short_name (above) pins down to a single country
        
        MAYBE: mainly to point out which country the event lies in
        """
        # todo

    def annotate_attributes(self, items: List):
        for item in items:
            item[self.DISPLACEMENT_MENTIONED] = self.is_displacement_mentioned(item)

    def get_feeds(self):
        json_feeds = XMLToJSONParser(url=self.URL)()
        items = self.get_items(json_feeds)
        hazard_types = self.fetch_hazard_types()
        self.map_idmc_hazard(items, hazard_types)
        self.annotate_attributes(items)
        items = self.filter_duplicate_items(items)
        return items


class ACLEDFeed():
    URL = 'https://api.acleddata.com/acled/read?terms=accept&year={year}'
    # geohash precision
    GEOHASH_PRECISION = 9

    # keys
    DATA_ID = 'data_id'
    LATITUDE_KEY = 'latitude'
    LONGITUDE_KEY = 'longitude'

    # keys transformation to match the database fields
    KEY_TRANSFORM = dict()

    def __init__(self, year):
        year = year or datetime.now().year
        self.url = self.URL.format(year=year)

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
        data = json.loads(content)['data']
        items = self.filter_duplicate_items(data)
        items = self.annotate_extra_features(items)
        return items


if __name__ == '__main__':
    feeds = GDACSFeed().get_feeds()[0]
    # feeds = HazardMonitoringFeed().get_feeds()[0]
    # feeds = ACLEDFeed(year=2020).get_feeds()[0]
    print(json.dumps(feeds))
