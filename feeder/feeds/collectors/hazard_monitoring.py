import csv
import uuid
import json
import re
import requests
from typing import List

import pygeohash as geohash

from feeds import XMLToJSONParser
from feeds.collectors.gdacs import GDACSFeed
from feeds.countries import countries


class HazardMonitoringFeed(GDACSFeed):
    """
    It uses the same response as from GDACS
    And few features are annotated into it
    """
    SHAPE_FILE = 'feeds/countries/TM_WORLD_BORDERS-0.3.shp'

    IDMC_HAZARD_TRANSLATOR = 'https://raw.githubusercontent.com/idmc-labs/idmc-monitoring-platform/master/hazard_monitoring/IDMC_Hazard_Types_Translator.csv'
    SOURCE = 'GDACS'
    SOURCE_TYPE = 'Disaster'

    # geohash precision
    GEOHASH_PRECISION = 9

    # gdacs keys
    EVENT_TYPE = 'eventtype'
    GDACS_EVENT_DESCRIPTION = 'description'
    GDACS_COUNTRY = 'country'
    GDACS_FROM_DATE = 'fromdate'
    # idmc keys
    IDMC_EVENT_NAME = 'idmc_event_name'
    IDMC_GDACS_EVENT_TYPE = 'GDACS_eventtype'
    IDMC_HAZARD_ID = 'IDMC_id'
    OUTPUT_IDMC_HAZARD_ID = 'hazard_type_id'
    IDMC_HAZARD_TYPE = 'IDMC_type'
    OUTPUT_IDMC_HAZARD_TYPE = 'hazard_type'
    # new keys
    DISPLACEMENT_MENTIONED = 'displacement_mentioned'
    LOCATION_ID = 'location_id'
    # to transform dates to ISO
    DATE_FEATURES = ('fromdate', 'pubDate', 'todate')
    # key transformation
    KEY_TRANSFORM = {
        'fromdate': 'start_date',
        'glide': 'glide_number',
        'todate': 'end_date',
        'pubDate': 'pub_date',
        'eventid': 'event_id',
        'eventname': 'event_name',
        'eventname': 'event_name',
        'eventtype': 'hazard_code_source',
        'alertlevel': 'alert_score',
        'population': 'affected',
        # country info from GDACS
        'country': 'countries_affected',
        # country info from GDAL shape file
        'country_gdal': 'country',
        'link': 'source_url',
    }

    def fetch_hazard_types(self):
        r = requests.get(self.IDMC_HAZARD_TRANSLATOR)
        reader = csv.reader([each.decode() for each in r.iter_lines()])
        headers = next(reader)
        hazard_types = []
        for line in reader:
            hazard_types.append(dict(zip(headers, line)))
        return hazard_types

    def map_idmc_hazard(self, items: List):
        hazard_types = self.fetch_hazard_types()
        for item in items:
            idmc_hazard = list(filter(lambda x: x[self.IDMC_GDACS_EVENT_TYPE] == item[self.EVENT_TYPE], hazard_types))
            item[self.OUTPUT_IDMC_HAZARD_ID] = None
            item[self.OUTPUT_IDMC_HAZARD_TYPE] = None
            if idmc_hazard:
                item[self.OUTPUT_IDMC_HAZARD_ID] = idmc_hazard[0][self.IDMC_HAZARD_ID]
                item[self.OUTPUT_IDMC_HAZARD_TYPE] = idmc_hazard[0][self.IDMC_HAZARD_TYPE]

    def annotate_is_displacement_mentioned(self, item: dict) -> bool:
        return {self.DISPLACEMENT_MENTIONED: bool(re.search('displace|destro|idp', item[self.GDACS_EVENT_DESCRIPTION]))}

    def annotate_event_occurring_country(self, item: dict) -> dict:
        """
        NeighborFinder using WD
        https://gist.githubusercontent.com/matemies/fe0be35f4fcbd4ae2adde41241b21b2f/raw/1864d9f06f6d7a33526232533965d31ea77a3360/WD_UltraSimplified_ADM0.geojson)
        Which will annotate, followings
            - OBJECTID, ISO3, ISO2, Short_name,lat, log, name, region, iso
            - also, distance and angles
        ... out of which most are removed except iso3 and short_name

        We will be using https://github.com/che0/countries
        """
        country_checker = countries.CountryChecker(self.SHAPE_FILE)
        country = country_checker.getCountry(countries.Point(
            float(item['Point'][0]['lat']),
            float(item['Point'][1]['long']))
        )
        if country is None:
            return {
                'iso3': None,
                'iso3_affected': None,
                'country_gdal': None
            }
        return {
            'iso3': country.iso,
            'iso3_affected': None,
            'country_gdal': country.name
        }

    def annotate_geohash(self, item: dict) -> dict:
        # todo clean this
        return {self.LOCATION_ID: geohash.encode(float(item['Point'][0]['lat']),
                                                 float(item['Point'][1]['long']),
                                                 precision=self.GEOHASH_PRECISION)
                }

    def annotate_uuid(self, *a) -> dict:
        return {'uuid_hazard': str(uuid.uuid4())}

    def annotate_lat_long(self, item) -> dict:
        return {
            'latitude': item['Point'][0]['lat'],
            'longitude': item['Point'][1]['long'],
        }

    def annotate_source(self, item) -> dict:
        return {
            'source_name': self.SOURCE,
            'displacement_type': self.SOURCE_TYPE,
        }

    def annotate_idmc_event_name(self, item) -> dict:
        return {
            self.IDMC_EVENT_NAME: f'{item[self.GDACS_COUNTRY] or ""}: {item[self.OUTPUT_IDMC_HAZARD_TYPE] or ""} - {item[self.GDACS_FROM_DATE] or ""}'
        }

    def annotate_undocumented(self, item) -> dict:
        """
        FME has the following exisiting fields undocumented
        """
        return {
            'hazard_type_source': None,
            'exposed_50': None,
            'exposed_20': None,
            'exposed_5': None,
            'evacuations': None,
            'homeless': None,
            'injured': None,
            'fatalities': None,
            # comment is generated by fme's rss reader, so # todo
            'comment': "",
        }

    def annotate_attributes(self, items: List):
        annotate_funcs = [
            self.annotate_is_displacement_mentioned,
            self.annotate_geohash,
            self.annotate_uuid,
            self.annotate_lat_long,
            self.annotate_source,
            self.annotate_idmc_event_name,
            self.annotate_undocumented,
            self.annotate_event_occurring_country,
        ]
        for func in annotate_funcs:
            items = list(map(lambda item: {**item, **func(item)}, items))
        return items

    def get_feeds(self):
        json_feeds = XMLToJSONParser(url=self.URL)()
        items = self.get_items(json_feeds)
        self.map_idmc_hazard(items)
        items = self.filter_duplicate_items(items)
        items = self.format_date_features(items)
        items = self.annotate_attributes(items)
        items = self.map_features(items)
        return items


if __name__ == '__main__':
    feeds = HazardMonitoringFeed().get_feeds()[0]
    print(json.dumps(feeds))