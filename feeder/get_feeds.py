import json

from feeds.collectors.gdacs import GDACSFeed
from feeds.collectors.hazard_monitoring import HazardMonitoringFeed
from feeds.collectors.acled import ACLEDFeed
from feeds.countries import countries


def check_shape_file_field_names():
    from osgeo import ogr
    name = 'feeds/countries/TM_WORLD_BORDERS-0.3.shp'
    source = ogr.Open(name)
    layer = source.GetLayer()
    field_names = [field.name for field in layer.schema]
    print('available field names', field_names)


if __name__ == '__main__':
    # feeds = GDACSFeed().get_feeds()[0]
    # feeds = HazardMonitoringFeed().get_feeds()[0]
    feeds = ACLEDFeed().get_feeds()[0]
    print(json.dumps(feeds))

    # check_shape_file_field_names()
    # country_checker = countries.CountryChecker('feeds/countries/TM_WORLD_BORDERS-0.3.shp')
    # country = country_checker.getCountry(countries.Point(49.7821, 3.5708))
    # print(country.iso)
