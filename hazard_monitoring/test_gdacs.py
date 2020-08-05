


# FME PythonCaller template v1.5 - dr@inser.ch
# from fmeobjects import *
# import urllib2

from urllib.request import urlopen
import pandas as pd

import json
from math import *

# baseurl="http://www.gdacs.org/XML/RSS.xml?profile=ARCHIVE&fromarchive=true&from=2017-01-01&to=2017-01-01"



class FeatureProcessor(object):
    def __init__(self):
        pass

    def input(self, feature):

        i = 0
        # url = feature.getAttribute('url')
        baseurl="http://www.gdacs.org/XML/RSS.xml?profile=ARCHIVE&fromarchive=true&from={}&to={}"
        url = baseurl
        year=2017
        daterange = pd.date_range("{}-01-01".format(year),"{}-12-31".format(year))
        # print(daterange)
        for date in daterange:
            day=date.strftime("%Y-%m-%d")
            new_url = url.format(day,day)
            print(new_url)
        #     response = urlopen(feature.getAttribute('url'))
        #
        # while True:
        #     i += 1
        #     new_url = url.format()
        # response = urlopen(feature.getAttribute('url'))
        # data = json.load(response)
        # number_of_features = len(data['data'])
        #
        # if number_of_features == 500:
        #     feature.setAttribute('url', new_url)
        #     self.pyoutput(feature)
        # else:
        #     feature.setAttribute('url', new_url)
        #     self.pyoutput(feature)
        #     break

    def close(self):
        pass

feature="yo"
test= FeatureProcessor()
test.input(feature)
