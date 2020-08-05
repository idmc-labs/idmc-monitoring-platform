import json

from feeds.collectors.gdacs import GDACSFeed
from feeds.collectors.hazard_monitoring import HazardMonitoringFeed
from feeds.collectors.acled import ACLEDFeed


if __name__ == '__main__':
    # feeds = GDACSFeed().get_feeds()[0]
    # feeds = HazardMonitoringFeed().get_feeds()[0]
    feeds = ACLEDFeed().get_feeds()[0]
    print(json.dumps(feeds))
