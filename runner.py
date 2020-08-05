from feeds.connectors.gdacs import runner as gdacs_runner
from feeds.connectors.hazard import runner as hazard_runner
from feeds.connectors.acled import runner as acled_runner


if __name__ == "__main__":
    gdacs_runner()
    hazard_runner()
    acled_runner()
