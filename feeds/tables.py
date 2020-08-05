from sqlalchemy import Column, String, Integer, Text, Date
from sqlalchemy import MetaData

from feeds.connectors import Base, engine


meta_data = MetaData(bind=engine, reflect=True)
GdacsTable = meta_data.tables['gdacs_data']
HazardTable = meta_data.tables['hazard_data']
AcledTable = meta_data.tables['acled_data']
