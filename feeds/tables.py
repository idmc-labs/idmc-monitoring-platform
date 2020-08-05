from sqlalchemy import Column, String, Integer, Text, Date
from sqlalchemy import MetaData

from feeds.connectors import Base, engine


meta_data = MetaData(bind=engine, reflect=True)
Gdacs = meta_data.tables['gdacs_data']
