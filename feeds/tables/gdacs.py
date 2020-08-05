from sqlalchemy import Column, String, Integer, Text, Date

from feeds.connectors import Base


class Gdacs(Base):
    __tablename__ = 'gdacs_data'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    summary = Column(Text)
    copyright = Column(Text)
    publisheddate = Column(Date)
    lastupdate = Column(Date)
    category = Column(Text)
    author = Column(Text)
    content = Column(Text)
    linkurl = Column(Text)
    gdacs_country = Column(Text)
    gdacs_cap = Column(Text)
    gdacs_alertlevel = Column(Text)
    gdacs_episodeid = Column(Text)
    gdacs_eventid = Column(Integer)
    gdacs_eventname = Column(Text)
    gdacs_eventtype = Column(Text)
    gdacs_fromdate = Column(Date)
    gdacs_gtslink = Column(Text)
    gdacs_todate = Column(Date)
    gdacs_version = Column(Text)
    gdacs_year = Column(Integer)
    gdacs_glide = Column(Text)
    gdacs_vulnerability = Column(Text)
    gdacs_population = Column(Text)
    gdacs_severity = Column(Text)
    gdacs_resources = Column(Text)
    gdacs_geojson_link = Column(Text)
    gdacs_geogson = Column(Text)
    gdacs_alertlevel = Column(String)
    gdacs_cap = Column(String)
    gdacs_country = Column(String)
    gdacs_episodeid = Column(String)
    gdacs_eventid = Column(String)
    gdacs_eventname = Column(String)
    gdacs_eventtype = Column(String)
    gdacs_fromdate = Column(String)
    gdacs_glide = Column(String)
    gdacs_gtslink = Column(String)
    gdacs_population = Column(String)
    gdacs_resources = Column(String)
    gdacs_severity = Column(String)
    gdacs_todate = Column(String)
    gdacs_version = Column(String)
    gdacs_vulnerability = Column(String)
    gdacs_year = Column(String)
    publisheddate = Column(String)
    content = Column(String)
    linkurl = Column(String)
