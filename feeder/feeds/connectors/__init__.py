"""
Connectors store the data fetched from the internet into the database...
"""

import os

from sqlalchemy import create_engine, desc, asc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_USER = os.environ.get('POSTGRES_USER', 'postgres')
DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')
DB_PORT = os.environ.get('POSTGRES_PORT', '5432')
DB_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
DB_NAME = os.environ.get('POSTGRES_NAME', 'postgres')
DB_SCHEMA = os.environ.get('POSTGRES_SCHEMA', 'monitoring_platform')

print(f'Connecting postgres over {DB_HOST}:{DB_PORT} as user={DB_USER}...')

engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
    connect_args={'options': f'-csearch_path={DB_SCHEMA}'},
    # echo=True
)

Base = declarative_base()
Base.metadata.create_all(bind=engine)

Session = sessionmaker(engine)
session = Session()
