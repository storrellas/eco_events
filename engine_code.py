from sqlalchemy import create_engine
# This tries to work around a SQLite design limitation. It's best to use PostgreSQL if you're affected
# by this as it doesn't have this limitation.
# Also see https://github.com/elemental-lf/benji/issues/11.
# Increase timeout to 60 seconds (5 seconds is the default). This will make "database is locked" errors
# due to concurrent database access less likely.
connect_args = {}
connect_args['timeout'] = 60
engine = create_engine('sqlite:///eco.db',  echo=True, connect_args=connect_args)
import logging

logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


with engine.connect() as connection:
    connection.execute("""CREATE TABLE IF NOT EXISTS eco_events (
                            datetime TEXT, 
                            event TEXT, 
                            period TEXT, 
                            region TEXT, 
                            day_of_month INTEGER, 
                            week_of_year INTEGER, 
                            eco_type INTEGER, 
                            country TEXT)""")
