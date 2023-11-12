import os

from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

# We need to import new models to have them automatically created
from models.card import Card
from models.condition import Condition
from models.printing import Printing
from models.set import Set
from models.sku import SKU
from models.sku_listing_data import SKUListingData
from models.sku_listing import SKUListing
from models.card_sale import CardSale

load_dotenv()

token = os.environ.get("INFLUXDB_TOKEN")
org = "Dev"
host = "https://us-east-2-1.aws.cloud2.influxdata.com"

sku_listing_history_db_client = InfluxDBClient3(host=host, token=token, org=org, database="sku_pricing_history")

DATABASE_URI = os.environ.get("DATABASE_URI")

engine = create_engine(DATABASE_URI, future=True)

Base.metadata.create_all(engine)

db_sessionmaker = sessionmaker(engine)

create_sku_listing_data_hypertable_sql = text(f"SELECT create_hypertable('{SKUListingData.__tablename__}',"
                                              f"'timestamp',"
                                              f"chunk_time_interval => interval '1 day',"
                                              f"if_not_exists => TRUE);"
                                              )

create_sku_listing_hypertable_sql = text(f"SELECT create_hypertable('{SKUListing.__tablename__}',"
                                         f"'timestamp',"
                                         f"chunk_time_interval => interval '1 day',"
                                         f"if_not_exists => TRUE);"
                                         )

create_card_sales_hypertable_sql = text(f"SELECT create_hypertable('{SKUListing.__tablename__}',"
                                        f"'order_date',"
                                        f"chunk_time_interval => INTERVAL '1 day',"
                                        f"if_not_exists => TRUE);"
                                        )
with engine.connect() as connection:
    connection.execute(create_sku_listing_data_hypertable_sql)
    connection.execute(create_sku_listing_hypertable_sql)
    connection.execute(create_card_sales_hypertable_sql)

    connection.commit()
