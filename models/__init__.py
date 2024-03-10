import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

# We need to import new models to have them automatically created
from models.card import Card
from models.condition import Condition
from models.printing import Printing
from models.set import Set
from models.sku import SKU
from models.sku_listing import SKUListing
from models.card_sale import CardSale
from models.card_sync_data import CardSyncData
from models.sku_listings_batch_aggregate_data import SKUListingsBatchAggregateData

load_dotenv()

DATABASE_URI = os.environ.get("DATABASE_URI")
engine = create_engine(DATABASE_URI, future=True)

Base.metadata.create_all(engine)

db_sessionmaker = sessionmaker(engine)

LISTINGS_CHUNK_TIME_INTERVAL = '1 day'
SALES_CHUNK_TIME_INTERVAL = '7 day'

create_sku_listing_hypertable_sql = text(f"SELECT create_hypertable('{SKUListing.__tablename__}',"
                                         f"'timestamp',"
                                         f"if_not_exists => TRUE);"
                                         )

create_card_sales_hypertable_sql = text(f"SELECT create_hypertable('{CardSale.__tablename__}',"
                                        f"'order_date',"
                                        f"if_not_exists => TRUE);"
                                        )

create_sku_listings_batch_aggregate_data_hypertable_sql = text(
    f"SELECT create_hypertable('{SKUListingsBatchAggregateData.__tablename__}',"
    f"'timestamp',"                                               
    f"if_not_exists => TRUE);"
)

with engine.connect() as connection:
    connection.execute(create_sku_listing_hypertable_sql)
    connection.execute(create_sku_listings_batch_aggregate_data_hypertable_sql)
    connection.execute(create_card_sales_hypertable_sql)

    connection.execute(text(f"SELECT add_retention_policy('{SKUListing.__tablename__}', INTERVAL '7 days')"))

    connection.commit()
