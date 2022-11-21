"""
    In this strategy, we identify and buy all items that we can flip for profit.
    As of 2022-11-20, the TCGPlayer Marketplace Commission Fee is 10.25%, and the payment processing fee is 2.5% + $0.3.
    Since California state tax is 9.5%, a card is profitable we can sell it for approximately 1.34 times + $0.3
    the buying price.

    Since we have a list of pricing history, we can use a forecasting model to predict the next selling price of a card.
    Then, we can just see if there's listings that we should be picking up!
"""
import os
from datetime import timedelta

import pymongo
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as DBSession
from statsmodels.tsa.holtwinters import SimpleExpSmoothing
from traces import TimeSeries

load_dotenv()
mongo_client = MongoClient(os.environ.get("ATLAS_URI"))
db = mongo_client.get_database('YGOPricing')
sales_collection = db.get_collection('ProductSalesHistory')
listings_collection = db.get_collection("ProductListingHourlyHistory")

SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URI")
engine = create_engine(SQLALCHEMY_DATABASE_URI, future=True)
db_session = DBSession(engine)

sales_documents = sales_collection.find()

for sales_document in sales_documents:
    sale_listings = [(sale['orderDate'], sale['price'] + sale['shippingPrice']) for sale in sales_document.get('sales')]

    sale_time_series = TimeSeries(sale_listings)

    try:
        regularized_sales_timeseries = sale_time_series.moving_average(sampling_period=timedelta(hours=1), pandas=True)
        regularized_sales_timeseries.index.freq = regularized_sales_timeseries.index.inferred_freq
        sales_forecast_model = SimpleExpSmoothing(regularized_sales_timeseries, initialization_method="estimated").fit()

        forecasted_sales_price = sales_forecast_model.forecast()

        recent_listings_document = listings_collection.find_one({
            'metadata': {
                'condition': sales_document['condition'],
                'printing': sales_document['printing'],
                'productId': sales_document['productId']
            },
        }, sort=[('timestamp', pymongo.DESCENDING)])

        recent_listings = recent_listings_document['listings']

        for listing in recent_listings:
            listing_total_price = listing['price'] + listing['sellerShippingPrice']
            if listing_total_price * 1.34 + 0.3 < forecasted_sales_price[0]:
                print(recent_listings_document['metadata'], "LISTING PRICE: ", listing_total_price, "FORECASTED SALES PRICE: ", forecasted_sales_price)

    except ValueError:
        continue
