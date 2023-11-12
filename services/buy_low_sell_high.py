"""
    In this strategy, we identify and buy all items that we can flip for profit.
    As of 2022-11-20, the TCGPlayer Marketplace Commission Fee is 10.25%, and the payment processing fee is 2.5% + $0.3.
    Since California state tax is 9.5%, a card is profitable we can sell it for approximately 1.34 times + $0.3
    the buying price.

    Since we have a list of pricing history, we can use a forecasting model to predict the next selling price of a card.
    Then, we can just see if there's listings that we should be picking up!
"""
import datetime
import json
import os
from datetime import timedelta

import pymongo
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as DBSession
from statsmodels.tsa.holtwinters import ExponentialSmoothing

load_dotenv()
mongo_client = MongoClient(os.environ.get("ATLAS_URI"))
db = mongo_client.get_database('YGOPricing')
listings_collection = db.get_collection("ProductListingHourlyHistory")

SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URI")
engine = create_engine(SQLALCHEMY_DATABASE_URI, future=True)
db_session = DBSession(engine)


def get_past_month_daily_documents(card):
    recent_listings_documents = listings_collection.find(
        filter={
            'metadata': {
                'condition': card['condition'],
                'printing': card['printing'],
                'productId': card['product_id'],
            },
            'timestamp': {'$gt': (datetime.datetime.now() - timedelta(days=30))}
        },
        projection={'timestamp': True, 'copiesCount': True, 'listings': True},
        sort=[('timestamp', pymongo.DESCENDING)])

    return list(filter(lambda x: x['timestamp'].hour == 23, recent_listings_documents))


with open("steals.txt", 'w') as output_file:
    f = open("money_cards.json")
    money_cards = json.load(f)

    for card in money_cards:
        past_month_hourly_docs = get_past_month_daily_documents(card)
        past_month_hourly_copies = [doc['copiesCount'] for doc in past_month_hourly_docs]

        past_month_hourly_copies_deltas = [copies - past_month_hourly_copies[i - 1] for i, copies in enumerate(past_month_hourly_copies)][1:]
        # print(past_month_hourly_copies_deltas)
        ses_model = ExponentialSmoothing(past_month_hourly_copies_deltas, trend="add").fit(smoothing_level=0.5, smoothing_trend=0.5)

        y_pred = ses_model.forecast(5)

        # print(y_pred)

        current_listings = past_month_hourly_docs[0]['listings']

        forecast_delta = -sum(y_pred)
        i = 0
        qty = forecast_delta

        while current_listings[i]['quantity'] < qty:
            qty -= current_listings[i]['quantity']
            i += 1

        output_file.write(str(card['product_id']) + " current: " + str(current_listings[0]['price']) + " predicted: " + str(current_listings[i]['price']) + "\n")
