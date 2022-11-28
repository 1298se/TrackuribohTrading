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

import numpy as np
import pandas as pd
import pymongo
from dotenv import load_dotenv
from matplotlib import dates
from pandas import DataFrame
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as DBSession

from models.card import Card
from models.set import Set
from models.condition import Condition
from models.printing import Printing
from models.rarity import Rarity
from models.sku import Sku
from utils.stats import remove_outliers_iqr, get_past_week_data, shift_series_by_time_delta

load_dotenv()
mongo_client = MongoClient(os.environ.get("ATLAS_URI"))
db = mongo_client.get_database('YGOPricing')
sales_collection = db.get_collection('ProductSalesHistory')
listings_collection = db.get_collection("ProductListingHourlyHistory")

SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URI")
engine = create_engine(SQLALCHEMY_DATABASE_URI, future=True)
db_session = DBSession(engine)

sales_documents = sales_collection.find()

with open("steals.txt", 'w') as output_file:
    output = []

    for sales_document in sales_documents:

        for sale in sales_document['sales']:
            sale['totalPrice'] = sale['price'] + sale['shippingPrice']

        sales_df = DataFrame(sales_document['sales'])

        last_week_sales_df = get_past_week_data(sales_df, 'orderDate')
        last_week_sales_df = remove_outliers_iqr(last_week_sales_df, 'totalPrice')

        if last_week_sales_df.empty:
            continue

        last_week_sold_quantity = last_week_sales_df['quantity'].sum()

        median_sale_price = last_week_sales_df['totalPrice'].median()

        x = shift_series_by_time_delta(last_week_sales_df['orderDate'], timedelta(days=7))
        y = last_week_sales_df['totalPrice']
        z = np.polynomial.polynomial.polyfit(x, y, 1)

        condition = sales_document['condition']
        printing = sales_document['printing']
        product_id = sales_document['productId']

        recent_listings_document = listings_collection.find_one({
            'metadata': {
                'condition': condition,
                'printing': printing,
                'productId': product_id,
            },
        }, sort=[('timestamp', pymongo.DESCENDING)])

        recent_listings = recent_listings_document['listings']

        for listing in recent_listings:
            listing_total_price = listing['price'] + listing['sellerShippingPrice']
            if listing_total_price < median_sale_price:
                card = db_session.get(Card, product_id)

                output_dict = {
                    'name': card.name,
                    'set_name': card.set.name,
                    'lowest_listing_price': round(listing_total_price, 2),
                    'median_sales_price': round(median_sale_price, 2),
                    'weekly_sold_quantity': int(last_week_sold_quantity),
                    'sales_trendline': round(z[1], 2),
                    'link': f'tcgplayer.com/product/{product_id}'
                }
                output.append(output_dict)
                break

    json.dump(output, output_file, indent=4)
