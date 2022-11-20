"""
    In this strategy, we identify and buy all items that we can flip for profit.
    As of 2022-11-20, the TCGPlayer Marketplace Commission Fee is 10.25%, and the payment processing fee is 2.5% + $0.3.
    Since California state tax is 9.5%, a card is profitable we can sell it for approximately 1.34 times + $0.3
    the buying price.

    Since we have a list of pricing history, we can use a forecasting model to predict the next selling price of a card.
    Then, we can just see if there's listings that we should be picking up!
"""
import os

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from repositories.tcgplayer_listing_repository import TCGPlayerListingRepository

load_dotenv()
mongo_client = MongoClient(os.environ.get("ATLAS_URI"))
db = mongo_client.get_database('YGOPricing')

tcgplayer_listing_repository = TCGPlayerListingRepository(mongo_client)
