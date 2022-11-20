from datetime import datetime, timedelta, date

import pytz
from bson import CodecOptions
from pymongo import MongoClient


def round_to_hour(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return t.replace(second=0, microsecond=0, minute=0, hour=t.hour) + timedelta(hours=t.minute // 30)


class TCGPlayerListingRepository:
    def __init__(self, mongodb_client: MongoClient):
        self.collection = mongodb_client.get_database("YGOPricing").get_collection("ProductListingHourlyHistory")

    def insert_product_listings(self, product_id: int, listing_results: dict):
        now = datetime.utcnow()
        hour_timestamp = round_to_hour(now)

        new_listings = listing_results['listings']

        listing_count = listing_results['total_listings']
        copies_count = listing_results['copies_count']
        lowest_price = new_listings[0][
            "price"]  # functools.reduce(lambda a, b: a if a["price"] < b["price"] else b, new_listings)n
        metadata = {
            'productId': product_id,
            'printing': listing_results['printing'],
            'condition': listing_results['condition']
        }

        parsed_listings = list(map(lambda x: {
            'verifiedSeller': x["verifiedSeller"],
            'goldSeller': x["goldSeller"],
            'listingId': int(x["listingId"]),
            'quantity': int(x["quantity"]),
            'sellerName': x["sellerName"],
            'price': x["price"],
            'sellerShippingPrice': x["sellerShippingPrice"],
        }, new_listings))

        self.collection.insert_one({
            'timestamp': hour_timestamp,
            'metadata': metadata,
            'lowestPrice': lowest_price,
            'listingCount': listing_count,
            'copiesCount': copies_count,
            'listings': parsed_listings
        })
