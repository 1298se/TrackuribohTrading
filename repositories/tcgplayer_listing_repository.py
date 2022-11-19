from datetime import datetime, timedelta, date

import pytz
from bson import CodecOptions
from pymongo import MongoClient


def round_to_hour(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return t.replace(second=0, microsecond=0, minute=0, hour=t.hour) + timedelta(hours=t.minute // 30)


class TCGPlayerListingRepository:
    def __init__(self, mongodb_client: MongoClient):
        self.db = mongodb_client["YGOPricing"]

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

        self.db["ProductListingHourlyHistory"].insert_one({
            'timestamp': hour_timestamp,
            'metadata': metadata,
            'lowestPrice': lowest_price,
            'listingCount': listing_count,
            'copiesCount': copies_count,
            'listings': parsed_listings
        })

    def insert_product_sales(self, product_id: int, sales_results: dict):
        new_sales = sales_results['sales']
        if len(new_sales) == 0:
            print("No new sales for %d" % product_id)
            return
        latest_sale_timestamp = new_sales[0]['orderDate']

        parsed_sales = list(map(lambda x: {
            'quantity': int(x["quantity"]),
            'orderDate': x["orderDate"],
            'price': x["purchasePrice"],
            'shippingPrice': x["shippingPrice"],
        }, new_sales))

        self.db['ProductSalesHistory'].update_one(
            {
                'productId': product_id,
                'condition': sales_results['condition'],
                'printing': sales_results['printing']
            },
            {
                '$set': {'latestSale': latest_sale_timestamp},
                '$push': {'sales': {'$each': parsed_sales}},
            },
            upsert=True
        )

    def get_product_latest_sale_date(self, product_id: int, condition: str, printing: str) -> date:
        sales = self.db.get_collection('ProductSalesHistory') \
            .with_options(codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('UTC'))) \
            .find_one({'productId': product_id, 'condition': condition, 'printing': printing})
        return sales['latestSale'] if sales is not None else datetime.min.replace(tzinfo=pytz.UTC)
