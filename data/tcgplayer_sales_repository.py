from datetime import date, datetime

import pytz
from bson import CodecOptions
from pymongo import MongoClient


class TCGPlayerSalesRepository:
    def __init__(self, mongodb_client: MongoClient):
        self.collection = mongodb_client.get_database('YGOPricing').get_collection('ProductSalesHistory')

    def insert_product_sales(self, product_id: int, sales_results: dict):
        new_sales = sales_results['sales']
        if len(new_sales) == 0:
            print("No new sales for %d" % product_id)
            return
        latest_sale_timestamp = new_sales[0]['orderDate']

        parsed_sales = list(map(lambda x: {
            'quantity': int(x['quantity']),
            'orderDate': x['orderDate'],
            'price': x['purchasePrice'],
            'shippingPrice': x['shippingPrice'],
        }, new_sales))

        self.collection.update_one(
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
        sales = self.collection \
            .with_options(codec_options=CodecOptions(tz_aware=True, tzinfo=pytz.timezone('UTC'))) \
            .find_one({'productId': product_id, 'condition': condition, 'printing': printing})
        return sales['latestSale'] if sales is not None else datetime.min.replace(tzinfo=pytz.UTC)
