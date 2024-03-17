from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import List

import requests

from models.card_sale import CardSale
from tasks.custom_types import CardRequestData, CardSalesResponse, CardSaleResponse, SKUListingResponse

LISTING_PAGINATION_SIZE = 50

BASE_LISTINGS_URL = f'https://mp-search-api.tcgplayer.com/v1/product/%d/listings'
BASE_SALES_URL = f'https://mpapi.tcgplayer.com/v2/product/%d/latestsales'

DEFAULT_LISTINGS_CONFIG = {
    'filter_custom': False,
}

DEFAULT_SALES_CONFIG = {
    'filter_custom': False,
}

# Use MappingProxyType to make immutable
BASE_HEADERS = MappingProxyType({
    "origin": "https://www.tcgplayer.com",
    "Referer": "https://www.tcgplayer.com",
    "accept": "application/json",
    "content-type": "application/json",
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36"
})


def get_product_active_listings_request_payload(
        offset: int,
        limit: int,
        printings: List[str],
        conditions: List[str],
):
    return {
        "filters": {
            "term": {
                "sellerStatus": "Live",
                "channelId": 0,
                "language": [
                    "English"
                ],
                "printing": printings,
                "condition": conditions,
                "listingType": "standard"
            },
            "range": {
                "quantity": {
                    "gte": 1
                }
            },
            "exclude": {
                "channelExclusion": 0,
                "listingType": "custom"
            }
        },
        "from": offset,
        "size": limit,
        "context": {
            "shippingCountry": "US",
            "cart": {}
        },
        "sort": {
            "field": "price+shipping",
            "order": "asc"
        }
    }


def get_sales_request_payload(count: int, offset: int, printings: List[str], conditions: List[str]):
    return {
        "listingType": "ListingWithoutPhotos",
        "limit": count,
        "offset": offset,
        "variants": printings,
        "time": datetime.now().timestamp() * 1000,
        "conditions": conditions,
    }


def get_product_active_listings(
        request: CardRequestData,
) -> list[SKUListingResponse]:
    product_id = request['product_id']
    listings = {}
    url = BASE_LISTINGS_URL % product_id
    cur_offset = 0

    while True:
        payload = get_product_active_listings_request_payload(
            offset=cur_offset,
            limit=LISTING_PAGINATION_SIZE,
            printings=request['printings'],
            conditions=request['conditions'],
        )

        response = requests.post(url=url, json=payload, headers=BASE_HEADERS)

        response.raise_for_status()

        data = response.json()

        listing_data = data['results'][0]
        total_listings = listing_data['totalResults']
        results = listing_data['results']

        # We put the results in a set because due to data updates pagination might give us the same listing on
        # adjacent pages
        listings.update([(result['listingId'], result) for result in results])

        cur_offset += len(results)

        if cur_offset >= total_listings:
            break

    return list(listings.values())


def get_sales(request: CardRequestData, time_delta: timedelta) -> list[CardSaleResponse]:
    sales = []
    product_id = request['product_id']

    url = BASE_SALES_URL % product_id

    while True:
        # The endpoint only gives back 25 at most...
        payload = get_sales_request_payload(
            count=25,
            offset=len(sales),
             conditions=request["conditions"], 
             printings=request["printings"]
            )
        
        response = requests.post(url=url, json=payload, headers=BASE_HEADERS)

        response.raise_for_status()

        data: CardSalesResponse = response.json()

        has_new_sales = True

        for sale_response in data['data']:
            if CardSale.parse_response_order_date(sale_response["orderDate"]) >= datetime.now(tz=timezone.utc) - time_delta:
                sales.append(sale_response)
            else:
                has_new_sales = False

        if data['nextPage'] == "" or not has_new_sales:
            break

    return sales
