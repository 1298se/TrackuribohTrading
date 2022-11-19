from datetime import datetime
from types import MappingProxyType

import requests

from repositories.tcgplayer_listing_repository import TCGPlayerListingRepository

BASE_URL = "https://mpapi.tcgplayer.com/v2/product/%d"
BASE_LISTINGS_URL = f'{BASE_URL}/listings'
BASE_SALES_URL = f'{BASE_URL}/latestsales'

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
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "sec-ch-ua": "\"Chromium\";v=\"106\", \"Google Chrome\";v=\"106\", \"Not;A=Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "accept": "application/json",
    "content-type": "application/json",
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 '
                  'Safari/537.36',
})


def get_listings(item: dict, count: int, config=None) -> (any, any):
    if config is None:
        config = DEFAULT_LISTINGS_CONFIG
    if count == 0:
        count = 1000

    item_id = item['product_id']
    listings, total_listings, copies_count = __get_listings(item_id, count, {**config, **item})

    return {
               'listings': listings,
               'condition': item['condition'],
               'printing': item['printing'],
               'total_listings': total_listings,
               'copies_count': copies_count
           }, None


def __get_listings(item_id: int, count: int, config: {}) -> ([any], int, int):
    listing_types = ["standard"]
    if config["filter_custom"]:
        listing_types.append("custom")
    url, data = __create_listings_request(
        item_id,
        count,
        listing_types,
        config["printing"],
        config["condition"]
    )
    result = requests.post(url=url, json=data, headers=BASE_HEADERS).json()
    if result["errors"]:
        raise Exception()
    results = result["results"][0]
    listings = results["results"]
    copies_count = 0

    for item in results['aggregations']['quantity']:
        copies_count = copies_count + item['value'] * item['count']

    return listings, results["totalResults"], copies_count


def __create_listings_request(item_id: int, count: int, listing_types: [str], printing: str, condition: str):
    url = BASE_LISTINGS_URL % item_id
    data = {
        "aggregations": ["listingType"],
        "context": {"shippingCountry": "US", "cart": {}},
        "filters": {"term": {"sellerStatus": "Live",
                             "channelId": 0,
                             "language": ["English"],
                             "condition": [condition],
                             "listingType": listing_types,
                             "printing": [printing]
                             },
                    "range": {"quantity": {"gte": 1}},
                    "exclude": {"channelExclusion": 0}
                    },
        "from": 0,
        "size": count,
        "sort": {"field": "price+shipping", "order": "asc"}
    }
    return url, data


# max count is 25
def get_sales(item: {}, count=25, config=None) -> ([any], any):
    if config is None:
        config = DEFAULT_SALES_CONFIG
    has_more = True
    sales = []
    page = 0
    fetch_all = count == 0

    item_id = item['product_id']

    api_count = min(count, 25)
    while has_more and (len(sales) < count or fetch_all):
        temp_sales, temp_has_more = __get_sales(item_id, count=api_count, offset=page * api_count, config={**config, **item})
        sales.extend(temp_sales)
        page = page + 1
        has_more = fetch_all and temp_has_more

    for i, sale in enumerate(sales):
        # 2022-11-14T05:00:36.436+00:00
        sale['cardId'] = item_id
        date = sale['orderDate']
        if ":" == date[-3:-2]:
            date = date[:-3] + date[-2:]
        try:
            parsed_time = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError:
            parsed_time = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
        sale['orderDate'] = parsed_time

    return {
               'sales': sales,
               'condition': item['condition'],
               'printing': item['printing']
           }, None


def filter_duplicate_sales(product_id: int, sale_results: {}, listing_repository: TCGPlayerListingRepository) -> []:
    latest_sale_timestamp = listing_repository.get_product_latest_sale_date(product_id, sale_results['condition'], sale_results['printing'])
    filtered_sales = list(filter(lambda x: x['orderDate'] > latest_sale_timestamp, sale_results['sales']))
    return filtered_sales


def __get_sales(item_id: int, count: int, offset: int, config: {}) -> ([any], bool):
    condition = 0
    if config['condition'] == 'Near Mint':
        condition = 1

    printing = 0
    if config['printing'] == '1st Edition':
        printing = 8
    elif config['printing'] == 'Unlimited':
        printing = 7

    listing_type = 'ListingWithoutPhotos' if config["filter_custom"] else 'All'
    url, data = __create_sales_request(item_id, count, offset, listing_type=listing_type, condition=condition, printing=printing)
    result = requests.post(url=url, json=data, headers=BASE_HEADERS).json()
    sales = result["data"]
    has_more = result["nextPage"] == 'Yes'
    return sales, has_more


def __create_sales_request(item_id: int, count: int, offset: int, listing_type: str, condition: int, printing: int) -> (str, dict):
    url = BASE_SALES_URL % item_id

    data = {
        "conditions": [condition],
        "languages": [1],
        "limit": count,
        "listingType": listing_type,
        "offset": offset,
        "variants": [printing]
    }

    return url, data
