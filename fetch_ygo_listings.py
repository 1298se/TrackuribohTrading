#!/usr/bin/python
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable

from dotenv import dotenv_values, load_dotenv

import argparse

from pymongo import MongoClient

from repositories.tcgplayer_listing_repository import TCGPlayerListingRepository
from services.tcgplayer_listing_service import get_listings, filter_duplicate_sales, get_sales

parser = argparse.ArgumentParser()

parser.add_argument("inputfile", help="Input file", type=str)
parser.add_argument("--listings", default=10, help="Listings count", type=int)
parser.add_argument("--sales", default=25, help="Sales count", type=int)
parser.add_argument("-o", "--output", help="Output directory", type=str)
parser.set_defaults(filter_custom=False)

MAX_WORKERS = 48


def parallel_execute(items: [dict], fun: Callable, **kwargs):
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fun, item, **kwargs): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            item_id = item['product_id']
            result, err = future.result()
            if err is not None:
                print("Error downloading %s for id: %d" % (fun.__name__, item_id))
            else:
                results[item_id] = result

    return results


def download(listing_repository: TCGPlayerListingRepository, items, listing_count, sales_count, config: dict):
    start_time = datetime.utcnow()
    listings = parallel_execute(items, get_listings, count=listing_count, config=config)
    print("%s listings downloaded" % len(listings))

    for (product_id, listing_results) in listings.items():
        listing_repository.insert_product_listings(product_id=product_id, listing_results=listing_results)

    sales = parallel_execute(items, get_sales, count=sales_count, config=config)
    print("%s sales downloaded" % len(sales))

    for (product_id, sale_results) in sales.items():
        filtered_sales = filter_duplicate_sales(product_id, sale_results, listing_repository)
        sale_results["sales"] = filtered_sales
        listing_repository.insert_product_sales(product_id=product_id, sales_results=sale_results)
    # store(filtered_sales, mode="sales", database=database, path=path)

    print("Completed in:", (datetime.utcnow() - start_time))


args = parser.parse_args()

items_file_path = args.inputfile

f = open(items_file_path)
data = json.load(f)

if not isinstance(data, list):
    sys.exit("File must be a json array of numbers")
else:
    listings_count = args.listings
    sales_count = args.sales

    config = dict()

    if args.output is not None:
        output_dir = args.output + "/"
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
            os.mkdir(output_dir + "listings/")
            os.mkdir(output_dir + "sales/")
        config['path'] = output_dir

    load_dotenv()
    mongo_client = MongoClient(os.environ.get("ATLAS_URI"))
    tcgplayer_listing_repository = TCGPlayerListingRepository(mongo_client)

    config['filter_custom'] = args.filter_custom

    download(tcgplayer_listing_repository, data, listings_count, sales_count, config)
