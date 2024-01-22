import heapq
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from typing import List, Tuple
from urllib.parse import urlencode

from data.dao import query_latest_listings
from models import db_sessionmaker, SKUListing, SKU
from tasks.utils import split_into_segments

logger = logging.getLogger(__name__)

# Max 14 workers because of DB config of 5 concurrent + 10 overflow connections
NUM_WORKERS = 14

ORDER_SHIPPING_COST = 2
ORDER_PROCESSING_COST = 5
PURCHASE_TAX_PERCENT = Decimal(0.089)
SALE_FEE_AMOUNT = Decimal(0.12)


def compute_top_pickups(data: List[Tuple[int, List[SKUListing]]], limit: int) -> List[Tuple[int, List[SKUListing]]]:
    # Filter out all cards that don't have more than a page of listings because these are probably some
    # random rare prize card bullshit
    data = filter(lambda x: len(x[1]) >= 10, data)
    data_dict = dict(data)

    heap = []

    for sku_id, listings in data_dict.items():
        listings = sorted(listings, key=lambda listing: listing.price)

        # Assume we can sell the item at the listing price at the bottom of the page.
        total_purchase_price = listings[0].total_price * (1 + PURCHASE_TAX_PERCENT)

        potential_profit_price = listings[9].price * (1 - SALE_FEE_AMOUNT) - ORDER_PROCESSING_COST - ORDER_SHIPPING_COST

        potential_profit_percent = ((potential_profit_price / total_purchase_price) / total_purchase_price) * 100

        if potential_profit_percent < 0:
            continue

        if len(heap) < limit:
            heapq.heappush(heap, (sku_id, potential_profit_percent))
        else:
            if potential_profit_percent > heap[0][1]:
                heapq.heapreplace(heap, (sku_id, potential_profit_percent))

    top_pickup_sku_ids = sorted(heap, reverse=True)

    return top_pickup_sku_ids


def find_potential_pickups():
    session = db_sessionmaker()
    latest_listings = query_latest_listings(session)

    sku_id_to_listings_dict = defaultdict(list)

    for listing in latest_listings:
        sku_id_to_listings_dict[listing.sku_id].append(listing)

    sku_id_to_listings_segments = split_into_segments(list(sku_id_to_listings_dict.items()), NUM_WORKERS)

    top_sku_ids_with_range = []

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(compute_top_pickups, segment, 20) for segment in sku_id_to_listings_segments]

        for future in as_completed(futures):
            top_sku_ids_with_range += future.result()

    top_sku_ids_with_range = dict(sorted(top_sku_ids_with_range, key=lambda x: x[1], reverse=True))

    top_skus = [session.get(SKU, sku_id) for sku_id in top_sku_ids_with_range.keys()]

    for sku in top_skus:
        card = sku.card

        parameters = urlencode(
            query={
                "Printing": sku.printing.name,
                "Condition": sku.condition.name,
            }
        )

        logger.debug(
            f"{card.name} {card.set.name} {top_sku_ids_with_range[sku.id]} {f'www.tcgplayer.com/product/{card.id}?{parameters}'}"
        )


if __name__ == "__main__":
    find_potential_pickups()
