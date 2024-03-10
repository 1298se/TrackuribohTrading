from decimal import Decimal
from typing import List, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import heapq
import logging
from data.dao import query_latest_listings
from sqlalchemy.dialects.postgresql import insert

from models import db_sessionmaker, SKU, Condition
from models.printing import Printing
from models.sku_listing import SKUListing
from models.sku_max_profit import SKUMaxProfit
from services.tcgplayer_catalog_service import TCGPlayerCatalogService
from services.tcgplayer_listing_service import get_product_active_listings
from tasks import fetch_card_listings
from tasks.custom_types import CardRequestData
from tasks.utils import split_into_segments

logger = logging.getLogger(__name__)

session = db_sessionmaker()


TAX = Decimal(0.10)
SELLER_COST = Decimal(0.85)
def determine_profit(listings: List[SKUListing]):
    max_profit_for_cards = (0, 0, 0)
    running_cost = 0
    num_cards = 0
    for i in range(len(listings) - 1):
        listing = listings[i]
        running_cost += listing.price * listing.quantity + listing.seller_shipping_price
        num_cards += listing.quantity

        total_cost = running_cost * (1 + TAX)

        next_listing = listings[i+1]
        shipping_cost = 0
        if next_listing.price >= 40:
            shipping_cost = 4

        revenue = (next_listing.price - shipping_cost) * num_cards * SELLER_COST

        profit = (revenue - total_cost)
        if profit > max_profit_for_cards[0]:
            max_profit_for_cards = (profit, num_cards, running_cost)

    return max_profit_for_cards

def determine_num_cards(listings: List[SKUListing]):
    return len(listings)


def compute_max_profit(data: List[Tuple[int, List[SKUListing]]], limit: int):
    heap = []
    for sku_id, listings in data:
        sorted_listings = sorted(listings, key=lambda x: x.price + x.seller_shipping_price, reverse=False)

        num_cards = determine_num_cards(sorted_listings)
        
        max_profit, num_cards, cost = determine_profit(sorted_listings[:num_cards])

        if len(heap) < limit:
            heapq.heappush(heap, (sku_id, (max_profit, num_cards, cost)))
        elif max_profit > heap[0][1][0]:
            heapq.heapreplace(heap, (sku_id, (max_profit, num_cards, cost)))
    
    return heap


# Max 14 workers because of DB config of 5 concurrent + 10 overflow connections
NUM_WORKERS = 14
def find_sku_max_profit():
    session.query(SKUMaxProfit).delete()
    session.commit()

    latest_listings = query_latest_listings(session)
    sku_id_to_listings_dict: dict[int, list[SKUListing]] = defaultdict(list[SKUListing])
    for listing in latest_listings:
        sku_id_to_listings_dict[listing.sku_id].append(listing)

    sku_id_to_listings_segments = split_into_segments(list(sku_id_to_listings_dict.items()), NUM_WORKERS)

    top_sku_profits = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(compute_max_profit, segment, 20) for segment in sku_id_to_listings_segments]

        for future in as_completed(futures):
            top_sku_profits += future.result()
        
    top_sku_profits = list(sorted(top_sku_profits, key=lambda x: x[1], reverse=True))

    stmt = insert(SKUMaxProfit).values(
        [
            dict(
                sku_id=sku_id,
                max_profit=profit,
                num_cards=num_cards,
                cost=cost
            )
            for sku_id, (profit, num_cards, cost) in top_sku_profits
        ]
    )
    session.execute(stmt)
    session.commit()
