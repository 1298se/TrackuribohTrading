from datetime import datetime, timedelta
from decimal import Decimal
from itertools import groupby
from typing import List, Optional, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import heapq
import logging
from data.dao import query_latest_listings
from models.card_sale import CardSale
from sqlalchemy.dialects.postgresql import insert

from models import db_sessionmaker, SKU, Condition
from models.printing import Printing
from models.sku_listing import SKUListing
from models.sku_max_profit import SKUMaxProfit
from services.tcgplayer_catalog_service import TCGPlayerCatalogService
from services.tcgplayer_listing_service import get_product_active_listings, get_sales
from tasks import fetch_card_listings
from tasks.custom_types import CardRequestData
from tasks.utils import split_into_segments

logger = logging.getLogger(__name__)

session = db_sessionmaker()


TAX = Decimal(0.10)
SELLER_COST = Decimal(0.85)
def determine_profit(listings: List[SKUListing], quantity_limit):
    max_profit_for_cards = (0, 0, 0)
    running_cost = 0
    running_quantity = 0
    for i in range(len(listings) - 1):
        listing = listings[i]

        cards_to_buy_from_listing = min(quantity_limit - running_quantity, listing.quantity)

        running_cost += listing.price * cards_to_buy_from_listing + listing.seller_shipping_price
        running_quantity += cards_to_buy_from_listing

        next_listing = listings[i+1]
        shipping_cost = 0
        if next_listing.price >= 40:
            shipping_cost = 4

        # We assume we can sell all the cards we've bought at the next listing price
        revenue = (next_listing.price - shipping_cost) * running_quantity * SELLER_COST

        total_cost = running_cost * (1 + TAX)
        profit = (revenue - total_cost)
        if profit > max_profit_for_cards[0]:
            max_profit_for_cards = (profit, running_quantity, running_cost)
        
        if running_quantity == quantity_limit:
            break

    return max_profit_for_cards

def determine_num_cards(listings: List[SKUListing]):
    return sum(listing.quantity for listing in listings)

def compute_max_profit(listings, purchase_copies_limit = None):
        sorted_listings = sorted(listings, key=lambda x: x.price + x.seller_shipping_price, reverse=False)

        num_cards = determine_num_cards(sorted_listings) if purchase_copies_limit is None else purchase_copies_limit
        
        return determine_profit(sorted_listings, num_cards)

def compute_max_profit_list(data: List[Tuple[int, List[SKUListing]]], limit: int):
    heap = []
    for sku_id, listings in data:
        max_profit, num_cards, cost = compute_max_profit(listings)

        if len(heap) < limit:
            heapq.heappush(heap, (sku_id, (max_profit, num_cards, cost)))
        elif max_profit > heap[0][1][0]:
            heapq.heapreplace(heap, (sku_id, (max_profit, num_cards, cost)))
    
    return heap


def determine_num_copies_sold_per_day(sku_id: int):

    sku: SKU = session.get(SKU, sku_id)

    card_request = CardRequestData(
        product_id=sku.card.id,
        printings=[sku.printing_id],
        conditions=[sku.condition_id],
    )

    sales = get_sales(request=card_request, time_delta=timedelta(days=7))
    if not sales:
        return 0

    grouped_sales_by_day = groupby(sales, key=lambda sale: CardSale.parse_response_order_date(sale['orderDate']).date())

    num_sales_by_day = [sum(sale['quantity'] for sale in sales) for (_, sales) in grouped_sales_by_day]

    avg_sales_per_day = sum(num_sales_by_day) / len(num_sales_by_day)

    return int(avg_sales_per_day)


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
        futures = [executor.submit(compute_max_profit_list, segment, 20) for segment in sku_id_to_listings_segments]

        for future in as_completed(futures):
            top_sku_profits += future.result()
        
    top_sku_profits = list(sorted(top_sku_profits, key=lambda x: x[1], reverse=True))

    print("have top skus", len(top_sku_profits))

    good_looking_profits = []
    for sku_id, _ in top_sku_profits:
        num_copies_sold_per_day = determine_num_copies_sold_per_day(sku_id=sku_id)

        ret = compute_max_profit(sku_id_to_listings_dict[sku_id], num_copies_sold_per_day)
        if ret[0] <= 1:  # at least 1 dollar in profit
            continue

        good_looking_profits.append((sku_id, ret))
        print(sku_id, ret)
    
    good_looking_profits = list(sorted(good_looking_profits, key=lambda x: x[1][0], reverse=True))

    stmt = insert(SKUMaxProfit).values(
        [
            dict(
                sku_id=sku_id,
                max_profit=profit,
                num_cards=num_cards,
                cost=cost
            )
            for sku_id, (profit, num_cards, cost) in good_looking_profits
        ]
    )
    session.execute(stmt)
    session.commit()
