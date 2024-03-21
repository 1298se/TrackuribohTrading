import heapq
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from decimal import Decimal
from itertools import groupby
from typing import List, Tuple

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from data.dao import query_latest_listings, query_listing_skus
from models import db_sessionmaker, SKU
from models.card_sale import CardSale
from models.sku_listing import SKUListing
from models.sku_max_profit import SKUMaxProfit
from services.tcgplayer_listing_service import get_sales
from tasks.custom_types import CardRequestData
from tasks.log_runtime_decorator import log_runtime
from tasks.utils import split_into_segments

logger = logging.getLogger(__name__)

TAX = Decimal(0.10)
SELLER_COST = Decimal(0.85)


def determine_profit(listings: List[SKUListing], quantity_limit) -> Tuple[Decimal, int, Decimal]:
    max_profit_for_cards = (Decimal(0.0), 0, Decimal(0.0))
    running_cost = Decimal(0.0)
    running_quantity = 0
    for i in range(len(listings) - 1):
        listing = listings[i]

        cards_to_buy_from_listing = min(quantity_limit - running_quantity, listing.quantity)

        running_cost += listing.price * cards_to_buy_from_listing + listing.seller_shipping_price
        running_quantity += cards_to_buy_from_listing

        next_listing = listings[i + 1]
        shipping_cost = 0
        if next_listing.price >= 40:
            shipping_cost = 4

        # We assume we can sell all the cards we've bought at the next listing price
        revenue = (Decimal(next_listing.price) - shipping_cost) * running_quantity * SELLER_COST

        total_cost = running_cost * (1 + TAX)
        profit = (revenue - total_cost)
        if profit > max_profit_for_cards[0]:
            max_profit_for_cards = (profit, running_quantity, running_cost)

        if running_quantity == quantity_limit:
            break

    return max_profit_for_cards


def compute_max_profit(session: Session, sku_id: int, purchase_copies_limit=None):
    listings = query_latest_listings(session, sku_id)

    sorted_listings = sorted(listings, key=lambda x: x.price + x.seller_shipping_price, reverse=False)

    num_cards = sum(listing.quantity for listing in listings) if purchase_copies_limit is None else purchase_copies_limit

    return determine_profit(sorted_listings, num_cards)


def compute_max_potential_profit_for_skus(sku_ids: List[int]) -> List[Tuple[int, Tuple[Decimal, int, Decimal]]]:
    session = db_sessionmaker()

    profitable_skus = []
    for sku_id in sku_ids:
        max_profit, num_cards, cost = compute_max_profit(session, sku_id)

        if max_profit >= 1:
            print(f'adding sku with {sku_id} and max profit {max_profit}')
            profitable_skus.append((sku_id, (max_profit, num_cards, cost)))

    return profitable_skus


def determine_num_copies_sold_per_day(session: Session, sku_id: int) -> Decimal:
    sku: SKU = session.get(SKU, sku_id)

    card_request = CardRequestData(
        product_id=sku.card.id,
        printings=[sku.printing_id],
        conditions=[sku.condition_id],
    )

    sales = get_sales(request=card_request, time_delta=timedelta(days=7))
    if not sales:
        return Decimal(0)

    grouped_sales_by_day = groupby(sales, key=lambda sale: CardSale.parse_response_order_date(sale['orderDate']).date())

    num_sales_by_day = [sum(sale['quantity'] for sale in sales) for (_, sales) in grouped_sales_by_day]

    avg_sales_per_day = sum(num_sales_by_day) / 7

    return avg_sales_per_day


# Max 14 workers because of DB config of 5 concurrent + 10 overflow connections
NUM_WORKERS = 14


@log_runtime
def find_sku_max_profit():
    session = db_sessionmaker()

    session.query(SKUMaxProfit).delete()

    listing_sku_ids = query_listing_skus(session)

    sku_id_segments = split_into_segments(list(listing_sku_ids), NUM_WORKERS)

    profitable_skus_with_profit = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(compute_max_potential_profit_for_skus, segment) for segment in sku_id_segments]

        for future in as_completed(futures):
            profitable_skus_with_profit += future.result()

    profitable_skus_with_profit = list(
        sorted(profitable_skus_with_profit, key=lambda x: x[1][0] / x[1][2], reverse=True)
    )[:200]

    good_looking_profits = []
    for sku_id, _ in profitable_skus_with_profit:
        num_copies_sold_per_day = determine_num_copies_sold_per_day(session, sku_id=sku_id)

        # TODO: room for optimization. We currently just use the 7-day sales count as the number of copies we can buy
        ret = compute_max_profit(session, sku_id, int(num_copies_sold_per_day * 3))

        if ret[0] >= 1:
            good_looking_profits.append((sku_id, ret))
            print(sku_id, ret)

    good_looking_profits = list(sorted(good_looking_profits, key=lambda x: x[1][0], reverse=True))

    values = [
        dict(
            sku_id=sku_id,
            max_profit=profit,
            num_cards=num_cards,
            cost=cost
        )
        for sku_id, (profit, num_cards, cost) in good_looking_profits
    ]

    if len(values) > 0:
        stmt = insert(SKUMaxProfit).values(values)
        session.execute(stmt)
        session.commit()
