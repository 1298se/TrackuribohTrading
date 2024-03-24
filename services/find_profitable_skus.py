import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from itertools import groupby
from typing import List, Tuple

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from data.dao import query_latest_listings
from models import db_sessionmaker, SKU
from models.card_sale import CardSale
from models.sku_listing import SKUListing
from models.sku_max_profit import SKUMaxProfit
from services.tcgplayer_listing_service import get_sales
from tasks.custom_types import CardRequestData
from tasks.log_runtime_decorator import log_runtime
from tasks.utils import split_into_segments

logger = logging.getLogger(__name__)

session = db_sessionmaker()

NUM_WORKERS = 14
"""Max 14 workers because of DB config of 5 concurrent + 10 overflow connections"""
TAX = Decimal(0.10)
SELLER_COST = Decimal(0.85)


@dataclass
class ProfitData:
    max_profit: Decimal
    num_cards: int
    cost: Decimal


@dataclass
class SkuProfitData:
    sku_id: int
    profit_data: ProfitData


def compute_profit_from_listings(listings: List[SKUListing], quantity_limit: int) -> ProfitData:
    max_profit_for_cards = ProfitData(Decimal(0.0), 0, Decimal(0.0))
    running_cost = Decimal(0.0)
    running_quantity = 0
    for i in range(len(listings) - 1):
        listing = listings[i]

        num_cards_to_buy_from_listing = min(quantity_limit - running_quantity, listing.quantity)

        running_cost += listing.price * num_cards_to_buy_from_listing + listing.seller_shipping_price
        running_quantity += num_cards_to_buy_from_listing

        next_listing = listings[i + 1]
        shipping_cost = 4 if next_listing.price >= 40 else 0

        # We assume we can sell all the cards we've bought at the next listing price
        revenue = (Decimal(next_listing.price) - shipping_cost) * running_quantity * SELLER_COST

        total_cost = running_cost * (1 + TAX)

        profit = (revenue - total_cost)
        if profit > max_profit_for_cards.max_profit:
            max_profit_for_cards = ProfitData(profit, running_quantity, running_cost)

        if running_quantity == quantity_limit:
            # cannot buy anymore card
            break

    return max_profit_for_cards


def compute_max_profit_for_listings(listings:  List[SKUListing], purchase_copies_limit: int | None = None) -> ProfitData:
    sorted_listings_by_cost = sorted(listings, key=lambda x: x.price + x.seller_shipping_price, reverse=False)

    num_cards = sum(
        listing.quantity for listing in listings
    ) if purchase_copies_limit is None else purchase_copies_limit

    return compute_profit_from_listings(sorted_listings_by_cost, num_cards)


def get_listings_dict(sku_ids: List[int]) -> dict[int, List[SKUListing]]:
    listings = query_latest_listings(session, sku_ids)
    listings = groupby(listings, key=lambda x: x.sku_id)

    return {
        sku_id: listings
        for sku_id, listings_ in listings
    }


@log_runtime
def compute_max_potential_profit_for_skus(sku_ids: List[int]) -> List[SkuProfitData]:
    profitable_skus: List[SkuProfitData] = []

    listings = get_listings_dict(sku_ids)
    for sku_id, listings_for_sku in listings.items():

        card_profit_data = compute_max_profit_for_listings(listings_for_sku)

        if card_profit_data.max_profit > 0:
            print(card_profit_data.max_profit)
        if card_profit_data.max_profit >= 1:
            profitable_skus.append(SkuProfitData(sku_id, card_profit_data))

    return profitable_skus


@log_runtime
def get_profitable_skus() -> List[SkuProfitData]:
    listing_sku_ids = session.scalars(select(SKU.id)).all()
    print(len(listing_sku_ids))

    sku_id_segments = split_into_segments(list(listing_sku_ids), NUM_WORKERS)

    profitable_skus_with_profit: List[SkuProfitData] = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(compute_max_potential_profit_for_skus, segment) for segment in sku_id_segments]

        for future in as_completed(futures):
            profitable_skus_with_profit += future.result()

    print("len", len(profitable_skus_with_profit))

    profitable_skus_with_profit = list(
        sorted(profitable_skus_with_profit, key=lambda x: float(x.profit_data.max_profit / x.profit_data.cost), reverse=True)
    )[:200]

    return profitable_skus_with_profit


def determine_num_copies_sold_per_day(sku_id: int) -> Decimal:
    sku: SKU | None = session.get(SKU, sku_id)
    if not sku:
        raise ValueError("No sku")

    card_request = CardRequestData(
        product_id=sku.card.id,
        printings=[sku.printing_id],
        conditions=[sku.condition_id],
    )

    sales = get_sales(request=card_request, time_delta=timedelta(hours=6))
    if not sales:
        return Decimal(0)

    grouped_sales_by_day = groupby(sales, key=lambda sale: CardSale.parse_response_order_date(sale['orderDate']).date())

    num_sales_by_day = [sum(sale['quantity'] for sale in sales) for (_, sales) in grouped_sales_by_day]

    avg_sales_per_day = Decimal(sum(num_sales_by_day) * 4)

    return avg_sales_per_day


@log_runtime
def get_good_looking_skus(profitable_skus: List[SkuProfitData]) -> List[SkuProfitData]:
    good_looking_profits = []
    listings_dict = get_listings_dict([sku_data.sku_id for sku_data in profitable_skus])
    for sku_data in profitable_skus:
        print(sku_data)
        sku_id = sku_data.sku_id
        num_copies_sold_per_day = determine_num_copies_sold_per_day(sku_id=sku_id)

        # TODO: room for optimization. We currently just use the 6-day sales count as the number of copies we can buy
        sku_profit_data = compute_max_profit_for_listings(listings_dict[sku_id], int(num_copies_sold_per_day * 3))

        print(sku_profit_data)
        if sku_profit_data.max_profit >= 1:
            good_looking_profits.append(SkuProfitData(sku_id, sku_profit_data))

    good_looking_profits = list(sorted(good_looking_profits, key=lambda x: x.profit_data.max_profit, reverse=True))

    return good_looking_profits


@log_runtime
def find_profitable_skus() -> None:
    session.query(SKUMaxProfit).delete()

    profitable_skus = get_profitable_skus()

    good_skus = get_good_looking_skus(profitable_skus)

    values = [
        dict(
            sku_id=sku_profit_data.sku_id,
            max_profit=sku_profit_data.profit_data.max_profit,
            num_cards=sku_profit_data.profit_data.num_cards,
            cost=sku_profit_data.profit_data.cost,
        )
        for sku_profit_data in good_skus
    ]
    if not values:
        return

    stmt = insert(SKUMaxProfit).values(values)
    session.execute(stmt)
    session.commit()


if __name__ == "__main__":
    find_profitable_skus()
