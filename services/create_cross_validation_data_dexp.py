# import logging
# from collections import defaultdict
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from dataclasses import dataclass
# from datetime import timedelta
# from decimal import Decimal
# from itertools import groupby
# from typing import List, Dict
#
# from sqlalchemy.dialects.postgresql import insert
#
# from data.dao import get_latest_listings_for_skus, get_listing_sku_ids, get_copies_delta_for_skus
# from models import db_sessionmaker, SKU, CardSale
# from models.sku_listing import SKUListing
# from models.sku_max_profit import SKUMaxProfit
# from services.tcgplayer_listing_service import get_sales
# from tasks.custom_types import CardRequestData
# from tasks.log_runtime_decorator import log_runtime
# from tasks.utils import split_into_segments
#
# logger = logging.getLogger(__name__)
#
# session = db_sessionmaker()
#
# NUM_WORKERS = 14
# """Max 14 workers because of DB config of 5 concurrent + 10 overflow connections"""
# TAX = Decimal(0.10)
# SELLER_COST = Decimal(0.85)
# BATCH_SIZE = 1000
# MAX_PROFIT_CUTOFF_DOLLARS = 1
# COPIES_DELTA_TIME_SPAN_DAYS = 7
#
#
# @dataclass
# class ProfitData:
#     max_profit: Decimal
#     num_cards: int
#     cost: Decimal
#
#
# @dataclass
# class SkuProfitData:
#     sku_id: int
#     profit_data: ProfitData
#
#
# def compute_profit_from_listings(listings: List[SKUListing], quantity_limit: int) -> ProfitData:
#     max_profit_for_cards = ProfitData(Decimal(0.0), 0, Decimal(0.0))
#     running_cost = Decimal(0.0)
#     running_quantity = 0
#
#     if quantity_limit <= 0:
#         return max_profit_for_cards
#
#     for i in range(len(listings) - 1):
#         listing = listings[i]
#
#         num_cards_to_buy_from_listing = min(quantity_limit - running_quantity, listing.quantity)
#
#         running_cost += listing.price * num_cards_to_buy_from_listing + listing.shipping_price
#         running_quantity += num_cards_to_buy_from_listing
#
#         next_listing = listings[i + 1]
#         shipping_cost = 4 if next_listing.price >= 40 else 0
#
#         # We assume we can sell all the cards we've bought at the next listing price
#         revenue = (Decimal(next_listing.price) - shipping_cost) * running_quantity * SELLER_COST
#
#         total_cost = running_cost * (1 + TAX)
#
#         profit = (revenue - total_cost)
#         if profit > max_profit_for_cards.max_profit:
#             max_profit_for_cards = ProfitData(profit, running_quantity, running_cost)
#
#         if running_quantity == quantity_limit:
#             # cannot buy anymore card
#             break
#
#     return max_profit_for_cards
#
#
# def compute_max_profit_for_listings(listings: List[SKUListing], purchase_copies_limit: int | None = None) -> ProfitData:
#     sorted_listings_by_cost = sorted(listings, key=lambda x: x.price + x.shipping_price)
#
#     num_cards = sum(
#         listing.quantity for listing in listings
#     ) if purchase_copies_limit is None else purchase_copies_limit
#
#     return compute_profit_from_listings(sorted_listings_by_cost, num_cards)
#
#
# def get_listings_dict(sku_ids: List[int]) -> Dict[int, List[SKUListing]]:
#     sku_id_to_listings_dict: Dict[int, List[SKUListing]] = defaultdict(list)
#
#     for listing in get_latest_listings_for_skus(session, sku_ids):
#         sku_id_to_listings_dict[listing.sku_id].append(listing)
#
#     return sku_id_to_listings_dict
#
#
# def get_purchase_copies_limit_dict(sku_ids: List[int]) -> Dict[int, int]:
#     sku_id_to_copies_dict: Dict[int, int] = {}
#
#     for sku_id, copies in get_copies_delta_for_skus(session, sku_ids, timedelta(days=COPIES_DELTA_TIME_SPAN_DAYS)):
#         sku_id_to_copies_dict[sku_id] = -copies
#
#     return sku_id_to_copies_dict
#
#
# @log_runtime
# def compute_max_potential_profit_for_skus(sku_ids: List[int]) -> List[SkuProfitData]:
#     profitable_skus: List[SkuProfitData] = []
#
#     for offset in range(0, len(sku_ids), BATCH_SIZE):
#         listings_dict = get_listings_dict(sku_ids[offset:offset + BATCH_SIZE])
#         purchase_copies_limit_dict = get_purchase_copies_limit_dict(sku_ids[offset:offset + BATCH_SIZE])
#
#         for sku_id, listings_for_sku in listings_dict.items():
#             card_profit_data = compute_max_profit_for_listings(
#                 listings_for_sku,
#                 purchase_copies_limit=purchase_copies_limit_dict[sku_id]
#             )
#
#             if card_profit_data.max_profit >= MAX_PROFIT_CUTOFF_DOLLARS:
#                 profitable_skus.append(SkuProfitData(sku_id, card_profit_data))
#
#     return profitable_skus
#
#
# @log_runtime
# def get_potentially_profitable_skus() -> List[SkuProfitData]:
#     listing_sku_ids = get_listing_sku_ids(session)
#     # listing_sku_ids = [1185481]
#
#     sku_id_segments = split_into_segments(list(listing_sku_ids), NUM_WORKERS)
#
#     profitable_skus_with_profit: List[SkuProfitData] = []
#     with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
#         futures = [executor.submit(compute_max_potential_profit_for_skus, segment) for segment in sku_id_segments]
#
#         for future in as_completed(futures):
#             profitable_skus_with_profit += future.result()
#
#     return profitable_skus_with_profit
#
# @log_runtime
# def get_good_looking_skus(profitable_skus: List[SkuProfitData]) -> List[SkuProfitData]:
#     good_looking_profits = []
#     listings_dict = get_listings_dict([sku_data.sku_id for sku_data in profitable_skus])
#     for sku_data in profitable_skus:
#         sku_id = sku_data.sku_id
#
#         sku: SKU | None = session.get(SKU, sku_id)
#
#         card_request = CardRequestData(
#             product_id=sku.card.id,
#             printings=[sku.printing_id],
#             conditions=[sku.condition_id],
#         )
#
#         sales = get_sales(request=card_request, time_delta=timedelta(days=7))
#
#         if len(sales) <= 1:
#
#
#         # TODO: room for optimization. We currently just use the 3-day sales count as the number of copies we can buy
#         sku_profit_data = compute_max_profit_for_listings(listings_dict[sku_id], int(num_copies_sold_per_day * 5))
#
#         if sku_profit_data.max_profit >= 1:
#             good_looking_profits.append(SkuProfitData(sku_id, sku_profit_data))
#
#     good_looking_profits = list(sorted(good_looking_profits, key=lambda x: x.profit_data.max_profit, reverse=True))
#
#     return good_looking_profits
#
#
# @log_runtime
# def find_profitable_skus() -> None:
#     profitable_skus = get_potentially_profitable_skus()
#
#     logger.info(f'found {len(profitable_skus)} potentially profitable skus')
#
#     profitable_skus = get_good_looking_skus(profitable_skus)
#
#     logger.info(f'found {len(profitable_skus)} profitable skus')
#
#     profitable_skus = list(
#         sorted(profitable_skus, key=lambda x: float(x.profit_data.max_profit / x.profit_data.cost), reverse=True)
#     )
#
#
# if __name__ == "__main__":
#     find_profitable_skus()
