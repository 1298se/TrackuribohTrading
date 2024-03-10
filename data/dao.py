from datetime import timedelta, datetime
from typing import List
from urllib.parse import urlencode

from sqlalchemy import and_, desc, func, asc
from sqlalchemy.orm import Session, Query

from models import db_sessionmaker, SKUListingsBatchAggregateData, SKU, SKUListing
from models.card_sale import CardSale


def query_most_recent_sale_timestamp_for_card(session: Session, card_id) -> Query:
    return session.query(CardSale.order_date) \
        .filter(CardSale.card_id == card_id) \
        .order_by(desc(CardSale.order_date))


def get_sales_count_since_date(session: Session, card_id: int, date: datetime):
    return session.query(func.count()).filter(
        and_(CardSale.card_id == card_id, CardSale.order_date >= date)
    ).scalar()


def get_past_top_listings_by_listings_delta(session: Session, delta: timedelta):
    start_date = datetime.now() - delta

    copies_delta_subquery = session.query(
        SKUListingsBatchAggregateData.sku_id,
        (func.first(SKUListingsBatchAggregateData.total_listings_count, SKUListingsBatchAggregateData.timestamp) -
         func.last(SKUListingsBatchAggregateData.total_listings_count, SKUListingsBatchAggregateData.timestamp))
            .label("listings_delta"),
    ).filter(SKUListingsBatchAggregateData.timestamp >= start_date) \
        .group_by(SKUListingsBatchAggregateData.sku_id) \
        .subquery()

    return session.query(SKU, copies_delta_subquery.c.listings_delta) \
        .join(SKU, SKU.id == copies_delta_subquery.c.sku_id) \
        .filter(copies_delta_subquery.c.listings_delta > 0) \
        .order_by(desc(copies_delta_subquery.c.listings_delta)) \
        .limit(30)


# def get_top_lowest_listing_price_changes_past_3_days(session: Session):
#     start_date = datetime.now() - timedelta(days=3)
#
#     return session.query(
#         SKU,
#         ((func.last(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp) -
#           func.first(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp)) /
#          func.first(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp))
#             .label("price_change"),
#     ).join(SKUListingsBatchAggregateData.sku) \
#         .filter(SKUListingsBatchAggregateData.timestamp >= start_date) \
#         .group_by(SKU.card_id, SKUListingsBatchAggregateData.sku_id) \
#         .order_by(desc("price_change")) \
#         .subquery()


def get_skus(session: Session):
    return session.query(SKU).join(SKUListingsBatchAggregateData.sku)


def query_most_recent_lowest_listing_price(session: Session, card_id) -> Query:
    return session.query(SKUListingsBatchAggregateData.lowest_listing_price) \
        .filter(SKUListingsBatchAggregateData.sku.card.id == card_id) \
        .order_by(desc(CardSale.order_date))


def query_most_recent_lowest_listing_price(session: Session, card_id: int, delta: timedelta):
    start_date = datetime.now() - delta

    return session.query(SKUListingsBatchAggregateData.lowest_listing_price) \
        .filter(SKUListingsBatchAggregateData.timestamp <= start_date) \
        .order_by(asc(SKUListingsBatchAggregateData.timestamp))


def query_latest_listings(session: Session, sku_id: int | None = None) -> List[SKUListing]:
    query = session.query(SKUListing)

    if sku_id:
        query.filter(SKUListing.sku_id == sku_id)

    latest_timestamp_subquery = session.query(func.last(SKUListing.timestamp, SKUListing.timestamp).label('latest_timestamp')).scalar_subquery()
    query = query.filter(
        SKUListing.timestamp == latest_timestamp_subquery
    )

    return query.all()


if __name__ == "__main__":
    print("1 DAY")

    sku_ids = [str(sku.id) for (sku, copies_delta) in get_past_top_listings_by_listings_delta(session=db_sessionmaker(), delta=timedelta(days=5))]

    print(sku_ids)

    print(", ".join(sku_ids))

    for sku, copies_delta in get_past_top_listings_by_listings_delta(session=db_sessionmaker(), delta=timedelta(days=5)):
        card = sku.card
        parameters = urlencode(
            query={
                "Printing": sku.printing.name,
                "Condition": sku.condition.name,
            }
        )

        print(
            f"{sku.id} {card.name} {card.rarity_name} {card.set.name} {sku.printing.name} {sku.condition.name} {copies_delta} "
            f"{f'www.tcgplayer.com/product/{card.id}?{parameters}'}"
        )

    # print("3 DAY")
    # for sku, copies_delta in get_past_top_listings_by_listings_delta(session=db_sessionmaker(), delta=timedelta(days=3)):
    #     card = sku.card
    #
    #     parameters = urlencode(
    #         query={
    #             "Printing": sku.printing.name,
    #             "Condition": sku.condition.name,
    #         }
    #     )
    #
    #     print(
    #         f"{sku.id} {card.name} {card.rarity_name} {card.set.name} {sku.printing.name} {sku.condition.name} {copies_delta} "
    #         f"{f'www.tcgplayer.com/product/{card.id}?{parameters}'}"
    #     )
    # print("5 DAY")
    # for sku, copies_delta in get_past_top_listings_by_listings_delta(session=db_sessionmaker(), delta=timedelta(days=5)):
    #     card = sku.card
    #
    #     parameters = urlencode(
    #         query={
    #             "Printing": sku.printing.name,
    #             "Condition": sku.condition.name,
    #         }
    #     )
    #
    #     print(
    #         f"{sku.id} {card.name} {card.rarity_name} {card.set.name} {sku.printing.name} {sku.condition.name} {copies_delta} "
    #         f"{f'www.tcgplayer.com/product/{card.id}?{parameters}'}"
    #     )
