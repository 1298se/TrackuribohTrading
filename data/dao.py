from datetime import timedelta, datetime
from typing import List, Tuple

from sqlalchemy import and_, desc, func, asc, select
from sqlalchemy.orm import Session, Query

from models import db_sessionmaker, SKUListingsBatchAggregateData, SKU, SKUListing
from models.card_sale import CardSale
from tasks.log_runtime_decorator import log_runtime


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


def get_top_lowest_listing_price_changes_past_3_days(session: Session):
    start_date = datetime.now() - timedelta(days=3)

    return session.query(
        SKU,
        ((func.last(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp) -
          func.first(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp)) /
         func.first(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp))
            .label("price_change"),
    ).join(SKUListingsBatchAggregateData.sku) \
        .filter(SKUListingsBatchAggregateData.timestamp >= start_date) \
        .group_by(SKU.card_id, SKUListingsBatchAggregateData.sku_id) \
        .order_by(desc("price_change")) \
        .subquery()


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


def get_listing_sku_ids(session: Session) -> List[int]:
    return session.scalars(select(SKUListing.sku_id).distinct()).all()


def get_latest_listings_for_skus(session: Session, sku_ids: List[int]) -> List[SKUListing]:
    latest_timestamp_subquery: datetime = session.query(
        func.last(SKUListing.timestamp, SKUListing.timestamp).label('latest_timestamp')).scalar()

    return session.query(SKUListing).filter(SKUListing.sku_id.in_(sku_ids)).filter(
        SKUListing.timestamp == latest_timestamp_subquery).all()


def get_copies_delta_for_skus(session: Session, sku_ids: List[int], delta: timedelta) -> List[Tuple[int, int]]:
    start_date = datetime.now() - delta

    return session.query(
        SKUListingsBatchAggregateData.sku_id,
        (
                func.last(
                    SKUListingsBatchAggregateData.total_copies_count,
                    SKUListingsBatchAggregateData.timestamp
                ) -
                func.first(
                    SKUListingsBatchAggregateData.total_copies_count,
                    SKUListingsBatchAggregateData.timestamp)
        )
    ).filter(SKUListingsBatchAggregateData.sku_id.in_(sku_ids)) \
        .filter(SKUListingsBatchAggregateData.timestamp >= start_date) \
        .group_by(SKUListingsBatchAggregateData.sku_id) \
        .all()


if __name__ == "__main__":
    print(get_top_lowest_listing_price_changes_past_3_days(db_sessionmaker()).all())
