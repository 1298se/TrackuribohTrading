from datetime import timedelta, datetime
from typing import Optional, List

from sqlalchemy import DateTime, and_, desc, func, text, asc
from sqlalchemy.orm import Session, Query

from models import db_sessionmaker, SKUListingsBatchAggregateData
from models.card_sale import CardSale


def query_most_recent_sale_timestamp_for_card(session: Session, card_id) -> Query:
    return session.query(CardSale.order_date) \
        .filter(CardSale.card_id == card_id) \
        .order_by(desc(CardSale.order_date))


def get_sales_count_since_date(session: Session, card_id: int, date: datetime):
    return session.query(func.count()).filter(
        and_(CardSale.card_id == card_id, CardSale.order_date >= date)
    ).scalar()


def get_top_lowest_listing_price_changes_past_3_days(session: Session):
    start_date = datetime.now() - timedelta(days=3)

    return session.query(
        SKUListingsBatchAggregateData.sku_id,
        (func.first(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp) -
        func.last(SKUListingsBatchAggregateData.lowest_listing_price, SKUListingsBatchAggregateData.timestamp))
            .label("price_change"),
    ).filter(SKUListingsBatchAggregateData.timestamp >= start_date) \
        .group_by(SKUListingsBatchAggregateData.sku_id) \
        .order_by(desc("price_change")) \
        .limit(20) \
        .all()


def query_most_recent_lowest_listing_price(session: Session, card_id) -> Query:
    return session.query(SKUListingsBatchAggregateData.lowest_listing_price) \
        .filter(SKUListingsBatchAggregateData.sku.card.id == card_id) \
        .order_by(desc(CardSale.order_date))


def query_most_recent_lowest_listing_price(session: Session, card_id: int, delta: timedelta):
    start_date = datetime.now() - delta

    return session.query(SKUListingsBatchAggregateData.lowest_listing_price) \
        .filter(SKUListingsBatchAggregateData.timestamp <= start_date) \
        .order_by(asc(SKUListingsBatchAggregateData.timestamp))


if __name__ == "__main__":
    print(get_top_lowest_listing_price_changes_past_3_days(session=db_sessionmaker()))
