from data.dao import query_most_recent_sale_timestamp_for_card
from models import db_sessionmaker, CardSale
from models.card_sync_data import SyncFrequency, CardSyncData
from services.tcgplayer_listing_service import get_sales
from tasks import scheduler
from tasks.set_card_sync_data import set_card_sync_data
from tasks.custom_types import CardSaleResponse
from tasks.utils import paginateWithBackoff

session = db_sessionmaker()


def _insert_sales_data(
        product_id_with_sales: tuple[int, list[CardSaleResponse]]
):
    (product_id, sales) = product_id_with_sales
    session.add_all(map(lambda response: CardSale.from_tcgplayer_response(response, product_id), sales))


def fetch_card_sales_data_for_frequency(frequency: SyncFrequency):
    card_ids = [card[0] for card in session.query(CardSyncData.card_id)
        .filter(CardSyncData.sync_frequency == frequency)
        .all()
    ]

    paginateWithBackoff(
        total=len(card_ids),
        paginate_fn=lambda offset: get_sales(
            card_ids[offset],
            query_most_recent_sale_timestamp_for_card(session, card_ids[offset]).scalar()),
        on_paginated=lambda product_id_with_sales: _insert_sales_data(product_id_with_sales),
        pagination_size=1,
    )

    session.commit()

    scheduler.add_job(set_card_sync_data, args=[card_ids])
