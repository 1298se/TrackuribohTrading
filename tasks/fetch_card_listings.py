import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import groupby
from typing import List, Dict

from sqlalchemy.orm import Session

from models import db_sessionmaker, SKU, Condition
# from models.sku_listing import SKUListing
from models.sku_listing import SKUListing
from models.sku_listings_batch_aggregate_data import SKUListingsBatchAggregateData
from services.tcgplayer_listing_service import get_product_active_listings
from tasks.log_runtime_decorator import log_runtime
from tasks.custom_types import CardRequestData, SKUListingResponse
from tasks.utils import paginateWithBackoff, split_into_segments

logger = logging.getLogger(__name__)

NUM_WORKERS = 14


def _insert_listing_data(
        session: Session,
        sku_listing_responses: List[SKUListingResponse],
        timestamp: datetime,
):
    sku_listings = [
        SKUListing.from_tcgplayer_response(listing_response, timestamp) for listing_response in sku_listing_responses
    ]

    session.add_all(sku_listings)

    sku_listings_groupby = groupby(sku_listings, lambda listing: listing.sku_id)

    for sku_id, listings in sku_listings_groupby:
        listings = list(listings)

        sku_id_to_batch_aggregate_data = SKUListingsBatchAggregateData(
            sku_id=sku_id,
            timestamp=timestamp,
            lowest_listing_price=min([listing.price + listing.shipping_price for listing in listings]),
            total_listings_count=len(listings),
            total_copies_count=sum([listing.quantity for listing in listings])
        )

        session.add(sku_id_to_batch_aggregate_data)

    session.commit()


def download_product_listings(requests: List[CardRequestData], timestamp: datetime):
    session = db_sessionmaker()

    for request in requests:
        listings = get_product_active_listings(request)

        _insert_listing_data(session, listings, timestamp)


def fetch_card_listings(
    requests: List[CardRequestData],
):
    start_time = datetime.utcnow()
    logger.info(f'Fetching listings for {len(requests)} requests')

    card_request_data_segments = split_into_segments(requests, NUM_WORKERS)

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        [executor.submit(download_product_listings, segment, start_time) for segment in card_request_data_segments]


@log_runtime
def fetch_all_near_mint_card_listing_data():
    session = db_sessionmaker()

    """
        Fetches all near mint listings for given printings. Currently this isn't job isn't affected by rate limiting.

        For a specific card, we only care its Near Mint variations. However, a card may have many printings that are
        Near Mint, so we need to aggregate all those printings for each card.
    """
    near_mint_condition: Condition = session.query(Condition).filter(Condition.name == "Near Mint").first()
    near_mint_skus = session.query(SKU).filter(SKU.condition_id == near_mint_condition.id)
    card_id_to_skus_list = [(sku.card_id, sku) for sku in near_mint_skus]

    card_id_to_skus_dict = defaultdict(list)

    for key, value in card_id_to_skus_list:
        card_id_to_skus_dict[key].append(value)

    near_mint_skus_requests = [
        CardRequestData(
            product_id=skus[0].card_id,
            conditions=[near_mint_condition.name],
            printings=[sku.printing.name for sku in skus],
        ) for skus in card_id_to_skus_dict.values()
    ]

    fetch_card_listings(near_mint_skus_requests)


if __name__ == "__main__":
    fetch_all_near_mint_card_listing_data()
