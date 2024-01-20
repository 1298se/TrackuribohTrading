import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Dict

from models import db_sessionmaker, SKU, Condition
from models.sku_listing import SKUListing
from models.sku_listings_batch_aggregate_data import SKUListingsBatchAggregateData
from services.tcgplayer_listing_service import get_product_active_listings
from tasks.log_runtime_decorator import log_runtime
from tasks.types import CardRequestData, SKUListingResponse
from tasks.utils import paginateWithBackoff

logger = logging.getLogger(__name__)

session = db_sessionmaker()


def _insert_listing_data(
        sku_listing_responses: List[SKUListingResponse],
        timestamp: datetime,
):
    sku_id_to_listing_response_dict: Dict[int, List[SKUListingResponse]] = defaultdict(list)

    for response in sku_listing_responses:
        sku_id_to_listing_response_dict[int(response['productConditionId'])].append(response)

    sku_id_to_batch_aggregate_data_dict = {sku_id: SKUListingsBatchAggregateData(
        sku_id=sku_id,
        timestamp=timestamp,
        lowest_listing_price=min(map(lambda response: response['price'] + response['sellerShippingPrice'], responses)),
        total_listings_count=len(responses),
        total_copies_count=int(sum(map(lambda response: response['quantity'], responses)))
    ) for sku_id, responses in sku_id_to_listing_response_dict.items()}

    session.add_all(sku_id_to_batch_aggregate_data_dict.values())

    session.commit()

    sku_listings = map(
        lambda response: SKUListing.from_tcgplayer_response(
            response,
            timestamp,
        ),
        sku_listing_responses,
    )

    print(sku_id_to_batch_aggregate_data_dict)

    session.add_all(sku_listings)


def fetch_card_listings(
        requests: list[CardRequestData],
):
    start_time = datetime.utcnow()
    logger.info(f'Fetching listings for {len(requests)} requests')

    # This doesn't seem to have a rate limit...
    paginateWithBackoff(
        total=len(requests),
        paginate_fn=lambda offset: get_product_active_listings(requests[offset]),
        pagination_size=1,
        on_paginated=lambda active_listings: _insert_listing_data(
            active_listings,
            start_time
        )
    )

    session.commit()


@log_runtime
def fetch_all_near_mint_card_listing_data():
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

    near_mint_skus_requests = list(
        map(
            lambda skus: CardRequestData(
                product_id=skus[0].card_id,
                conditions=[near_mint_condition.name],
                printings=[sku.printing.name for sku in skus],
            ),
            card_id_to_skus_dict.values(),
        )
    )

    fetch_card_listings(near_mint_skus_requests)


if __name__ == "__main__":
    fetch_all_near_mint_card_listing_data()
