import math
from datetime import datetime
from typing import List

from sqlalchemy import and_

from data.dao import get_most_recent_sale_timestamp_for_card
from models import db_sessionmaker, SKU, Condition
from models.card_sale import CardSale
from models.sku_listing import SKUListing
from models.sku_listing_data import SKUListingData
from jobs.types import CardSaleResponse, CardListingRequestData, SKUListingResponse
from jobs.utils import paginate
from services.tcgplayer_listing_service import get_product_active_listings, get_sales

db_session = db_sessionmaker()


def _insert_listing_data(
        sku_id_with_listing_responses: tuple[int, List[SKUListingResponse]],
        timestamp: datetime,
):
    (sku_id, listing_responses) = sku_id_with_listing_responses

    lowest_listing_price = math.inf
    total_copies = 0

    for listing in listing_responses:
        lowest_listing_price = min(
            lowest_listing_price,
            listing["price"] + listing["shippingPrice"]
        )

        total_copies += listing["quantity"]

    sku_listing_data = SKUListingData(
        timestamp=timestamp,
        sku_id=sku_id,
        lowest_price=lowest_listing_price,
        copies_count=total_copies,
        listings_count=len(listing_responses)
    )

    sku_listings = map(
        lambda response: SKUListing.from_tcgplayer_response(response, timestamp, sku_id),
        listing_responses
    )

    db_session.add(sku_listing_data)
    db_session.add_all(sku_listings)


def _insert_sales_data(
        product_id_with_sales: tuple[int, list[CardSaleResponse]]
):
    (product_id, sales) = product_id_with_sales
    db_session.add_all(map(lambda response: CardSale.from_tcgplayer_response(response, product_id), sales))


def fetch_card_listing_prices(
        listing_requests: list[CardListingRequestData],
):
    start_time = datetime.utcnow()

    paginate(
        total=len(listing_requests),
        paginate_fn=lambda offset, limit: get_product_active_listings(listing_requests[offset]),
        pagination_size=50,
        on_paginated=lambda active_listings: _insert_listing_data(
            active_listings,
            start_time
        )
    )

    print("Completed Fetching Listing Data")

    paginate(
        total=len(listing_requests),
        paginate_fn=lambda offset, limit: get_sales(
            listing_requests[offset],
            get_most_recent_sale_timestamp_for_card(db_session, listing_requests[offset]['product_id'])),
        on_paginated=lambda product_id_with_sales: _insert_sales_data(product_id_with_sales),
        pagination_size=50,
    )

    print("Completed Fetching Sales Data")
    db_session.commit()


def fetch_all_near_mint_card_listing_data():
    """
        For a specific card, we only care its Near Mint variations, hence the requests are just the SKUs
    """
    near_mint_condition: Condition = db_session.query(Condition).filter(Condition.name == "Near Mint").first()
    near_mint_skus = db_session.query(SKU).filter(SKU.condition_id == near_mint_condition.id)

    near_mint_skus_requests = list(
        map(
            lambda sku: CardListingRequestData(
                product_id=sku.card_id,
                condition=sku.condition.name,
                printing=sku.printing.name,
                sku_id=sku.id
            ),
            near_mint_skus
        )
    )

    fetch_card_listing_prices(near_mint_skus_requests)
