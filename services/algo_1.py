from collections import defaultdict
import logging

from models import db_sessionmaker, SKU, Condition
from models.printing import Printing
from services.tcgplayer_catalog_service import TCGPlayerCatalogService
from services.tcgplayer_listing_service import get_product_active_listings
from tasks import fetch_card_listings
from tasks.custom_types import CardRequestData
from tasks.utils import split_into_segments

logger = logging.getLogger(__name__)

session = db_sessionmaker()



def compare_price(a, b):
    return (b["price"] + b["sellerShippingPrice"]) * 0.85 - (a["price"] + a["sellerShippingPrice"]) * 1.10

def find_outliers():
    near_mint_condition: Condition = session.query(Condition).filter(Condition.name == "Near Mint").first()
    near_mint_skus = session.query(SKU).join(SKU.printing).filter(SKU.condition_id == near_mint_condition.id, Printing.name == "1st Edition")
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

    best_seen = 0
    counter = 0
    for sku_request in near_mint_skus_requests[6900:10000]:
        counter += 1
        if counter % 100 == 0:
            print(counter)

        listings = get_product_active_listings(sku_request)
        
        sorted_listings = sorted(listings, key=lambda x: x["price"] + x["sellerShippingPrice"], reverse=False)

        MAX_NUM_CARDS = 10
        bottom_20_percent = int(len(listings) * 0.2)
        num_cards = min(MAX_NUM_CARDS, bottom_20_percent)
        if num_cards == 0:
            continue

        best_difference = 0
        for i in 1, len(listings[:num_cards]):
            best_difference = max(best_difference, compare_price(sorted_listings[i-1], sorted_listings[i]))
        
        if best_difference > best_seen:
            best_seen = best_difference
            print(sku_request)
            print(best_difference)


   
