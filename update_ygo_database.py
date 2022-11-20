import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as DBSession

from models import Base
# We need to import new models to have them automatically created
from models.card import Card
from models.set import Set
from models.condition import Condition
from models.printing import Printing
from models.rarity import Rarity
from models.sku import Sku
from repositories.tcgplayer_catalog_repository import TCGPlayerCatalogRepository
from services.tcgplayer_catalog_service import TCGPlayerApiService

load_dotenv()

SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URI")

engine = create_engine(SQLALCHEMY_DATABASE_URI, future=True)

db_session = DBSession(engine)
tcgplayer_api_service = TCGPlayerApiService()
tcgplayer_catalog_repository = TCGPlayerCatalogRepository(tcgplayer_api_service, db_session)

Base.metadata.create_all(engine)


def _add_updated_set_models(outdated_sets: list, set_responses: list):
    for set_response in set_responses:
        response_set_model = Set.from_tcgplayer_response(set_response)

        existing_set_model: Set = db_session.get(Set, response_set_model.id)

        if (existing_set_model is None or
                existing_set_model.modified_date < response_set_model.modified_date):
            outdated_sets.append(response_set_model)


def _convert_and_insert_cards_and_skus(card_responses):
    tcgplayer_catalog_repository.insert_cards(
        map(lambda card_response: Card.from_tcgplayer_response(card_response, db_session), card_responses)
    )

    for card_response in card_responses:
        tcgplayer_catalog_repository.insert_skus(
            map(lambda sku_response: Sku.from_tcgplayer_response(sku_response), card_response['skus'])
        )


def _fetch_cards_in_set(set_id: int) -> list:
    print(set_id)
    set_card_count = tcgplayer_catalog_repository.fetch_total_card_count(set_id)
    set_cards = []

    paginate(
        total=set_card_count,
        paginate_fn=lambda offset, limit: tcgplayer_catalog_repository.fetch_cards(offset, limit, set_id),
        on_paginated=lambda card_responses: set_cards.extend(card_responses)
    )

    return set_cards


from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Callable, Any

MAX_PARALLEL_NETWORK_REQUESTS = 5
PAGINATION_SIZE = 100


def paginate(
        total,
        paginate_fn: Callable[[int, int], Any],
        on_paginated: Callable[[Any], None],
        num_parallel_requests=MAX_PARALLEL_NETWORK_REQUESTS,
        pagination_size=PAGINATION_SIZE
):
    if total == 0:
        return

    batch_offset_increments = min(total, num_parallel_requests * pagination_size)

    for batch_offset in range(0, total, batch_offset_increments):
        with ThreadPoolExecutor(num_parallel_requests) as executor:
            futures = [executor.submit(paginate_fn, cur_offset, pagination_size)
                       for cur_offset in range(
                    batch_offset,
                    min(batch_offset + batch_offset_increments, total),
                    pagination_size
                )]

            for future in futures:
                on_paginated(future.result())


def update_card_database():
    print(f'{__name__} started at {datetime.now()}')

    printing_responses = tcgplayer_catalog_repository.fetch_card_printings()
    condition_responses = tcgplayer_catalog_repository.fetch_card_conditions()
    rarity_responses = tcgplayer_catalog_repository.fetch_card_rarities()

    condition_models = list(map(lambda x: Condition.from_tcgplayer_response(x), condition_responses))
    printing_models = list(map(lambda x: Printing.from_tcgplayer_response(x), printing_responses))
    rarity_models = list(map(lambda x: Rarity.from_tcgplayer_response(x), rarity_responses))

    tcgplayer_catalog_repository.insert_conditions(condition_models)
    tcgplayer_catalog_repository.insert_printings(printing_models)
    tcgplayer_catalog_repository.insert_rarities(rarity_models)

    set_total_count = tcgplayer_catalog_repository.fetch_total_card_set_count()

    outdated_sets = []
    paginate(
        total=set_total_count,
        paginate_fn=tcgplayer_catalog_repository.fetch_card_sets,
        on_paginated=lambda set_responses: _add_updated_set_models(outdated_sets, set_responses)
    )

    print(f'{len(outdated_sets)} sets are oudated: {[outdated_set.name for outdated_set in outdated_sets]}')

    tcgplayer_catalog_repository.insert_sets(outdated_sets)

    # We want to "paginate" on all the card sets and fetch the cards in each set. Hence, we call paginate
    # with pagination_size=1.
    paginate(
        total=len(outdated_sets),
        paginate_fn=lambda offset, limit: _fetch_cards_in_set(outdated_sets[offset].id),
        on_paginated=lambda card_responses: _convert_and_insert_cards_and_skus(card_responses),
        num_parallel_requests=1,
        pagination_size=1,
    )

    db_session.commit()

    print(f'{__name__} done at {datetime.now()}')


update_card_database()
