import logging
from datetime import datetime

from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert

from models import db_sessionmaker
from models.card import Card
from models.condition import Condition
from models.printing import Printing
from models.set import Set
from models.sku import SKU
from services.tcgplayer_catalog_service import TCGPlayerCatalogService
from tasks import scheduler
from tasks.log_runtime_decorator import log_runtime
from tasks.set_card_sync_data import set_card_sync_data
from tasks.utils import paginateWithBackoff

logger = logging.getLogger(__name__)
tcgplayer_catalog_service = TCGPlayerCatalogService()

db_session = db_sessionmaker()

PAGINATION_SIZE = 100


def _to_dict(model):
    return {column.name: getattr(model, column.name) for column in model.__table__.columns}


def _get_upsert_conflict_set_args(model_class, excluded):
    columns = [column.name for column in inspect(model_class).columns if column.name != 'id']
    return {column: getattr(excluded, column) for column in columns}


def _add_updated_set_models(outdated_sets: list, set_responses: list):
    for set_response in set_responses:
        response_set_model = Set.from_tcgplayer_response(set_response)

        existing_set_model: Set = db_session.get(Set, response_set_model.id)

        if (existing_set_model is None or
                existing_set_model.modified_date < response_set_model.modified_date):
            outdated_sets.append(response_set_model)


def _convert_and_insert_cards_and_skus(card_responses: list):
    if len(card_responses) > 0:
        insert_card_stmt = insert(Card).values(
            list(map(lambda response: _to_dict(Card.from_tcgplayer_response(response)), card_responses))
        )

        insert_card_stmt = insert_card_stmt.on_conflict_do_update(
            index_elements=['id'],
            set_=_get_upsert_conflict_set_args(Card, insert_card_stmt.excluded)
        )
        #
        db_session.execute(insert_card_stmt)

        sku_values = []
        for card_response in card_responses:
            for sku_response in card_response['skus']:
                sku_values.append(_to_dict(SKU.from_tcgplayer_response(sku_response)))

        if len(sku_values) > 0:
            insert_sku_stmt = insert(SKU).values(sku_values)

            insert_sku_stmt = insert_sku_stmt.on_conflict_do_update(
                index_elements=['id'],
                set_=_get_upsert_conflict_set_args(SKU, insert_sku_stmt.excluded)
            )

            db_session.execute(insert_sku_stmt)


def _fetch_cards_in_set(set_id: int) -> list:
    set_card_count = tcgplayer_catalog_service.fetch_total_card_count(set_id)
    set_cards = []

    paginateWithBackoff(
        total=set_card_count,
        paginate_fn=lambda offset: tcgplayer_catalog_service.get_cards(offset, PAGINATION_SIZE, set_id),
        pagination_size=PAGINATION_SIZE,
        on_paginated=lambda card_responses: set_cards.extend(card_responses),
        # on_paginated_failure=lambda card_responses: delete_set_by_id(set_id)
    )

    return set_cards


@log_runtime
def update_card_database():
    printing_responses = tcgplayer_catalog_service.get_card_printings()
    condition_responses = tcgplayer_catalog_service.get_card_conditions()
    # rarity_responses = tcgplayer_catalog_service.get_card_rarities()

    # rarity_models = list(map(lambda x: Rarity.from_tcgplayer_response(x), rarity_responses))
    insert_printing_stmt = insert(Printing).values(
        list(map(lambda response: _to_dict(Printing.from_tcgplayer_response(response)), printing_responses))
    )

    insert_printing_stmt = insert_printing_stmt.on_conflict_do_update(
        index_elements=['id'],
        set_=_get_upsert_conflict_set_args(Printing, insert_printing_stmt.excluded)
    )

    insert_condition_stmt = insert(Condition).values(
        list(map(lambda response: _to_dict(Condition.from_tcgplayer_response(response)), condition_responses))
    )

    insert_condition_stmt = insert_condition_stmt.on_conflict_do_update(
        index_elements=['id'],
        set_=_get_upsert_conflict_set_args(Condition, insert_condition_stmt.excluded)
    )

    db_session.execute(insert_printing_stmt)
    db_session.execute(insert_condition_stmt)

    set_total_count = tcgplayer_catalog_service.get_total_card_set_count()

    outdated_sets = []

    paginateWithBackoff(
        total=set_total_count,
        paginate_fn=lambda offset: tcgplayer_catalog_service.get_sets(offset, PAGINATION_SIZE),
        pagination_size=PAGINATION_SIZE,
        on_paginated=lambda set_responses: _add_updated_set_models(outdated_sets, set_responses),
    )

    logger.info(f'{len(outdated_sets)} sets are outdated: {[outdated_set.name for outdated_set in outdated_sets]}')

    if len(outdated_sets) > 0:
        insert_outdated_sets_stmt = insert(Set).values(
            list(map(lambda model: _to_dict(model), outdated_sets))
        )

        insert_outdated_sets_stmt = insert_outdated_sets_stmt.on_conflict_do_update(
            index_elements=['id'],
            set_=_get_upsert_conflict_set_args(Set, insert_outdated_sets_stmt.excluded)
        )

        db_session.execute(insert_outdated_sets_stmt)

    # We want to "paginate" on all the card sets and fetch the cards in each set. Hence, we call paginate
    # with pagination_size=1.
    paginateWithBackoff(
        total=len(outdated_sets),
        paginate_fn=lambda offset: _fetch_cards_in_set(outdated_sets[offset].id),
        pagination_size=1,
        on_paginated=lambda card_responses: _convert_and_insert_cards_and_skus(card_responses),
    )

    db_session.commit()


if __name__ == "__main__":
    update_card_database()
