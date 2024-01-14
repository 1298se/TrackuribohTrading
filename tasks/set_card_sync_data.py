import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List

from sqlalchemy.dialects.postgresql import insert

from data.dao import get_sales_count_since_date
from models import db_sessionmaker, Card
from models.card_sync_data import SyncFrequency, CardSyncData
import numpy as np

logger = logging.getLogger(__name__)

# Max 14 workers because of DB config of 5 concurrent + 10 overflow connections
NUM_WORKERS = 14
SALES_DELTA = timedelta(days=7)

worker_id = 0


def assign_sync_frequency(card_ids: List[int], date: datetime):
    """
    Assign a fetch tier based on the sales_count. HIGH is roughly 5%, MEDIUM is 45%.
    """
    global worker_id
    local_worker_id = worker_id
    session = db_sessionmaker()
    logger.debug(f'Worker {local_worker_id} received {len(card_ids)} items')
    worker_id += 1

    for index, card_id in enumerate(card_ids):
        sales_count = get_sales_count_since_date(session, card_id, date)

        if sales_count >= 25:
            frequency = SyncFrequency.HIGH
        elif sales_count >= 5:
            frequency = SyncFrequency.MEDIUM
        else:
            frequency = SyncFrequency.LOW

        logger.debug(f'Worker {local_worker_id} processed card {index + 1} out of {len(card_ids)}')

        data = {'card_id': card_id, 'sync_frequency': frequency}

        # Update or create the CardFetchTier entry
        stmt = insert(CardSyncData).values(data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['card_id'],
            set_={k: v for k, v in data.items() if k != 'card_id'}
        )

        session.execute(stmt)

    session.commit()


def set_card_sync_data(card_ids: List[int]):
    """
        Recomputes the card sync data for cards with a given sync frequency. If none, recomputes the sync data for
        all cards. Currently, we just look at card sales to determine sync frequency.
    """
    start_date = datetime.now() - SALES_DELTA

    card_id_segments = np.array_split(card_ids, NUM_WORKERS)

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(assign_sync_frequency, segment.tolist(), start_date) for segment in card_id_segments]

        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    session = db_sessionmaker()

    set_card_sync_data([card[0] for card in session.query(Card.id).all()])
