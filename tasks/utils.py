import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any, List
from urllib.parse import urlencode

from models import SKU
from tasks import logger

MAX_PARALLEL_NETWORK_REQUESTS = 48


def paginateWithBackoff(
        total,
        paginate_fn: Callable[[int], Any],
        pagination_size: int,
        on_paginated: Callable[[Any], None],
        start=0,
        num_parallel_requests=MAX_PARALLEL_NETWORK_REQUESTS,
        retry_delay_sec=300,
):
    if total == 0:
        return

    batch_offset_increments = min(total, num_parallel_requests * pagination_size)

    for batch_offset in range(start, total, batch_offset_increments):
        with ThreadPoolExecutor(num_parallel_requests) as executor:
            futures_to_offset_map = {
                executor.submit(paginate_fn, cur_offset): cur_offset

                for cur_offset in range(
                    batch_offset,
                    min(batch_offset + batch_offset_increments, total),
                    pagination_size
                )
            }

            while futures_to_offset_map:
                retry_offsets = []

                for future in as_completed(futures_to_offset_map.keys()):
                    try:
                        result = future.result()

                        on_paginated(result)

                    except Exception as e:
                        logger.error(f'Error on offset {futures_to_offset_map[future]}: {e}')

                        # If there's an error, add the offset for retry and mark the batch
                        retry_offsets.append(futures_to_offset_map[future])

                    finally:
                        del futures_to_offset_map[future]

                # If there's any tasks we need to retry, delay and rerun the batch
                if retry_offsets:
                    logger.info(f'Delaying for {retry_delay_sec} seconds and retrying...')
                    time.sleep(retry_delay_sec)

                    futures_to_offset_map = {
                        executor.submit(paginate_fn, offset): offset

                        for offset in retry_offsets
                    }


def split_into_segments(array, num_segments) -> List[List[Any]]:
    """Splits the array into the specified number of segments."""
    if num_segments <= 0:
        raise ValueError("Number of segments must be positive")

    segment_size = len(array) // num_segments
    remainder = len(array) % num_segments

    segments = []
    start = 0
    for i in range(num_segments):
        end = start + segment_size + (1 if i < remainder else 0)
        segments.append(array[start:end])
        start = end

    return segments
