import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any

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

                        print(f'Success on offset {futures_to_offset_map[future]}: {result}')

                        on_paginated(result)

                    except Exception as e:
                        print(f'Error on offset {futures_to_offset_map[future]}: {e}')

                        # If there's an error, add the offset for retry and mark the batch
                        retry_offsets.append(futures_to_offset_map[future])

                    finally:
                        del futures_to_offset_map[future]

                # If there's any tasks we need to retry, delay and rerun the batch
                if retry_offsets:
                    print(f'Delaying for {retry_delay_sec} seconds and retrying...')
                    time.sleep(retry_delay_sec)

                    futures_to_offset_map = {
                        executor.submit(paginate_fn, offset): offset

                        for offset in retry_offsets
                    }
