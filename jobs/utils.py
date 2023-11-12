from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any

MAX_PARALLEL_NETWORK_REQUESTS = 48


def paginate(
        total,
        paginate_fn: Callable[[int, int], Any],
        pagination_size: int,
        on_paginated: Callable[[Any], None],
        start=0,
        num_parallel_requests=MAX_PARALLEL_NETWORK_REQUESTS,
):
    if total == 0:
        return

    batch_offset_increments = min(total, num_parallel_requests * pagination_size)

    for batch_offset in range(start, total, batch_offset_increments):
        with ThreadPoolExecutor(num_parallel_requests) as executor:
            futures = [executor.submit(paginate_fn, cur_offset, pagination_size)
                       for cur_offset in range(
                    batch_offset,
                    min(batch_offset + batch_offset_increments, total),
                    pagination_size
                )]

            for future in futures:
                result = future.result()

                on_paginated(result)
