import concurrent
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import sleep


def execute_parallel(func, items, item_arg_name, parallel_threads=4):
    with ThreadPoolExecutor(max_workers=parallel_threads) as executor:
        futures = \
            [executor.submit(partial(func, **{item_arg_name: item})) for item in items]

        try:
            while not all([future.done() for future in futures]):
                sleep(0.2)

            return [f.result() for f in futures]
        except KeyboardInterrupt:
            print("SIGTERM non gracefully stopping thread pool!")
            executor._threads.clear()
            concurrent.futures.thread._threads_queues.clear()
            raise