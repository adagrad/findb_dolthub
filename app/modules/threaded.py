import concurrent
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from functools import partial
from time import sleep


def execute_parallel(func, items, item_arg_name, parallel_threads=4, early_exit=None, shutdown_hook=None):
    with ThreadPoolExecutor(max_workers=parallel_threads) as executor:
        futures = \
            [executor.submit(partial(func, **{item_arg_name: item})) for item in items]

        try:
            while not all([future.done() for future in futures]):
                if early_exit is not None and early_exit():
                    concurrent.futures.thread._threads_queues.clear()
                    executor.shutdown(False)
                    return [f.result(timeout=0) for f in futures]

                sleep(0.2)

            return [f.result(timeout=0) for f in futures]
        except TimeoutError:
            print("maximum time reached")
        except KeyboardInterrupt:
            print("SIGTERM non gracefully stopping thread pool!")
            concurrent.futures.thread._threads_queues.clear()
            executor.shutdown(False)
            raise
        except Exception as e:
            raise e
        finally:
            if shutdown_hook is not None:
                shutdown_hook()