import threading
import timeit
import datetime
from typing import Iterable, Dict
from elasticsearch import Elasticsearch
from elastic_transport import ObjectApiResponse
from concurrent.futures import ThreadPoolExecutor, as_completed


class ElasticIndexer:

    def __init__(self, client: Elasticsearch, index_name: str, pipeline: str = None, id_key: str = None) -> None:
        self.client = client
        self.index_name = index_name
        self.pipeline = pipeline
        self.id_key = id_key
        self.total_records = 0
        self.counter = 0
        self.counter_lock = threading.Lock()
        self.start_timer = None

    def reset(self):
        self.total_records = 0
        self.counter = 0
        self.start_timer = None

    def index_record(self, doc: dict):
        event_id = None
        if self.id_key is not None:
            event_id = doc.get(self.id_key, None)
            # TODO: Fix event_id to be more unique
        res = self.client.index(index=self.index_name, document=doc, id=event_id, pipeline=self.pipeline)
        # print(res)
        if not isinstance(res, ObjectApiResponse):
            print(f"Error: {res}")
        return res

                


    def progress_callback(self, future):
        if future._state == "CANCELLED":
            return
        self.counter_lock.acquire()
        self.counter += 1
        if self.counter % 1000 == 0:
            elapsed_time = timeit.default_timer() - self.start_timer
            average_time = elapsed_time / self.counter
            estimated_remaining = (self.total_records - self.counter) * average_time
            print(f"Indexed: {self.counter} of {self.total_records} ({(self.counter/self.total_records)*100} %)\nElapsed Time: {datetime.timedelta(seconds=elapsed_time)}\nAverage Time: {average_time} s\nEstimated Remaining {datetime.timedelta(seconds=estimated_remaining)}\n")

        if hasattr(future, 'exception') and future.exception():
            print(f"Error during indexing: {repr(future.exception())}")

        # Print the response regardless of whether it's an ObjectApiResponse
        res = future.result()
        # print(f"Response: {res}")

        self.counter_lock.release()

    def index_data(self, data: Iterable[Dict], total_records: int, max_workers: int = 20):
        self.total_records = total_records
        with ThreadPoolExecutor(max_workers=30) as executor:
            self.start_timer = timeit.default_timer()
            futures = (executor.submit(self.index_record, x) for x in data)
            for future in futures:
                future.add_done_callback(self.progress_callback)
            try:
                for c_future in as_completed(futures):
                    c_future.result()
            except KeyboardInterrupt:
                executor.shutdown(wait=False, cancel_futures=True)
                print("KeyboardInterrup: Exiting")
            except Exception as e:
                executor.shutdown(wait=False, cancel_futures=True)
                print(repr(e))
        self.reset()