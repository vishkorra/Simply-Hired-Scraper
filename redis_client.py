from threading import current_thread
from typing import Any, List
import redis
import json

REDIS_CLIENT = redis.Redis(host="localhost", port=6379, db=0)

def set(key: str, value: str) -> None:
    REDIS_CLIENT.set(key, value)

def get(key: str) -> Any:
    return REDIS_CLIENT.get(key)

def get_thread() -> str:
    return current_thread().getName()

def get_page_number_from_redis(redis_key: str) -> int:
    with REDIS_CLIENT.lock("page_number_lock"):
        page_number_cached_value = get(f'{redis_key}-page_number')
        page_number = None

        if page_number_cached_value is None:
            # If first time, increment for the next thread and return 1.
            set(f"{redis_key}-page_number", str(2))
            page_number = 1
        else:
            # If there's already a value in the cache, increment and return the cached value.
            page_number = int(page_number_cached_value)
            new_page_number = str(page_number + 1)
            set(f"{redis_key}-page_number", new_page_number)
        
        print(f'Got the page number {page_number} - {get_thread()} exiting.')
        return page_number

def get_next_page_from_redis(redis_key: str) -> bool:
    with REDIS_CLIENT.lock("next_page_lock"):
        next_page_cached_value = get(f'{redis_key}-next_page')
        next_page = None

        if next_page_cached_value is None:
            set(f"{redis_key}-next_page", '1')
            next_page = 1
        else:
            next_page = int(next_page_cached_value)
        
        print(f'Got the next page {next_page} - {get_thread()} exiting.')
        return next_page == 1

def set_next_page_in_redis(redis_key: str) -> None:
    with REDIS_CLIENT.lock("next_page_lock"):
        set(f"{redis_key}-next_page", '0')

def set_job_positions(redis_key: str, value: List[List[str]]):
    with REDIS_CLIENT.lock("job_positions_lock"):
        job_position_cached_value = get(f"{redis_key}-job_positions")

        if job_position_cached_value is None:
            set(f"{redis_key}-job_positions", json.dumps(value))
        else:
            job_positions: List[List[str]] = json.loads(job_position_cached_value)
            job_positions += value
            set(f"{redis_key}-job_positions", json.dumps(job_positions))

def get_job_positions(redis_key: str) -> List[List[str]]:
    with REDIS_CLIENT.lock("job_positions_lock"):
        job_positions_cached_value = get(f"{redis_key}-job_positions")
        if job_positions_cached_value is None:
            return []
        return json.loads(job_positions_cached_value)

def flush_redis_db() -> None:
    REDIS_CLIENT.flushdb()