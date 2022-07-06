from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import wraps
import math
import threading
from collections import Counter, defaultdict
from typing import Callable, DefaultDict, Dict, Tuple
from flask import Response
from rich import inspect
from functools import wraps
import time
from rich import inspect
import redis
from redis.exceptions import ConnectionError

redis_client = redis.Redis('localhost',port=6379)


def floor_dt(dt, delta):
    rounded = dt - (dt - datetime.min) % delta
    return rounded

@dataclass
class RequestRate:
    requests: int
    period: timedelta

    @property
    def per_second(self):
        return self.requests / self.period.total_seconds()


"""
Data structures & classes for various rate limiting algorithms with 
corresponding storage backends.
"""

class LocalTokenBucket:
    """
    A token bucket is a container that has pre-defined capacity, and is refilled at a preset rate.Once the bucket is full, additional tokens overflow.
    """

    def __init__(self):
        self.buckets = {}
        self.lock = threading.Lock()

    def refill(self, key: str, refill_rate: RequestRate, token_capacity):
        """
        Perform a 'refill' at request time based on elapsed time * rate
        """
        with self.lock:
            try:
                current_tokens, last_refill = self.buckets[key]
                now = time.monotonic()

                # Catch race condition of another
                # thread refilling the bucket
                if now < last_refill:
                    return

                additional_tokens = refill_rate.per_second * (now - last_refill)
                self.buckets[key] = [
                    min(token_capacity, current_tokens + additional_tokens),
                    now,
                ]
            except KeyError:
                self.buckets[key] = [token_capacity, time.monotonic()]

    def consume(self, key, token_cost) -> Tuple[bool, int, datetime]:
        """
        Try to consume a cost from a token bucket
        """
        with self.lock:
            current_tokens, last_refill = self.buckets[key]
            if current_tokens < token_cost:
                return False, current_tokens, last_refill
            self.buckets[key][0] -= token_cost
            return True, current_tokens - token_cost, last_refill

    def inspect(self):
        inspect(self.buckets)

    def __call__(
        self,
        identifier_func: Callable,
        refill_rate: RequestRate,
        token_capacity=6,
        token_cost=1,
    ):
        """
        Decorator that limits usage of the decorated function using a Token Bucket.

        Args:
            backend (TokenBucket): a TokenBucket storage backend
            identifier_func (Callable): some function that discriminates each request
            refill_rate (float): refill rate in tokens per second
            token_capacity (int): maximum token amount per identifier
            token_cost (int): the token cost of the decorated function

        Returns:
            A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
        """

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = identifier_func()
                self.refill(key, refill_rate, token_capacity)
                allow, tokens, last_refill = self.consume(key, token_cost)
                rate_headers = {
                    "api-ratelimit-limit": refill_rate.per_second,
                    "api-ratelimit-remaining": tokens / token_cost,
                }

                if allow:
                    resp = f(*args, **kwargs)
                    return Response(response=resp, status=200, headers=rate_headers)
                else:
                    return Response(
                        response=f"Rate Limit for {key} exceeded.",
                        status=429,
                        headers=rate_headers,
                    )

            return wrapper

        return decorator


class LocalLeakyBucket:
    """
    This implementation of a Leaky Bucket is based on the
    leaky bucket 'as a meter' description found in reference texts.

    'as a queue' confuses me and I'd like to review a minimal implementation of it.
    """

    def __init__(self) -> None:
        self.buckets = {}
        self.lock = threading.Lock()

    def pour(self, key, token_capacity, token_cost):
        with self.lock:
            current_tokens, last_leak = self.buckets[key]
            if current_tokens + token_cost > token_capacity:
                return False, current_tokens, last_leak
            self.buckets[key][0] += token_cost
            return True, current_tokens + token_cost, last_leak

    def leak(self, key, leak_rate: RequestRate):
        with self.lock:
            try:
                current_tokens, last_leak = self.buckets[key]
                now = time.monotonic()

                # Catch race condition of another
                # thread refilling the bucket
                if now < last_leak:
                    return

                reduction = leak_rate.per_second * (now - last_leak)
                self.buckets[key] = [max(0, current_tokens - reduction), now]
            except KeyError:
                self.buckets[key] = [0, time.monotonic()]

    def __call__(
        self,
        identifier_func: Callable,
        leak_rate: RequestRate,
        token_capacity=6,
        token_cost=1,
    ):
        """
        Decorator that limits usage of the decorated function using a Leaky Bucket (as a meter).

        Args:
            backend (LeakyBucket): a storage backend
            identifier_func (Callable): some function that discriminates each request
            rate (float): leak rate in tokens per second
            capacity (int): maximum token amount per identifier
            cost (int): the token cost of the decorated function

        Returns:
            A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
        """

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = identifier_func()
                self.leak(key, leak_rate)
                allow, tokens, last_leak = self.pour(key, token_capacity, token_cost)
                rate_headers = {
                    "api-ratelimit-limit": leak_rate.per_second,
                    "api-ratelimit-remaining": (token_capacity - tokens) / token_cost,
                }

                if allow:
                    resp = f(*args, **kwargs)
                    return Response(response=resp, status=200, headers=rate_headers)
                else:
                    return Response(
                        response=f"Rate Limit for {key} exceeded.",
                        status=429,
                        headers=rate_headers,
                    )

            return wrapper

        return decorator


class LocalSlidingLog:
    def __init__(self):
        self.logs = {}
        self.lock = threading.Lock()

    def consume(self, key, rate: RequestRate):
        with self.lock:
            now = datetime.now()
            # TODO: use some sorted, thread safe data structure for this
            lower_bound = now - rate.period
            try:
                log = self.logs[key]
                log.append(now)
                log = [*filter(lambda ts: ts > lower_bound, log)]
                size = len(log)
                if size > rate.requests:
                    return False, rate.requests - size, log[-1] + rate.period
                return True, rate.requests - size, log[-1] + rate.period
            except KeyError:
                self.logs[key] = [now]
                return True, rate.requests - 1, now + rate.period

    def __call__(self, identifier_func: Callable, rate: RequestRate):
        """
        Decorator that limits usage of the decorated function using a Sliding Window Log.

        Args:
            identifier_func (Callable): some function that discriminates each request
            rate (RequestRate): rate limit

        Returns:
            A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
        """

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = identifier_func()
                allow, remaining, next_reset = self.consume(key, rate)
                rate_headers = {
                    "api-ratelimit-limit": rate.per_second,
                    "api-ratelimit-remaining": remaining,
                    "api-ratelimit-reset": next_reset,
                }
                if allow:
                    resp = f(*args, **kwargs)
                    return Response(response=resp, status=200, headers=rate_headers)
                else:
                    return Response(
                        response=f"Rate Limit for {key} exceeded.",
                        status=429,
                        headers=rate_headers,
                    )

            return wrapper

        return decorator


class LocalFixedWindowCounter:
    """
    A fixed window counter maps an identifier to a (count, timestamp) tuple.
    Rate limits are imposed by a maximum rate for each fixed timestamp 'window'.
    """

    def __init__(self):
        self.counter = {}
        self.lock = threading.Lock()

    def consume(self, key, rate: RequestRate):
        with self.lock:
            now = datetime.now()
            latest_window = floor_dt(now, rate.period)
            try:
                count, active_window = self.counter[key]
                if active_window < latest_window:
                    self.counter[key] = [0, latest_window]
                    return True, rate.requests, latest_window + rate.period
                if count + 1 < rate.requests:
                    self.counter[key][0] += 1
                    return True, rate.requests - count - 1, active_window + rate.period
                return False, 0, active_window + rate.period
            except KeyError:
                self.counter[key] = [0, latest_window]
                return True, rate.requests, latest_window + rate.period

    def __call__(self, identifier_func: Callable, rate: RequestRate):
        """
        Decorator that limits usage of the decorated function using a Fixed Window.

        Args:
            backend (WindowCounter): a FixedWindow storage backend
            identifier_func (Callable): some function that discriminates each request
            rate (float): refill rate in tokens per second

        Returns:
            A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
        """

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = identifier_func()
                allow, remaining, next_reset = self.consume(key, rate)
                rate_headers = {
                    "api-ratelimit-limit": rate.per_second,
                    "api-ratelimit-remaining": remaining,
                    "api-ratelimit-reset": next_reset,
                }
                if allow:
                    resp = f(*args, **kwargs)
                    return Response(response=resp, status=200, headers=rate_headers)
                else:
                    return Response(
                        response=f"Rate Limit for {key} exceeded.",
                        status=429,
                        headers=rate_headers,
                    )

            return wrapper

        return decorator

class RedisFixedWindowCounter:
    """
    A fixed window counter maps an identifier to a (count, timestamp) tuple.
    Rate limits are imposed by a maximum rate for each fixed timestamp 'window'.
    """

    def __init__(self,keyspace):
        self.hash_name = keyspace

    def consume(self, key, rate: RequestRate):
        now = datetime.now()
        latest_window = floor_dt(now, rate.period)
        if redis_client.hexists(self.hash_name,key):
            val = redis_client.hget(self.hash_name,key).decode()
            count, active_window = val.split('##')
            count = int(count)
            active_window = datetime.fromtimestamp(int(float(active_window)))
            if active_window < latest_window:
                redis_client.hset(self.hash_name,key,f"0##{latest_window.timestamp()}")
                return True, rate.requests, latest_window + rate.period
            if count + 1 < rate.requests:
                redis_client.hset(self.hash_name,key,f"{count+1}##{latest_window.timestamp()}")
                return True, rate.requests - count - 1, active_window + rate.period
            return False, 0, active_window + rate.period
        else:
            redis_client.hset(self.hash_name,key,f"0##{latest_window.timestamp()}")
            return True, rate.requests, latest_window + rate.period

    def __call__(self, identifier_func: Callable, rate: RequestRate):
        """
        Decorator that limits usage of the decorated function using a Fixed Window.

        Args:
            backend (WindowCounter): a FixedWindow storage backend
            identifier_func (Callable): some function that discriminates each request
            rate (float): refill rate in tokens per second

        Returns:
            A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
        """

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = identifier_func()
                allow, remaining, next_reset = self.consume(key, rate)
                rate_headers = {
                    "api-ratelimit-limit": rate.per_second,
                    "api-ratelimit-remaining": remaining,
                    "api-ratelimit-reset": next_reset,
                }
                if allow:
                    resp = f(*args, **kwargs)
                    return Response(response=resp, status=200, headers=rate_headers)
                else:
                    return Response(
                        response=f"Rate Limit for {key} exceeded.",
                        status=429,
                        headers=rate_headers,
                    )

            return wrapper

        return decorator


class LocalSlidingWindowCounter:
    """
    A fixed window counter maps an identifier to a (count, timestamp) tuple.
    Rate limits are imposed by a maximum rate for each fixed timestamp 'window'.
    """

    def __init__(self):
        self.counters: Dict[str, DefaultDict] = {}
        self.lock = threading.Lock()

    def consume(self, key, rate: RequestRate):
        with self.lock:
            now = datetime.now()
            now_minute_floor = floor_dt(now, rate.period)
            rolling_lower_bound = now - rate.period
            frame = floor_dt(now, timedelta(seconds=1))
            # todo: this feels clunky
            try:
                windows = self.counters[key]
                pruned_windows = defaultdict(int)
                prior_sum = 0
                current_sum = 0
                for k, v in windows.items():
                    if k > rolling_lower_bound:
                        pruned_windows[k] = v
                        if k < now_minute_floor:
                            prior_sum += v
                        else:
                            current_sum += v
                self.counters[key] = pruned_windows
                weighted_sum = math.floor(
                    prior_sum * (1 - (now.second / 60)) + current_sum
                )
                self.counters[key][frame] += 1
                if weighted_sum < rate.requests:
                    return True, rate.requests - weighted_sum, None
                else:
                    return False, rate.requests - weighted_sum, None
            except KeyError:
                self.counters[key] = defaultdict(int)
                windows = self.counters[key]
                self.counters[key][frame] += 1

                return True, rate.requests - 1, None

    def __call__(self, identifier_func: Callable, rate: RequestRate):
        """
        Decorator that limits usage of the decorated function using a Fixed Window.

        Args:
            backend (WindowCounter): a FixedWindow storage backend
            identifier_func (Callable): some function that discriminates each request
            rate (float): refill rate in tokens per second

        Returns:
            A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
        """

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = identifier_func()
                allow, remaining, _ = self.consume(key, rate)
                rate_headers = {
                    "api-ratelimit-limit": rate.per_second,
                    "api-ratelimit-remaining": remaining,
                }
                if allow:
                    resp = f(*args, **kwargs)
                    return Response(response=resp, status=200, headers=rate_headers)
                else:
                    return Response(
                        response=f"Rate Limit for {key} exceeded.",
                        status=429,
                        headers=rate_headers,
                    )

            return wrapper

        return decorator
