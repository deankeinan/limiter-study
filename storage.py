from datetime import datetime, timedelta
from functools import wraps
import math
import threading
from collections import Counter, defaultdict
from typing import Callable, DefaultDict, Dict
from flask import Response
from rich import inspect
from functools import wraps
import abc
import time
from rich import inspect

"""
Data structures & classes for various rate limiting algorithms with 
corresponding storage backends.
"""


class LocalTokenBucket:
    """
    A token bucket is a container that has pre-defined capacity, and is refilled at a preset rate.
    Once the bucket is full, additional tokens overflow.
    """

    def __init__(self):
        self.buckets = {}
        self.lock = threading.Lock()

    def refill(self, key, rate, capacity):
        with self.lock:
            try:
                current_tokens, last_refill = self.buckets[key]
                now = time.monotonic()

                # Catch race condition of another
                # thread refilling the bucket
                if now < last_refill:
                    return

                additional_tokens = rate * (now - last_refill)
                self.buckets[key] = [
                    min(capacity, current_tokens + additional_tokens),
                    now,
                ]
            except KeyError:
                self.buckets[key] = [capacity, time.monotonic()]

    def consume(self, key, cost=1):
        with self.lock:
            current_tokens, last_refill = self.buckets[key]
            if current_tokens < cost:
                return False, current_tokens, last_refill
            self.buckets[key][0] -= cost
            return True, current_tokens - cost, last_refill

    def inspect(self):
        inspect(self.buckets)

    def __call__(self, identifier_func: Callable, rate=1, capacity=6, cost=1):
        """
        Decorator that limits usage of the decorated function using a Token Bucket.

        Args:
            backend (TokenBucket): a TokenBucket storage backend
            identifier_func (Callable): some function that discriminates each request
            rate (float): refill rate in tokens per second
            capacity (int): maximum token amount per identifier
            cost (int): the token cost of the decorated function

        Returns:
            A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
        """

        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                key = identifier_func()
                self.refill(key, rate, capacity)
                allow, tokens, last_refill = self.consume(key, cost)
                rate_headers = {
                    "api-ratelimit-limit": rate,
                    "api-ratelimit-remaining": tokens / cost,
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

    def pour(self, key, capacity, cost=1):
        with self.lock:
            current_tokens, last_leak = self.buckets[key]
            if current_tokens + cost > capacity:
                return False, current_tokens, last_leak
            self.buckets[key][0] += cost
            return True, current_tokens + cost, last_leak

    def leak(self, key, rate):
        with self.lock:
            try:
                current_tokens, last_leak = self.buckets[key]
                now = time.monotonic()

                # Catch race condition of another
                # thread refilling the bucket
                if now < last_leak:
                    return

                reduction = rate * (now - last_leak)
                self.buckets[key] = [max(0, current_tokens - reduction), now]
            except KeyError:
                self.buckets[key] = [0, time.monotonic()]

    def __call__(self, identifier_func: Callable, rate=1, capacity=6, cost=1):
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
                self.leak(key, rate)
                allow, tokens, last_leak = self.pour(key, capacity, cost)
                print(allow, tokens, last_leak)
                rate_headers = {
                    "api-ratelimit-limit": rate,
                    "api-ratelimit-remaining": (capacity - tokens) / cost,
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

    def consume(self, key, rate):
        with self.lock:
            now = datetime.now()
            # TODO: use some sorted, thread safe data structure for this
            lower_bound = now - timedelta(minutes=1)
            try:
                log = self.logs[key]
                log.append(now)
                log = [*filter(lambda ts: ts > lower_bound, log)]
                size = len(log)
                if size > rate:
                    return False, rate - size, log[-1] + timedelta(minutes=1)
                return True, rate - size, log[-1] + timedelta(minutes=1)
            except KeyError:
                self.logs[key] = [now]
                return True, rate - 1, now + timedelta(minutes=1)

    def __call__(self, identifier_func: Callable, rate=2):
        """
        Decorator that limits usage of the decorated function using a Sliding Window Log.

        Args:
            backend (SlidingWindow): a Slidingwindow storage backend
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
                    "api-ratelimit-limit": rate,
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

    def consume(self, key, rate):
        with self.lock:
            now = datetime.now()
            latest_window = now.replace(second=0, microsecond=0, minute=now.minute)
            try:
                count, active_window = self.counter[key]
                if active_window < latest_window:
                    self.counter[key] = [0, latest_window]
                    return True, rate, latest_window + timedelta(minutes=1)
                if count < rate:
                    self.counter[key][0] += 1
                    return True, rate - count - 1, active_window + timedelta(minutes=1)
                return False, 0, active_window + timedelta(minutes=1)
            except KeyError:
                self.counter[key] = [0, latest_window]
                return True, rate, latest_window + timedelta(minutes=1)

    def __call__(self, identifier_func: Callable, rate=2):
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
                    "api-ratelimit-limit": rate,
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
        self.counters:Dict[str,DefaultDict] = {}
        self.lock = threading.Lock()

    def consume(self, key, rate):
        with self.lock:
            now = datetime.now()
            now_minute_floor = now.replace(microsecond=0,second=0)
            rolling_lower_bound= now - timedelta(minutes=1)
            sub_window = now.replace(microsecond=0)
            #todo: this feels clunky
            try:
                _windows = self.counters[key]
                windows = {k:v for k,v in _windows.items() if k < rolling_lower_bound}
                self.counters[key] = windows
                windows[sub_window] +=1
                prior_sum = sum(windows[k] for k in windows.keys() if k < now_minute_floor)
                current_sum = sum(windows[k] for k in windows.keys() if k > now_minute_floor)
                weighted_sum = math.floor(prior_sum * (1-(now.minute/60)) + current_sum)
                if weighted_sum < rate:
                    return True, rate - weighted_sum, None
                else:
                    return False, rate - weighted_sum, None
            except KeyError:
                self.counters[key] = defaultdict(int)
                self.counters[key][sub_window] +=1
                return True, rate, None

    def __call__(self, identifier_func: Callable, rate=2):
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
                    "api-ratelimit-limit": rate,
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
