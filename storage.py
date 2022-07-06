from datetime import datetime, timedelta
from functools import wraps
from rich import inspect
from functools import wraps
import abc
import time
from rich import inspect

"""
Data structures & classes for various rate limiting algorithms with 
corresponding storage backends.
"""


class TokenBucket:
    def refill(self, key, rate, capacity):
        pass

    def consume(self, key, cost=1):
        pass


class LocalTokenBucket(TokenBucket):
    """
    A token bucket is a container that has pre-defined capacity, and is refilled at a preset rate.
    Once the bucket is full, additional tokens overflow.
    """

    def __init__(self):
        self.buckets = {}

    def refill(self, key, rate, capacity):
        try:
            current_tokens, last_refill = self.buckets[key]
            now = time.monotonic()

            # Catch race condition of another
            # thread refilling the bucket
            if now < last_refill:
                return

            additional_tokens = rate * (now - last_refill)
            self.buckets[key] = [min(capacity, current_tokens + additional_tokens), now]
        except KeyError:
            self.buckets[key] = [capacity, time.monotonic()]

    def consume(self, key, cost=1):
        current_tokens, last_refill = self.buckets[key]
        if current_tokens < cost:
            return False, current_tokens, last_refill
        self.buckets[key][0] -= cost
        return True, current_tokens - cost, last_refill

    def inspect(self):
        inspect(self.buckets)


class LeakyBucket:
    def pour(self, key, capacity, cost):
        pass

    def leak(self, key, rate):
        pass


class LocalLeakyBucket(LeakyBucket):
    """
    This implementation of a Leaky Bucket is based on the
    leaky bucket 'as a meter' description found in reference texts.

    'as a queue' confuses me and I'd like to review a minimal implementation of it.
    """

    def __init__(self) -> None:
        self.buckets = {}

    def pour(self, key, capacity, cost=1):
        current_tokens, last_leak = self.buckets[key]
        if current_tokens + cost > capacity:
            return False, current_tokens, last_leak
        self.buckets[key][0] += cost
        return True, current_tokens + cost, last_leak

    def leak(self, key, rate):
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


class WindowCounter:
    def consume(self, key, rate):
        pass


class LocalFixedWindowCounter(WindowCounter):
    """
    A fixed window counter maps an identifier to a (count, timestamp) tuple.
    Rate limits are imposed by a maximum rate for each fixed timestamp 'window'.
    """

    def __init__(self):
        self.counter = {}

    def consume(self, key, rate):
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
