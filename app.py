from ast import Call
from datetime import datetime, timedelta
from functools import wraps
from tkinter import N
from typing import Callable
from flask import Flask, jsonify, Request, request
from rich import inspect
from functools import wraps
from typing import Callable
import abc
import time
from flask import Response
from rich import inspect

from storage import (
    LeakyBucket,
    LocalFixedWindowCounter,
    LocalLeakyBucket,
    LocalTokenBucket,
    TokenBucket,
    WindowCounter,
)


def fixed_window_limiter(
    backend: WindowCounter, identifier_func: Callable, rate=2
):
    """
    Decorator that limits usage of the decorated function using a Fixed Window.

    Args:
        backend (FixedWindowCounter): a FixedWindow storage backend
        identifier_func (Callable): some function that discriminates each request
        rate (float): refill rate in tokens per second

    Returns:
        A 429 message if the rate limit is exceeded, the result of the decorated function otherwise.
    """

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            key = identifier_func()
            allow, remaining, next_reset = backend.consume(key, rate)
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


def token_bucket_limiter(
    backend: TokenBucket, identifier_func: Callable, rate=1, capacity=6, cost=1
):
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
            backend.refill(key, rate, capacity)
            allow, tokens, last_refill = backend.consume(key, cost)
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


def leaky_bucket_limiter(
    backend: LeakyBucket, identifier_func: Callable, rate=1, capacity=6, cost=1
):
    """
    Decorator that limits usage of the decorated function using a Leaky Bucket (as a meter).

    Args:
        backend (LocalLeakyBucket): a storage backend
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
            backend.leak(key, rate)
            allow, tokens, last_leak = backend.pour(key, capacity, cost)
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


def identify_by_client_ip():
    return request.remote_addr


app = Flask(__name__)

local_tb = LocalTokenBucket()
local_lb = LocalLeakyBucket()
local_fw = LocalFixedWindowCounter()


@app.route("/")
# @token_bucket_limiter(backend=local_tb,identifier_func=identify_by_client_ip,rate=0.5,capacity=10)
@leaky_bucket_limiter(
    backend=local_lb, identifier_func=identify_by_client_ip, rate=0.5, capacity=6
)
# @fixed_window_limiter(backend=local_fw,identifier_func=identify_by_client_ip,rate=2)
def hello_world():
    return "Hello, World!"
