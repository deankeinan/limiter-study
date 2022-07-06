from ast import Call
from datetime import datetime, timedelta
from functools import wraps
from tkinter import N
from typing import Callable
from flask import (
    Flask,
    jsonify,
    Request,
    render_template,
    render_template_string,
    request,
)
from rich import inspect
from functools import wraps
from typing import Callable
import abc
import time
from flask import Response
from rich import inspect

from storage import (
    LocalFixedWindowCounter,
    LocalLeakyBucket,
    LocalSlidingLog,
    LocalSlidingWindowCounter,
    LocalTokenBucket,
    RedisFixedWindowCounter,
    RequestRate,
)


def identify_by_client_ip():
    return request.remote_addr


def identify_by_api_key():
    return request.headers["api-key"]


app = Flask(__name__)


local_tb = LocalTokenBucket()
local_lb = LocalLeakyBucket()
local_fw = LocalFixedWindowCounter()
local_sl = LocalSlidingLog()
local_swc = LocalSlidingWindowCounter()
redis_fwc = RedisFixedWindowCounter('test_fwc')
# Intended rate limit for all limiters is 6 reqs/min
@app.route("/")
def hello_world():
    return render_template_string(
        """
        <html>

            <body>
                <h1>Hello World!</h1>
                <h2>intended rate limit for all limiters is 6reqs/min</h2>
                <ul>
                    <li><a href="/local/tb">Local_TokenBucket</a></li>
                    <li><a href="/local/lb">Local_LeakyBucket</a></li>
                    <li><a href="/local/fw">Local_FixedWindowCount</a></li>
                    <li><a href="/local/swl">Local_SlidingWindowLog</a></li>
                    <li><a href="/local/swc">Local_SlidingWindowCounter</a></li>
                    <li><a href="/redis/fw">Redis_FixedWindowCount</a></li>
                </ul>
            </body>

        </html>
        """
    )


SIX_PER_MIN = RequestRate(6, timedelta(minutes=1))


@app.route("/local/tb")
@local_tb(
    identifier_func=identify_by_client_ip, refill_rate=SIX_PER_MIN, token_capacity=6
)
def hello_local_tb():
    return "Hello! I'm rate limited by an in-memory TokenBucket algo"


@app.route("/local/lb")
@local_lb(
    identifier_func=identify_by_client_ip, leak_rate=SIX_PER_MIN, token_capacity=6
)
def hello_local_lb():
    return "Hello! I'm rate limited by an in-memory LeakyBucket algo"


@app.route("/local/fw")
@local_fw(identifier_func=identify_by_client_ip, rate=SIX_PER_MIN)
def hello_local_fw():
    return "Hello! I'm rate limited by an in-memory FixedWindowCounter algo"

@app.route("/redis/fw")
@redis_fwc(identifier_func=identify_by_client_ip, rate=SIX_PER_MIN)
def hello_redis_fwc():
    return "Hello! I'm rate limited by a redis based Fixed Window Counter algo"

@app.route("/local/swl")
@local_sl(identifier_func=identify_by_client_ip, rate=SIX_PER_MIN)
def hello_local_sl():
    return "Hello! I'm rate limited by an in-memory Sliding Window Log algo"


@app.route("/local/swc")
@local_swc(identifier_func=identify_by_client_ip, rate=SIX_PER_MIN)
def hello_local_swc():
    return "Hello! I'm rate limited by an in-memory Sliding Window Counter algo"


