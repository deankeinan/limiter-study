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
    LocalTokenBucket,
)


def identify_by_client_ip():
    return request.remote_addr

def identify_by_api_key():
    return request.headers['api-key']

app = Flask(__name__)


local_tb = LocalTokenBucket()
local_lb = LocalLeakyBucket()
local_fw = LocalFixedWindowCounter()
local_sl = LocalSlidingLog()

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
                </ul>
            </body>

        </html>
        """
    )


@app.route("/local/tb")
@local_tb(identifier_func=identify_by_api_key, rate=0.1, capacity=6)
def hello_local_tb():
    return "Hello! I'm rate limited by an in-memory TokenBucket algo"


@app.route("/local/lb")
@local_lb(identifier_func=identify_by_api_key, rate=0.1, capacity=6)
def hello_local_lb():
    return "Hello! I'm rate limited by an in-memory LeakyBucket algo"


@app.route("/local/fw")
@local_fw(identifier_func=identify_by_api_key, rate=6)
def hello_local_fw():
    return "Hello! I'm rate limited by an in-memory FixedWindowCounter algo"


@app.route("/local/swl")
@local_sl(identifier_func=identify_by_api_key, rate=6)
def hello_local_sl():
    return "Hello! I'm rate limited by an in-memory Sliding Window Log algo"
