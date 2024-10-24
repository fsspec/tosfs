# ByteDance Volcengine EMR, Copyright 2024.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The module contains retry utility functions for the tosfs stability."""
import math
import time
from typing import Any, Optional, Tuple

import requests
from requests import RequestException
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectTimeout,
    HTTPError,
    ProxyError,
    ReadTimeout,
    RetryError,
    SSLError,
    StreamConsumedError,
    Timeout,
)
from tos.exceptions import TosClientError, TosError, TosServerError

from tosfs.exceptions import TosfsError

CONFLICT_CODE = 409
TOO_MANY_REQUESTS_CODE = 429
SERVICE_UNAVAILABLE = 503

TOS_SERVER_RETRYABLE_STATUS_CODES = {
    CONFLICT_CODE,
    TOO_MANY_REQUESTS_CODE,
    500,  # INTERNAL_SERVER_ERROR,
    SERVICE_UNAVAILABLE,
}

TOS_SERVER_NOT_RETRYABLE_CONFLICT_ERROR_CODES = {
    "0026-00000013",  # DELETE_NON_EMPTY_DIR
    "0026-00000020",  # LOCATED_UNDER_A_FILE
    "0026-00000021",  # COPY_BETWEEN_DIR_AND_FILE
    "0026-00000022",  # PATH_LOCK_CONFLICT
    "0026-00000025",  # RENAME_TO_AN_EXISTED_DIR
    "0026-00000026",  # RENAME_TO_SUB_DIR
    "0026-00000027",  # RENAME_BETWEEN_DIR_AND_FILE
    "0017-00000208",  # APPEND_OFFSET_NOT_MATCHED
    "0017-00000209",  # APPEND_NOT_APPENDABLE
}

TOS_CLIENT_RETRYABLE_EXCEPTIONS = {
    HTTPError,
    requests.ConnectionError,
    ProxyError,
    SSLError,
    Timeout,
    ConnectTimeout,
    ReadTimeout,
    StreamConsumedError,
    RetryError,
    InterruptedError,
    ConnectionResetError,
    ConnectionError,
    ChunkedEncodingError,
    RequestException,
}

MAX_RETRY_NUM = 20
SLEEP_BASE_SECONDS = 0.1
SLEEP_MAX_SECONDS = 60


def retryable_func_executor(
    func: Any,
    *,
    args: Tuple[Any, ...] = (),
    kwargs: Optional[Any] = None,
    max_retry_num: int = MAX_RETRY_NUM,
) -> Any:
    """Retry a function in case of catch errors."""
    if kwargs is None:
        kwargs = {}

    attempt = 0

    while attempt < max_retry_num:
        attempt += 1
        try:
            return func(*args, **kwargs)
        except TosError as e:
            from tosfs.core import logger

            if attempt >= max_retry_num:
                logger.error("Retry exhausted after %d times.", max_retry_num)
                raise e

            if is_retryable_exception(e):
                logger.warning(
                    "Retry TOS request in the %d times, error: %s", attempt, e
                )
                try:
                    sleep_time = _get_sleep_time(e, attempt)
                    time.sleep(sleep_time)
                except InterruptedError as ie:
                    raise TosfsError(f"Request {func} interrupted.") from ie
            else:
                _rethrow_retryable_exception(e)
        # Note: maybe not all the retryable exceptions are warped by `TosError`
        # Will pay attention to those cases
        except Exception as e:
            raise TosfsError(f"{e}") from e


def _rethrow_retryable_exception(e: TosError) -> None:
    """For debug purpose."""
    raise e


def is_retryable_exception(e: TosError) -> bool:
    """Check if the exception is retryable."""
    return _is_retryable_tos_server_exception(e) or _is_retryable_tos_client_exception(
        e
    )


def _is_retryable_tos_server_exception(e: TosError) -> bool:
    if not isinstance(e, TosServerError):
        return False

    # not all conflict errors are retryable
    if e.status_code == CONFLICT_CODE:
        return (
            e.ec not in TOS_SERVER_NOT_RETRYABLE_CONFLICT_ERROR_CODES
            and
            # TODO: currently, hack for supporting hns,
            #  need to refactor when tos python sdk GA
            e.header._store["x-tos-ec"][1]
            not in TOS_SERVER_NOT_RETRYABLE_CONFLICT_ERROR_CODES
        )

    return e.status_code in TOS_SERVER_RETRYABLE_STATUS_CODES


def _is_retryable_tos_client_exception(e: TosError) -> bool:
    if isinstance(e, TosClientError):
        cause = e.cause
        while cause is not None:
            for excp in TOS_CLIENT_RETRYABLE_EXCEPTIONS:
                if isinstance(cause, excp):
                    return True
            cause = getattr(cause, "cause", None)
    return False


def _get_sleep_time(err: TosError, retry_count: int) -> float:
    sleep_time = SLEEP_BASE_SECONDS * math.pow(2, retry_count)
    sleep_time = min(sleep_time, SLEEP_MAX_SECONDS)
    if (
        isinstance(err, TosServerError)
        and (
            err.status_code == TOO_MANY_REQUESTS_CODE
            or err.status_code == SERVICE_UNAVAILABLE
        )
        and "retry-after" in err.headers
    ):
        try:
            sleep_time = max(int(err.headers["retry-after"]), int(sleep_time))
        except Exception as e:
            from tosfs.core import logger

            logger.warning("try to parse retry-after from headers error: {}".format(e))
    return sleep_time
