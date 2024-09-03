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

"""The module contains utility functions for the tosfs package."""

import random
import re
import string
import tempfile
import time
from typing import Any, Optional, Tuple

import tos

from tosfs.consts import TOS_SERVER_RETRYABLE_ERROR_CODE_SET


def random_str(length: int = 5) -> str:
    """Generate a random string of the given length.

    Args:
    ----
    length (int): The length of the random string.

    Returns:
    -------
    str: The random string.

    """
    return "".join(random.choices(string.ascii_letters, k=length))


def create_temp_dir() -> str:
    """Create a temporary directory for testing purposes.

    Returns
    -------
    str: The path of the created temporary directory.

    """
    return tempfile.mkdtemp()


def find_bucket_key(tos_path: str) -> Tuple[str, str]:
    """It's a helper function to find bucket and key pair.

    It receives a tos path such that the path
    is of the form: bucket/key.
    It will return the bucket and the key represented by the tos path.
    """
    bucket_format_list = [
        re.compile(
            r"^(?P<bucket>:tos:[a-z\-0-9]*:[0-9]{12}:accesspoint[:/][^/]+)/?"
            r"(?P<key>.*)$"
        ),
        re.compile(
            r"^(?P<bucket>:tos-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
            # pylint: disable=line-too-long
            r"[a-zA-Z0-9\-]{1,63}[/:](bucket|accesspoint)[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
        ),
        re.compile(
            r"^(?P<bucket>:tos-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
            r"[a-zA-Z0-9\-]{1,63}[/:]bucket[/:]"
            r"[a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
        ),
        re.compile(
            r"^(?P<bucket>:tos-object-lambda:[a-z\-0-9]+:[0-9]{12}:"
            r"accesspoint[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
        ),
    ]
    for bucket_format in bucket_format_list:
        match = bucket_format.match(tos_path)
        if match:
            return match.group("bucket"), match.group("key")
    tos_components = tos_path.split("/", 1)
    bucket = tos_components[0]
    tos_key = ""
    if len(tos_components) > 1:
        tos_key = tos_components[1]
    return bucket, tos_key


def retryable_func_wrapper(
    func: Any, *, args: tuple[()] = (), kwargs: Optional[Any] = None, retries: int = 5
) -> Any:
    """Retry a function in case of server errors."""
    if kwargs is None:
        kwargs = {}

    err = None

    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except tos.exceptions.TosServerError as e:
            err = e
            from tosfs.core import logger

            logger.debug("Server error (maybe retryable): %s", e)
            if e.code in TOS_SERVER_RETRYABLE_ERROR_CODE_SET:
                time.sleep(min(1.7**i * 0.1, 15))
            else:
                break
        except Exception as e:
            err = e
            from tosfs.core import logger

            logger.debug("Nonretryable error: %s", e)
            break

    raise err if err is not None else ""
