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

"""
This module contains utility functions for the tosfs package.
"""

import random
import re
import string


def random_path(length: int = 5) -> str:
    """
    Generate a random path(dir or file) of the given length.

    Args:
        length (int): The length of the random string.

    Returns:
        str: The random string.
    """
    return "".join(
        random.choices(string.ascii_letters + string.digits, k=length)
    )


def find_bucket_key(tos_path):
    """
    This is a helper function that given an tos path such that the path
    is of the form: bucket/key
    It will return the bucket and the key represented by the tos path
    """
    bucket_format_list = [
        re.compile(
            r"^(?P<bucket>:tos:[a-z\-0-9]*:[0-9]{12}:accesspoint[:/][^/]+)/?"  # noqa: E501
            r"(?P<key>.*)$"
        ),
        re.compile(
            r"^(?P<bucket>:tos-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
            # pylint: disable=line-too-long
            r"[a-zA-Z0-9\-]{1,63}[/:](bucket|accesspoint)[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"  # noqa: E501
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