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

"""The module contains all the business logic for tagging tos buckets ."""

import fcntl
import functools
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from volcengine.ApiInfo import ApiInfo
from volcengine.base.Service import Service
from volcengine.Credentials import Credentials
from volcengine.ServiceInfo import ServiceInfo

from tosfs.consts import ENV_NAME_VOLC_REGION

PUT_TAG_ACTION_NAME = "PutBucketDoubleMeterTagging"
GET_TAG_ACTION_NAME = "GetBucketTagging"
EMR_OPEN_API_VERSION = "2022-12-29"
OPEN_API_HOST = "open.volcengineapi.com"
ACCEPT_HEADER_KEY = "accept"
ACCEPT_HEADER_JSON_VALUE = "application/json"

THREAD_POOL_SIZE = 2
TAGGED_BUCKETS_FILE = "/tmp/.emr_tagged_buckets"

CONNECTION_TIMEOUT_DEFAULT_SECONDS = 60 * 5
SOCKET_TIMEOUT_DEFAULT_SECONDS = 60 * 5

service_info_map = {
    "cn-beijing": ServiceInfo(
        OPEN_API_HOST,
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr", "cn-beijing"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "http",
    ),
    "cn-guangzhou": ServiceInfo(
        OPEN_API_HOST,
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr", "cn-guangzhou"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "http",
    ),
    "cn-shanghai": ServiceInfo(
        OPEN_API_HOST,
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr", "cn-shanghai"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "http",
    ),
    "ap-southeast-1": ServiceInfo(
        OPEN_API_HOST,
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr", "ap-southeast-1"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "http",
    ),
    "cn-beijing-qa": ServiceInfo(
        OPEN_API_HOST,
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr_qa", "cn-beijing"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "http",
    ),
    "cn-beijing-selfdrive": ServiceInfo(
        OPEN_API_HOST,
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr", "cn-beijing-selfdrive"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "http",
    ),
    "cn-shanghai-autodriving": ServiceInfo(
        "emr.cn-shanghai-autodriving.volcengineapi.com",
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr", "cn-shanghai-autodriving"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "https",
    ),
    "cn-beijing-autodriving": ServiceInfo(
        "emr.cn-beijing-autodriving.volcengineapi.com",
        {
            ACCEPT_HEADER_KEY: ACCEPT_HEADER_JSON_VALUE,
        },
        Credentials("", "", "emr", "cn-beijing-autodriving"),
        CONNECTION_TIMEOUT_DEFAULT_SECONDS,
        SOCKET_TIMEOUT_DEFAULT_SECONDS,
        "https",
    ),
}

api_info = {
    PUT_TAG_ACTION_NAME: ApiInfo(
        "POST",
        "/",
        {"Action": PUT_TAG_ACTION_NAME, "Version": EMR_OPEN_API_VERSION},
        {},
        {},
    ),
    GET_TAG_ACTION_NAME: ApiInfo(
        "GET",
        "/",
        {"Action": GET_TAG_ACTION_NAME, "Version": EMR_OPEN_API_VERSION},
        {},
        {},
    ),
}


class BucketTagAction(Service):
    """BucketTagAction is a class to manage the tag of bucket."""

    _instance_lock = threading.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        """Singleton."""
        if not hasattr(BucketTagAction, "_instance"):
            with BucketTagAction._instance_lock:
                if not hasattr(BucketTagAction, "_instance"):
                    BucketTagAction._instance = object.__new__(cls)
        return BucketTagAction._instance

    def __init__(
        self,
        key: Optional[str],
        secret: Optional[str],
        session_token: Optional[str],
        region: str,
    ) -> None:
        """Init BucketTagAction."""
        super().__init__(self.get_service_info(region), self.get_api_info())
        if key:
            self.set_ak(key)

        if secret:
            self.set_sk(secret)

        if session_token:
            self.set_session_token(session_token)

    @staticmethod
    def get_api_info() -> dict:
        """Get api info."""
        return api_info

    @staticmethod
    def get_service_info(region: str) -> ServiceInfo:
        """Get service info."""
        service_info = service_info_map.get(region)
        if service_info:
            from tosfs.core import logger

            logger.debug(f"The service name is : {service_info.credentials.service}")
            return service_info

        raise Exception(f"Do not support region: {region}")

    def put_bucket_tag(self, bucket: str) -> tuple[str, bool]:
        """Put tag for bucket."""
        params = {
            "Bucket": bucket,
        }

        try:
            res = self.json(PUT_TAG_ACTION_NAME, params, json.dumps(""))
            res_json = json.loads(res)
            logging.debug("Put tag for bucket %s successfully: %s .", bucket, res_json)
            return bucket, True
        except Exception as e:
            logging.debug("Put tag for bucket %s failed: %s .", bucket, e)
            return bucket, False

    def get_bucket_tag(self, bucket: str) -> bool:
        """Get tag for bucket."""
        params = {
            "Bucket": bucket,
        }
        try:
            res = self.get(GET_TAG_ACTION_NAME, params)
            res_json = json.loads(res)
            logging.debug("The get bucket tag's response is %s", res_json)
            return True
        except Exception as e:
            logging.debug("Get tag for %s is failed: %s", bucket, e)
            return False


def singleton(cls: Any) -> Any:
    """Singleton decorator."""
    _instances = {}

    @functools.wraps(cls)
    def get_instance(*args: Any, **kwargs: Any) -> Any:
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance


@singleton
class BucketTagMgr:
    """BucketTagMgr is a class to manage the tag of bucket."""

    def __init__(
        self,
        key: Optional[str],
        secret: Optional[str],
        session_token: Optional[str],
        region: str,
    ):
        """Init BucketTagMgr."""
        self.executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
        self.cached_bucket_set: set = set()
        self.key = key
        self.secret = secret
        self.session_token = session_token
        self.region = region

        actual_region = os.environ.get(ENV_NAME_VOLC_REGION)
        if actual_region:
            from tosfs.core import logger

            logger.debug(
                f"Get the region from {ENV_NAME_VOLC_REGION} env, "
                f"value: {actual_region}."
            )
        else:
            actual_region = self.region

        self.bucket_tag_service = BucketTagAction(
            self.key, self.secret, self.session_token, actual_region
        )

    def add_bucket_tag(self, bucket: str) -> None:
        """Add tag for bucket."""
        from tosfs.core import logger

        collect_bucket_set = {bucket}

        if not collect_bucket_set - self.cached_bucket_set:
            return

        if os.path.exists(TAGGED_BUCKETS_FILE):
            with open(TAGGED_BUCKETS_FILE, "r") as file:
                tagged_bucket_from_file_set = set(file.read().split(" "))
                from tosfs.core import logger

                logger.debug(
                    f"Marked tagged buckets in the file : "
                    f"{tagged_bucket_from_file_set}"
                )
            self.cached_bucket_set |= tagged_bucket_from_file_set

        need_tag_buckets = collect_bucket_set - self.cached_bucket_set
        logger.debug(f"Need to tag buckets : {need_tag_buckets}")

        for res in self.executor.map(
            self.bucket_tag_service.put_bucket_tag, need_tag_buckets
        ):
            if res[1]:
                self.cached_bucket_set.add(res[0])

        with open(TAGGED_BUCKETS_FILE, "w") as fw:
            fcntl.flock(fw, fcntl.LOCK_EX)
            fw.write(" ".join(self.cached_bucket_set))
            fcntl.flock(fw, fcntl.LOCK_UN)
            fw.close()
