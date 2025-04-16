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
"""It contains everything about certification via a file-based provider."""

import threading
from datetime import datetime, timedelta
from json import JSONDecodeError
from typing import Optional
from xml.etree import ElementTree

import requests
from tos.consts import ECS_DATE_FORMAT
from tos.credential import Credentials, CredentialsProvider

from tosfs.core import logger
from tosfs.exceptions import TosfsCertificationError

CERTIFICATION_REFRESH_INTERVAL_MINUTES = 60
CERTIFICATION_MAX_VALID_PERIOD_HOURS = 12


class FileCredentialsProvider(CredentialsProvider):
    """The class provides the credentials from a file.

    The file should be in the format of:
      <configuration>
        <property>
            <name>fs.tos.access-key-id</name>
            <value>access_key</value>
        </property>
        <property>
            <name>fs.tos.secret-access-key</name>
            <value>secret_key</value>
        </property>
        <property>
            <name>fs.tos.session-token</name>
            <value>session_token</value>
        </property>
      </configuration>

    It can only receive a file path which exists in the local file system.

    Note :
    1. The default refresh interval is 60 minutes.
    2. The maximum valid period for provisional certification is 12 hours.

    This provider will cache the credentials and refresh them every 60 minutes.
    And note that, it only reads the credentials from the file and refreshes itself,
    to guarantee the credentials are always up-to-date,
    we need a service update it internally.

    Examples
    --------
    >>> from tosfs import TosFileSystem
    >>> tosfs = TosFileSystem(
    >>>             endpoint="tos-cn-beijing.volcengine.com",
    >>>             regions="cn-bejing",
    >>>             credentials_provider=FileCredentialsProvider("dummy_path"))
    >>> tosfs.ls("tos://bucket/path")

    """

    def __init__(
        self,
        file_path: str,
        refresh_interval_min: int = CERTIFICATION_REFRESH_INTERVAL_MINUTES,
    ) -> None:
        """Initialize the FileCredentialsProvider."""
        self.file_path = file_path
        self.refresh_interval = refresh_interval_min
        if self.refresh_interval <= 0:
            logger.warning(
                f"Invalid refresh interval {self.refresh_interval}, "
                f"set to default value: 60 minutes"
            )
            self.refresh_interval = CERTIFICATION_REFRESH_INTERVAL_MINUTES
        if self.refresh_interval > CERTIFICATION_MAX_VALID_PERIOD_HOURS * 60:
            logger.warning(
                f"Invalid refresh interval {self.refresh_interval}, "
                f"set to maximum value: 60 minutes"
            )
            self.refresh_interval = CERTIFICATION_REFRESH_INTERVAL_MINUTES
        self.prev_refresh_time: Optional[datetime] = None
        self.credentials = None
        self._lock = threading.Lock()

    def get_credentials(self) -> Credentials:
        """Get the credentials from the file.

        Returns
        -------
        Credentials: The credentials object.

        Raises
        ------
        TosfsCertificationError: If the credentials cannot be retrieved.

        """
        res = self._try_get_credentials()
        if res is not None:
            return res
        with self._lock:
            try:
                res = self._try_get_credentials()
                if res is not None:
                    return res

                with open(self.file_path, "r") as f:
                    logger.debug(
                        f"Trying to refresh the credentials from file: "
                        f"{self.file_path}"
                    )
                    tree = ElementTree.parse(f)  # noqa S314
                    root = tree.getroot()

                    access_key_element = root.find(
                        ".//property[name='fs.tos.access-key-id']/value"
                    )
                    secret_key_element = root.find(
                        ".//property[name='fs.tos.secret-access-key']/value"
                    )
                    session_token_element = root.find(
                        ".//property[name='fs.tos.session-token']/value"
                    )

                    access_key = (
                        access_key_element.text
                        if access_key_element is not None
                        else None
                    )
                    secret_key = (
                        secret_key_element.text
                        if secret_key_element is not None
                        else None
                    )
                    session_token = (
                        session_token_element.text
                        if session_token_element is not None
                        else None
                    )

                    if None in (
                        access_key,
                        secret_key,
                        session_token,
                    ):
                        raise TosfsCertificationError(
                            "Missing required credential elements in the file"
                        )

                    self.prev_refresh_time = datetime.now()
                    self.credentials = Credentials(
                        access_key, secret_key, session_token
                    )
                    logger.debug(
                        f"Successfully refreshed the credentials from file: "
                        f"{self.file_path}"
                    )

                return self.credentials
            except Exception as e:
                raise TosfsCertificationError("Get certification error: ") from e

    def _try_get_credentials(self) -> Optional[Credentials]:
        if self.prev_refresh_time is None or self.credentials is None:
            return None
        if (
            datetime.now() - self.prev_refresh_time
        ).total_seconds() / 60 > CERTIFICATION_REFRESH_INTERVAL_MINUTES:
            logger.debug(
                f"Credentials are expired, "
                f"will try to refresh the credentials from file: "
                f"{self.file_path}"
            )
            return None
        return self.credentials


class UrlCredentialsProvider(CredentialsProvider):
    """The class provides the credentials from an url."""

    def __init__(self, credential_url: str):
        """Initialize the UrlCredentialsProvider."""
        if not credential_url:
            raise TosfsCertificationError("The credential_url param must not be empty.")
        self._lock = threading.Lock()
        self.expires: Optional[datetime] = None
        self.credentials = None
        self.credential_url = credential_url

    def get_credentials(self) -> Credentials:
        """Get the credentials from the url."""
        res = self._try_get_credentials()
        if res is not None:
            return res
        with self._lock:
            try:
                res = self._try_get_credentials()
                if res is not None:
                    return res

                from tosfs.retry import retryable_func_executor

                res = retryable_func_executor(
                    requests.get,
                    args=(self.credential_url,),
                    kwargs={"timeout": 60},
                    max_retry_num=3,
                )
                res_body = res.json()
                self.credentials = Credentials(
                    res_body.get("AccessKeyId"),
                    res_body.get("SecretAccessKey"),
                    res_body.get("SessionToken"),
                )
                self.expires = datetime.strptime(
                    res_body.get("ExpiredTime"), ECS_DATE_FORMAT
                )
                return self.credentials
            except JSONDecodeError as e:
                logger.error(
                    f"Failed to parse the response from the url: {self.credential_url}, "
                    f"error: {e}, detail: {e.doc}"
                )
                if self.expires is not None and (
                    datetime.now().timestamp() < self.expires.timestamp()
                ):
                    return self.credentials
                raise TosfsCertificationError(
                    "Get token failed, please check the credential url"
                ) from e
            except Exception as e:
                if self.expires is not None and (
                    datetime.now().timestamp() < self.expires.timestamp()
                ):
                    return self.credentials
                raise TosfsCertificationError(f"Get token failed: {e}") from e

    def _try_get_credentials(self) -> Optional[Credentials]:
        if self.expires is None or self.credentials is None:
            return None
        if (
            datetime.now().timestamp()
            > (self.expires - timedelta(minutes=10)).timestamp()
        ):
            return None
        return self.credentials


class NoLockUrlCredentialsProvider(CredentialsProvider):
    """The class provides the credentials from an url.

    It does not use lock to protect the credentials.
    Due to threading.Lock is not serializable,
    it can not be used in Ray remote function.
    """

    def __init__(self, credential_url: str):
        """Initialize the UrlCredentialsProvider."""
        if not credential_url:
            raise TosfsCertificationError("The credential_url param must not be empty.")
        self.expires: Optional[datetime] = None
        self.credentials = None
        self.credential_url = credential_url

    def get_credentials(self) -> Credentials:
        """Get the credentials from the url."""
        res = self._try_get_credentials()
        if res is not None:
            return res
        try:
            res = self._try_get_credentials()
            if res is not None:
                return res

            from tosfs.retry import retryable_func_executor

            res = retryable_func_executor(
                requests.get,
                args=(self.credential_url,),
                kwargs={"timeout": 60},
                max_retry_num=3,
            )
            res_body = res.json()
            self.credentials = Credentials(
                res_body.get("AccessKeyId"),
                res_body.get("SecretAccessKey"),
                res_body.get("SessionToken"),
            )
            self.expires = datetime.strptime(
                res_body.get("ExpiredTime"), ECS_DATE_FORMAT
            )
            return self.credentials
        except JSONDecodeError as e:
            logger.error(
                f"Failed to parse the response from the url: {self.credential_url}, "
                f"error: {e}, detail: {e.doc}"
            )
            if self.expires is not None and (
                    datetime.now().timestamp() < self.expires.timestamp()
            ):
                return self.credentials
            raise TosfsCertificationError(
                "Get token failed, please check the credential url"
            ) from e
        except Exception as e:
            if self.expires is not None and (
                datetime.now().timestamp() < self.expires.timestamp()
            ):
                return self.credentials
            raise TosfsCertificationError("Get token failed") from e

    def _try_get_credentials(self) -> Optional[Credentials]:
        if self.expires is None or self.credentials is None:
            return None
        if (
            datetime.now().timestamp()
            > (self.expires - timedelta(minutes=10)).timestamp()
        ):
            return None
        return self.credentials
