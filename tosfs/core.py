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

"""The core module of TOSFS."""
import io
import logging
import mimetypes
import os
import tempfile
import warnings
from glob import has_magic
from typing import Any, BinaryIO, Collection, Generator, List, Optional, Tuple, Union

import tos
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import other_paths
from fsspec.utils import setup_logging as setup_logger
from tos.auth import CredentialProviderAuth
from tos.exceptions import TosClientError, TosServerError
from tos.models import CommonPrefixInfo
from tos.models2 import (
    ListedObject,
    ListedObjectVersion,
    ListObjectType2Output,
    PartInfo,
    UploadPartCopyOutput,
)

from tosfs.compatible import FsspecCompatibleFS
from tosfs.consts import (
    ENV_NAME_TOS_BUCKET_TAG_ENABLE,
    ENV_NAME_TOS_SDK_LOGGING_LEVEL,
    ENV_NAME_TOSFS_LOGGING_LEVEL,
    FILE_OPERATION_READ_WRITE_BUFFER_SIZE,
    GET_OBJECT_OPERATION_DEFAULT_READ_CHUNK_SIZE,
    LS_OPERATION_DEFAULT_MAX_ITEMS,
    MANAGED_COPY_MAX_THRESHOLD,
    MANAGED_COPY_MIN_THRESHOLD,
    MPU_PART_SIZE_THRESHOLD,
    PART_MAX_SIZE,
    PUT_OBJECT_OPERATION_SMALL_FILE_THRESHOLD,
    TOS_BUCKET_TYPE_FNS,
    TOS_BUCKET_TYPE_HNS,
    TOS_SERVER_STATUS_CODE_NOT_FOUND,
    TOSFS_LOG_FORMAT,
)
from tosfs.exceptions import TosfsError
from tosfs.fsspec_utils import glob_translate
from tosfs.models import DeletingObject
from tosfs.retry import CONFLICT_CODE, INVALID_RANGE_CODE, retryable_func_executor
from tosfs.tag import BucketTagMgr
from tosfs.utils import find_bucket_key, get_brange
from tosfs.version import Version

logger = logging.getLogger("tosfs")


def setup_logging() -> None:
    """Set up the logging configuration for TOSFS."""
    setup_logger(
        logger=logger,
        level=os.environ.get(ENV_NAME_TOSFS_LOGGING_LEVEL, "WARNING"),
    )

    formatter = logging.Formatter(TOSFS_LOG_FORMAT)
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    # set and config tos client's logger
    tos.set_logger(
        name="tosclient",
        level=os.environ.get(ENV_NAME_TOS_SDK_LOGGING_LEVEL, "WARNING"),
        log_handler=logging.StreamHandler(),
        format_string=TOSFS_LOG_FORMAT,
    )


setup_logging()

if logger.level < logging.WARNING:
    logger.warning(
        "The tosfs's log level is set to be %s", logging.getLevelName(logger.level)
    )


class TosFileSystem(FsspecCompatibleFS):
    """Tos file system.

    It's an implementation of AbstractFileSystem which is an
    abstract super-class for pythonic file-systems.
    """

    protocol = ("tos",)

    def __init__(
            self,
            endpoint: Optional[str] = None,
            key: str = "",
            secret: str = "",
            region: Optional[str] = None,
            session_token: Optional[str] = None,
            max_retry_num: int = 20,
            max_connections: int = 1024,
            connection_timeout: int = 10,
            socket_timeout: int = 30,
            high_latency_log_threshold: int = 100,
            version_aware: bool = False,
            credentials_provider: Optional[object] = None,
            default_block_size: Optional[int] = None,
            default_fill_cache: bool = True,
            default_cache_type: str = "readahead",
            multipart_staging_dirs: str = tempfile.mkdtemp(),
            multipart_size: int = 8 << 20,
            multipart_thread_pool_size: int = max(2, os.cpu_count() or 1),
            multipart_threshold: int = 5 << 20,
            enable_crc: bool = True,
            enable_verify_ssl: bool = True,
            dns_cache_timeout: int = 0,
            proxy_host: Optional[str] = None,
            proxy_port: Optional[int] = None,
            proxy_username: Optional[str] = None,
            proxy_password: Optional[str] = None,
            disable_encoding_meta: Optional[bool] = None,
            except100_continue_threshold: int = 65536,
            endpoint_url: Optional[str] = None,  # Deprecated parameter
            **kwargs: Any,
    ) -> None:
        """Initialise the TosFileSystem.

        Parameters
        ----------
        endpoint : str, optional
            The endpoint of the TOS service.
        key : str
            The access key ID(ak) to access the TOS service.
        secret : str
            The secret access key(sk) to access the TOS service.
        region : str, optional
            The region of the TOS service.
        session_token : str, optional
            The temporary session token to access the TOS service.
        max_retry_num : int, optional
            The maximum number of retries for a failed request (default is 20).
        max_connections : int, optional
            The maximum number of HTTP connections that can be opened in the
            connection pool (default is 1024).
        connection_timeout : int, optional
            The time to keep a connection open in seconds (default is 10).
        socket_timeout : int, optional
            The socket read and write timeout time for a single request after
            a connection is successfully established, in seconds.
            The default is 30 seconds.
            Reference: https://requests.readthedocs.io/en/latest/user/quickstart/
            #timeouts (default is 30).
        high_latency_log_threshold : int, optional
            The threshold for logging high latency operations. When greater than 0,
            it represents enabling high-latency logs. The unit is KB.
            By default, it is 100.
            When the total transmission rate of a single request is lower than
            this value and the total request time is greater than 500 milliseconds,
            WARN-level logs are printed.
        version_aware : bool, optional
            Whether the filesystem is version aware (default is False).
            Currently, not been supported, please DO NOT set to True.
        credentials_provider : object, optional
            The credentials provider for the TOS service.
        default_block_size : int, optional
            The default block size for reading and writing (default is None).
        default_fill_cache : bool, optional
            Whether to fill the cache (default is True).
        default_cache_type : str, optional
            The default cache type (default is 'readahead').
        multipart_staging_dirs : str, optional
            The staging directories for multipart uploads (default is a temporary
            directory). Separate the staging dirs with comma if there are many
            staging dir paths.
        multipart_size : int, optional
            The multipart upload part size of the given object storage.
            (default is 8MB).
        multipart_thread_pool_size : int, optional
            The size of thread pool used for uploading multipart in parallel for the
            given object storage. (default is max(2, os.cpu_count()).
        multipart_threshold : int, optional
            The threshold which control whether enable multipart upload during
            writing data to the given object storage, if the write data size is less
            than threshold, will write data via simple put instead of multipart upload.
            default is 10 MB.
        enable_crc : bool
            Whether to enable client side CRC check after upload, default is true
        enable_verify_ssl : bool
            Whether to verify the SSL certificate, default is true.
        dns_cache_timeout : int
            The DNS cache timeout in minutes, if it is less than or equal to 0,
            it means to close the DNS cache, default is 0.
        proxy_host : str, optional
            The host address of the proxy server, currently only supports
            the http protocol.
        proxy_port : int, optional
            The port of the proxy server.
        proxy_username : str, optional
            The username to use when connecting to the proxy server.
        proxy_password : str, optional
            The password to use when connecting to the proxy server.
        disable_encoding_meta : bool, optional
            Whether to encode user-defined metadata x-tos-meta- Content-Disposition,
            default encoding, no encoding when set to true.
        except100_continue_threshold : int
            When it is greater than 0, it means that the interface related to the
            upload object opens the 100-continue mechanism for requests with the
            length of the data to be uploaded greater than the threshold
            (if the length of the data cannot be predicted, it is uniformly determined
            to be greater than the threshold), unit byte, default 65536
        endpoint_url : str, optional
            (deprecated) The endpoint URL of the TOS service.
        kwargs : Any, optional
            Additional arguments.

        """
        if endpoint_url is not None:
            warnings.warn(
                "The 'endpoint_url' parameter is deprecated and will be removed"
                " in a future release. Please use 'endpoint' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            endpoint = endpoint_url

        self.tos_client = tos.TosClientV2(
            key,
            secret,
            endpoint,
            region,
            security_token=session_token,
            max_retry_count=0,
            max_connections=max_connections,
            connection_time=connection_timeout,
            socket_timeout=socket_timeout,
            high_latency_log_threshold=high_latency_log_threshold,
            credentials_provider=credentials_provider,
            enable_crc=enable_crc,
            enable_verify_ssl=enable_verify_ssl,
            disable_encoding_meta=disable_encoding_meta,
            dns_cache_time=dns_cache_timeout,
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_username=proxy_username,
            proxy_password=proxy_password,
            except100_continue_threshold=except100_continue_threshold,
            user_agent_product_name="EMR",
            user_agent_soft_name="TOSFS",
            user_agent_soft_version=Version.version,
            user_agent_customized_key_values={"revision": Version.revision},
        )
        if version_aware:
            raise ValueError("Currently, version_aware is not supported.")

        self.tag_enabled = (
                os.environ.get(ENV_NAME_TOS_BUCKET_TAG_ENABLE, "true").lower() == "true"
        )
        if self.tag_enabled:
            logger.debug("The tos bucket tag is enabled.")
            self._init_tag_manager()

        self.version_aware = version_aware
        self.default_block_size = (
                default_block_size or FILE_OPERATION_READ_WRITE_BUFFER_SIZE
        )
        self.default_fill_cache = default_fill_cache
        self.default_cache_type = default_cache_type
        self.max_retry_num = max_retry_num

        self.multipart_staging_dirs = [
            d.strip() for d in multipart_staging_dirs.split(",")
        ]
        self.multipart_size = multipart_size
        self.multipart_thread_pool_size = multipart_thread_pool_size
        self.multipart_threshold = multipart_threshold

        super().__init__(**kwargs)

    def _open(
            self,
            path: str,
            mode: str = "rb",
            block_size: Optional[int] = None,
            version_id: Optional[str] = None,
            fill_cache: Optional[bool] = None,
            cache_type: Optional[str] = None,
            autocommit: bool = True,
            **kwargs: Any,
    ) -> AbstractBufferedFile:
        """Open a file for reading or writing.

        Parameters
        ----------
        path: string
            Path of file on TOS
        mode: string
            One of 'r', 'w', 'a', 'rb', 'wb', or 'ab'. These have the same meaning
            as they do for the built-in `open` function.
        block_size: int
            Size of data-node blocks if reading
        version_id : str
            Explicit version of the object to open.  This requires that the tos
            filesystem is version aware and bucket versioning is enabled on the
            relevant bucket.
        fill_cache: bool
            If seeking to new a part of the file beyond the current buffer,
            with this True, the buffer will be filled between the sections to
            best support random access. When reading only a few specific chunks
            out of a file, performance may be better if False.
        cache_type : str
            See fsspec's documentation for available cache_type values. Set to "none"
            if no caching is desired. If None, defaults to ``self.default_cache_type``.
        autocommit : bool
            If True, writes will be committed to the filesystem on flush or close.
        kwargs: dict-like
            Additional parameters.

        """
        if block_size is None:
            block_size = self.default_block_size
        if fill_cache is None:
            fill_cache = self.default_fill_cache

        if version_id:
            raise ValueError(
                "version_id cannot be specified if the filesystem "
                "is not version aware"
            )

        if cache_type is None:
            cache_type = self.default_cache_type

        return TosFile(
            self,
            path,
            mode,
            block_size=block_size,
            version_id=version_id,
            fill_cache=fill_cache,
            cache_type=cache_type,
            autocommit=autocommit,
        )

    def ls(
            self,
            path: str,
            detail: bool = False,
            versions: bool = False,
            **kwargs: Union[str, bool, float, None],
    ) -> Union[List[dict], List[str]]:
        """List objects under the given path.

        Parameters
        ----------
        path : str
            The path to list.
        detail : bool, optional
            Whether to return detailed information (default is False).
        versions : bool, optional
            Whether to list object versions (default is False).
        **kwargs : dict, optional
            Additional arguments.

        Returns
        -------
        Union[List[dict], List[str]]
            A list of objects under the given path. If `detail` is True,
            returns a list of dictionaries with detailed information.
            Otherwise, returns a list of object names.

        Raises
        ------
        IOError
            If there is an error accessing the parent directory.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.ls("mybucket")
        ['mybucket/file1', 'mybucket/file2']
        >>> fs.ls("mybucket", detail=True)
        [{'name': 'mybucket/file1', 'size': 123, 'type': 'file'},
        {'name': 'mybucket/file2', 'size': 456, 'type': 'file'}]

        """
        path = self._strip_protocol(path)
        if path in ["", "/"]:
            files = self._ls_buckets()
            return files if detail else sorted([o["name"] for o in files])

        files = self._ls_dirs_and_files(path, versions=versions)
        if not files and "/" in path:
            try:
                files = self._ls_dirs_and_files(self._parent(path), versions=versions)
            except IOError:
                pass
            files = [
                o
                for o in files
                if o["name"].rstrip("/") == path and o["type"] != "directory"
            ]

        return files if detail else sorted([o["name"] for o in files])

    def ls_iterate(
            self,
            path: str,
            detail: bool = False,
            versions: bool = False,
            batch_size: int = LS_OPERATION_DEFAULT_MAX_ITEMS,
            recursive: bool = False,
            **kwargs: Union[str, bool, float, None],
    ) -> Generator[Union[List[dict], List[str]], None, None]:
        """List objects under the given path in batches then returns an iterator.

        Parameters
        ----------
        path : str
            The path to list.
        detail : bool, optional
            Whether to return detailed information (default is False).
        versions : bool, optional
            Whether to list object versions (default is False).
        batch_size : int, optional
            The number of items to fetch in each batch (default is 1000).
        recursive : bool, optional
            Whether to list objects recursively (default is False).
        **kwargs : dict, optional
            Additional arguments.

        Returns
        -------
        Generator[Union[dict, str], None, None]
            An iterator that yields objects under the given path.

        Raises
        ------
        ValueError
            If versions is specified but the filesystem is not version aware.

        """
        if versions:
            raise ValueError(
                "versions cannot be specified if the filesystem "
                "is not version aware."
            )

        path = self._strip_protocol(path)
        bucket, key, _ = self._split_path(path)

        if recursive and self._is_hns_bucket(bucket):
            raise ValueError("Recursive listing is not supported for HNS bucket.")

        prefix = key.lstrip("/") + "/" if key else ""
        continuation_token = ""
        is_truncated = True

        while is_truncated:

            def _call_list_objects_type2(
                    continuation_token: str = continuation_token,
            ) -> ListObjectType2Output:
                return self.tos_client.list_objects_type2(
                    bucket,
                    prefix,
                    start_after=prefix,
                    delimiter=None if recursive else "/",
                    max_keys=batch_size,
                    continuation_token=continuation_token,
                )

            resp = retryable_func_executor(
                _call_list_objects_type2,
                args=(continuation_token,),
                max_retry_num=self.max_retry_num,
            )
            is_truncated = resp.is_truncated
            continuation_token = resp.next_continuation_token
            results = resp.contents + resp.common_prefixes

            batch = []
            for obj in results:
                if isinstance(obj, CommonPrefixInfo):
                    info = self._fill_dir_info(bucket, obj)
                elif obj.key.endswith("/"):
                    info = self._fill_dir_info(bucket, None, obj.key)
                else:
                    info = self._fill_file_info(obj, bucket, versions)

                batch.append(info if detail else info["name"])

            yield batch

    def info(
            self,
            path: str,
            bucket: Optional[str] = None,
            key: Optional[str] = None,
            version_id: Optional[str] = None,
    ) -> dict:
        """Give details of entry at path.

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.

        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.

        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.

        """
        if path in ["/", ""]:
            return {"name": path, "size": 0, "type": "directory"}
        path = self._strip_protocol(path)
        bucket, key, path_version_id = self._split_path(path)
        fullpath = "/".join((bucket, key))

        if version_id:
            raise ValueError(
                "version_id cannot be specified due to the "
                "filesystem is not support version aware."
            )

        if not key:
            return self._bucket_info(bucket)

        bucket_type = self._get_bucket_type(bucket)
        if bucket_type == TOS_BUCKET_TYPE_FNS:
            result = self._object_info(bucket, key, version_id)

            if not result:
                result = self._get_dir_info(bucket, key, fullpath)
        else:
            # Priority is given to judging dir, followed by file.
            result = self._get_dir_info(bucket, key, fullpath)

            if not result:
                result = self._object_info(bucket, key, version_id)

        if not result:
            raise FileNotFoundError(f"Can not get information for path: {path}")

        return result

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a path exists in the TOS.

        Parameters
        ----------
        path : str
            The path to check for existence.
        **kwargs : Any, optional
            Additional arguments if needed in the future.

        Returns
        -------
        bool
            True if the path exists, False otherwise.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while checking the path.
        tos.exceptions.TosServerError
            If there is a server error while checking the path.
        TosfsError
            If there is an unknown error while checking the path.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.exists("tos://bucket/to/file")
        True
        >>> fs.exists("tos://mybucket/nonexistentfile")
        False

        """
        if path in ["", "/"]:
            # the root always exists
            return True

        path = self._strip_protocol(path)
        bucket, key, version_id = self._split_path(path)
        # if the path is a bucket
        if not key:
            return self._exists_bucket(bucket)

        try:
            resp = retryable_func_executor(
                lambda: self.tos_client.get_file_status(bucket, key),
                max_retry_num=self.max_retry_num,
            )
            return resp.key is not None
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                return False
            raise e

    def rm_file(self, path: str) -> None:
        """Delete a file."""
        logger.warning("Call rm_file api: path %s", path)
        super().rm_file(path)

    def rmdir(self, path: str) -> None:
        """Remove a directory if it is empty.

        Parameters
        ----------
        path : str
            The path of the directory to remove. The path should be in the format
            `tos://bucket/path/to/directory`.

        Raises
        ------
        FileNotFoundError
            If the directory does not exist.
        NotADirectoryError
            If the path is not a directory.
        TosfsError
            If the directory is not empty,
             or the path is a bucket.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.rmdir("tos://mybucket/mydir/")

        """
        logger.warning("Call rmdir api: path %s", path)
        path = self._strip_protocol(path) + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            raise TosfsError("Cannot remove a bucket using rmdir api.")

        if not self.exists(path):
            raise FileNotFoundError(f"Directory {path} not found.")

        if not self.isdir(path):
            raise NotADirectoryError(f"{path} is not a directory.")

        if len(self._ls_objects(bucket, max_items=1, prefix=key.rstrip("/") + "/")) > 0:
            raise TosfsError(f"Directory {path} is not empty.")

        retryable_func_executor(
            lambda: self.tos_client.delete_object(bucket, key.rstrip("/") + "/"),
            max_retry_num=self.max_retry_num,
        )

    def rm(
            self, path: str, recursive: bool = False, maxdepth: Optional[int] = None
    ) -> None:
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.

        """
        logger.warning(
            "Call rm api: path %s, recursive %s, maxdepth %s", path, recursive, maxdepth
        )
        if isinstance(path, str):
            if not self.exists(path):
                raise FileNotFoundError(path)

            bucket, key, _ = self._split_path(path)
            if not key:
                raise TosfsError(f"Cannot remove a bucket {bucket} using rm api.")

            if not recursive or maxdepth:
                return super().rm(path, recursive=recursive, maxdepth=maxdepth)

            if self.isfile(path):
                self.rm_file(path)
            else:
                try:
                    self._list_and_batch_delete_objs(bucket, key)
                except (TosClientError, TosServerError) as e:
                    raise e
                except Exception as e:
                    raise TosfsError(f"Tosfs failed with unknown error: {e}") from e
        else:
            for single_path in path:
                self.rm(single_path, recursive=recursive, maxdepth=maxdepth)

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Create directory entry at path.

        For systems that don't have true directories, may create an object for
        this instance only and not touch the real filesystem

        Parameters
        ----------
        path: str
            location
        create_parents: bool
            if True, this is equivalent to ``makedirs``
        kwargs: Any
            may be permissions, etc.

        """
        path = self._strip_protocol(path) + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            raise TosfsError(f"Cannot create a bucket {bucket} using mkdir api.")

        if create_parents:
            parent = self._parent(f"{bucket}/{key}".rstrip("/") + "/")
            if not self.exists(parent):
                # here we need to create the parent directory recursively
                self.mkdir(parent, create_parents=True)

            retryable_func_executor(
                lambda: self.tos_client.put_object(bucket, key.rstrip("/") + "/"),
                max_retry_num=self.max_retry_num,
            )
        else:
            parent = self._parent(path)
            if not self.exists(parent):
                raise FileNotFoundError(f"Parent directory {parent} does not exist.")
            else:
                retryable_func_executor(
                    lambda: self.tos_client.put_object(bucket, key.rstrip("/") + "/"),
                    max_retry_num=self.max_retry_num,
                )

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Recursively make directories.

        Creates directory at path and any intervening required directories.
        Raises exception if, for instance, the path already exists but is a
        file.

        Parameters
        ----------
        path: str
            leaf directory name
        exist_ok: bool (False)
            If False, will error if the target already exists

        """
        path = self._strip_protocol(path) + "/"
        path_exist = self.exists(path)
        if exist_ok and path_exist:
            return
        if not exist_ok and path_exist:
            raise FileExistsError(path)

        self.mkdir(path, create_parents=True)

    def touch(self, path: str, truncate: bool = True, **kwargs: Any) -> None:
        """Create an empty file at the given path.

        Parameters
        ----------
        path : str
            The path of the file to create.
        truncate : bool, optional
            Whether to truncate the file if it already exists (default is True).
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileExistsError
            If the file already exists and `truncate` is False.
        TosfsError
            If there is an unknown error while creating the file.
        tos.exceptions.TosClientError
            If there is a client error while creating the file.
        tos.exceptions.TosServerError
            If there is a server error while creating the file.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.touch("tos://mybucket/myfile")

        """
        path = self._strip_protocol(path)
        bucket, key, _ = self._split_path(path)

        if not truncate and self.exists(path):
            raise FileExistsError(f"File {path} already exists.")

        retryable_func_executor(
            lambda: self.tos_client.put_object(bucket, key),
            max_retry_num=self.max_retry_num,
        )

    def isdir(self, path: str) -> bool:
        """Check if the path is a directory.

        Parameters
        ----------
        path : str
            The path to check.

        Returns
        -------
        bool
            True if the path is a directory, False otherwise.

        Raises
        ------
        TosClientError
            If there is a client error while accessing the path.
        TosServerError
            If there is a server error while accessing the path.
        TosfsError
            If there is an unknown error while accessing the path.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.isdir("tos://mybucket/mydir/")

        """
        path = self._strip_protocol(path) + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            return False

        try:
            if self._is_fns_bucket(bucket):
                resp = retryable_func_executor(
                    lambda: self.tos_client.get_file_status(bucket, key),
                    max_retry_num=self.max_retry_num,
                )
                return resp.key != key
            else:
                resp = retryable_func_executor(
                    lambda: self.tos_client.head_object(bucket, key),
                    max_retry_num=self.max_retry_num,
                )
                return resp.is_directory
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                return False
            raise e

    def isfile(self, path: str) -> bool:
        """Check if the path is a file.

        Parameters
        ----------
        path : str
            The path to check.

        Returns
        -------
        bool
            True if the path is a file, False otherwise.

        """
        if path.endswith("/"):
            return False

        bucket, key, _ = self._split_path(path)
        if not key:
            return False

        try:
            resp = retryable_func_executor(
                lambda: self.tos_client.head_object(bucket, key),
                max_retry_num=self.max_retry_num,
            )
            if self._is_fns_bucket(bucket):
                return True
            else:
                return not resp.is_directory
        except TosClientError as e:
            raise e
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                return False
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def put(
            self,
            lpath: str,
            rpath: str,
            recursive: bool = False,
            callback: Any = None,
            maxdepth: Optional[int] = None,
            disable_glob: bool = False,
            **kwargs: Any,
    ) -> None:
        """Copy file(s) from local.

        Copies a specific file or tree of files (if recursive=True). If rpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.

        Calls put_file for each source.
        """
        super().put(
            lpath, rpath, recursive=recursive, disable_glob=disable_glob, **kwargs
        )

    def put_file(
            self,
            lpath: str,
            rpath: str,
            chunksize: int = FILE_OPERATION_READ_WRITE_BUFFER_SIZE,
            **kwargs: Any,
    ) -> None:
        """Put a file from local to TOS.

        Parameters
        ----------
        lpath : str
            The local path of the file to put.
        rpath : str
            The remote path of the file to receive.
        chunksize : int, optional
            The size of the chunks to read from the file (default is 5 * 2**20).
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileNotFoundError
            If the local file does not exist.
        TosClientError
            If there is a client error while putting the file.
        TosServerError
            If there is a server error while putting the file.
        TosfsError
            If there is an unknown error while putting the file.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs.put_file("localfile.txt", "tos://mybucket/remote.txt")

        """
        if not os.path.exists(lpath):
            raise FileNotFoundError(f"Local file {lpath} not found.")

        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
            return

        size = os.path.getsize(lpath)

        content_type = None
        if "ContentType" not in kwargs:
            content_type, _ = mimetypes.guess_type(lpath)

        if self.isfile(rpath):
            self.makedirs(self._parent(rpath), exist_ok=True)

        if self.isdir(rpath):
            rpath = os.path.join(rpath, os.path.basename(lpath))
            self.mkdirs(self._parent(rpath), exist_ok=True)

        bucket, key, _ = self._split_path(rpath)

        with open(lpath, "rb") as f:
            if size < min(PUT_OBJECT_OPERATION_SMALL_FILE_THRESHOLD, 2 * chunksize):
                chunk = f.read()
                retryable_func_executor(
                    lambda: self.tos_client.put_object(
                        bucket,
                        key,
                        content=chunk,
                        content_type=content_type,
                    ),
                    max_retry_num=self.max_retry_num,
                )
            else:
                mpu = retryable_func_executor(
                    lambda: self.tos_client.create_multipart_upload(
                        bucket, key, content_type=content_type
                    ),
                    max_retry_num=self.max_retry_num,
                )
                retryable_func_executor(
                    lambda: self.tos_client.upload_part_from_file(
                        bucket, key, mpu.upload_id, file_path=lpath, part_number=1
                    ),
                    max_retry_num=self.max_retry_num,
                )
                retryable_func_executor(
                    lambda: self.tos_client.complete_multipart_upload(
                        bucket, key, mpu.upload_id, complete_all=True
                    ),
                    max_retry_num=self.max_retry_num,
                )

    def get(
            self,
            rpath: str,
            lpath: str,
            recursive: bool = False,
            callback: Any = None,
            maxdepth: Optional[int] = None,
            **kwargs: Any,
    ) -> None:
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.

        Calls get_file for each source.
        """
        if isinstance(lpath, list) and isinstance(rpath, list):
            # No need to expand paths when both source and destination
            # are provided as lists
            rpaths = rpath
            lpaths = lpath
        else:
            from fsspec.implementations.local import (
                LocalFileSystem,
                make_path_posix,
                trailing_sep,
            )

            # to make the exists check work
            if self.isdir(rpath):
                rpath = rpath.rstrip("/") + "/"

            source_is_str = isinstance(rpath, str)
            rpaths = self.expand_path(rpath, recursive=recursive, maxdepth=maxdepth)
            if source_is_str and (not recursive or maxdepth is not None):
                # Non-recursive glob does not copy directories
                rpaths = [p for p in rpaths if not (trailing_sep(p) or self.isdir(p))]
                if not rpaths:
                    return

            if isinstance(lpath, str):
                lpath = make_path_posix(lpath)

            source_is_file = len(rpaths) == 1
            dest_is_dir = isinstance(lpath, str) and (
                    trailing_sep(lpath) or LocalFileSystem().isdir(lpath)
            )

            exists = source_is_str and (
                    (has_magic(rpath) and source_is_file)
                    or (not has_magic(rpath) and dest_is_dir and not trailing_sep(rpath))
            )
            lpaths = other_paths(
                rpaths,
                lpath,
                exists=exists,
                flatten=not source_is_str,
            )

            for lpath, rpath in zip(lpaths, rpaths):
                self.get_file(rpath, lpath, callback=None, **kwargs)

    def get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        """Get a file from the TOS filesystem and write to a local path.

           This method will retry the download if there is error.

        Parameters
        ----------
        rpath : str
            The remote path of the file to get.
        lpath : str
            The local path to save the file.
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        tos.exceptions.TosClientError
            If there is a client error while getting the file.
        tos.exceptions.TosServerError
            If there is a server error while getting the file.
        TosfsError
            If there is an unknown error while getting the file.

        """
        if os.path.isdir(lpath):
            return

        if not self.exists(rpath):
            raise FileNotFoundError(rpath)

        if self.isdir(rpath):
            logger.debug("The remote path is a directory, skip downloading.")
            return

        bucket, key, version_id = self._split_path(rpath)

        def _read_chunks(body: BinaryIO, f: BinaryIO) -> None:
            bytes_read = 0
            while True:
                chunk = body.read(GET_OBJECT_OPERATION_DEFAULT_READ_CHUNK_SIZE)
                if not chunk:
                    break
                bytes_read += len(chunk)
                f.write(chunk)

        def download_file() -> None:
            body, content_length = self._open_remote_file(
                bucket, key, version_id, range_start=0, **kwargs
            )

            dir_path = os.path.dirname(lpath)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

            with open(lpath, "wb") as f:
                retryable_func_executor(_read_chunks, args=(body, f))

        retryable_func_executor(download_file)

    def walk(
            self,
            path: str,
            maxdepth: Optional[int] = None,
            topdown: bool = True,
            on_error: str = "omit",
            **kwargs: Any,
    ) -> Generator[str, List[str], List[str]]:
        """List objects under the given path.

        Parameters
        ----------
        path : str
            The path to list.
        maxdepth : int, optional
            The maximum depth to walk to (default is None).
        topdown : bool, optional
            Whether to walk top-down or bottom-up (default is True).
        on_error : str, optional
            How to handle errors (default is 'omit').
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        ValueError
            If the path is an invalid path.

        """
        if path in ["", "*"] + ["{}://".format(p) for p in self.protocol]:
            raise ValueError("Cannot access all of TOS via path {}.".format(path))

        return super().walk(
            path, maxdepth=maxdepth, topdown=topdown, on_error=on_error, **kwargs
        )

    def find(
            self,
            path: str,
            maxdepth: Optional[int] = None,
            withdirs: bool = False,
            detail: bool = False,
            prefix: str = "",
            **kwargs: Any,
    ) -> Union[List[str], dict]:
        """Find all files or dirs with conditions.

        Like posix ``find`` command without conditions

        Parameters
        ----------
        path : str
            The path to search.
        maxdepth: int, optional
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        prefix: str
            Only return files that match ``^{path}/{prefix}`` (if there is an
            exact match ``filename == {path}/{prefix}``, it also will be included)
        detail: bool
            If True, return a dict with file information, else just the path
        **kwargs: Any
            Additional arguments.

        """
        if path in ["", "*"] + ["{}://".format(p) for p in self.protocol]:
            raise ValueError("Cannot access all of TOS via path {}.".format(path))

        path = self._strip_protocol(path)
        bucket, key, _ = self._split_path(path)
        if not bucket:
            raise ValueError("Cannot access all of TOS without specify a bucket.")

        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        if maxdepth and prefix:
            raise ValueError(
                "Can not specify 'prefix' option alongside 'maxdepth' options."
            )
        if maxdepth:
            return super().find(
                bucket + "/" + key,
                maxdepth=maxdepth,
                withdirs=withdirs,
                detail=detail,
                **kwargs,
                )

        out = self._find_file_dir(key, path, prefix, withdirs, kwargs)

        if detail:
            return {o["name"]: o for o in out}
        else:
            return [o["name"] for o in out]

    def expand_path(
            self,
            path: Union[str, List[str]],
            recursive: bool = False,
            maxdepth: Optional[int] = None,
    ) -> List[str]:
        """Expand path to a list of files.

        Parameters
        ----------
        path : str
            The path to expand.
        recursive : bool, optional
            Whether to expand recursively (default is False).
        maxdepth : int, optional
            The maximum depth to expand to (default is None).
        **kwargs : Any, optional
            Additional arguments.

        Returns
        -------
        List[str]
            A list of expanded paths.

        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        if isinstance(path, str):
            return self.expand_path([path], recursive, maxdepth)

        out = set()
        path = [self._strip_protocol(p) for p in path]
        for p in path:  # can gather here
            if has_magic(p):
                bit = set(self.glob(p, maxdepth=maxdepth))
                out |= bit
                if recursive:
                    # glob call above expanded one depth so if maxdepth is defined
                    # then decrement it in expand_path call below. If it is zero
                    # after decrementing then avoid expand_path call.
                    if maxdepth is not None and maxdepth <= 1:
                        continue
                    out |= set(
                        self.expand_path(
                            list(bit),
                            recursive=recursive,
                            maxdepth=maxdepth - 1 if maxdepth is not None else None,
                        )
                    )
                continue
            elif recursive:
                rec = set(self.find(p, maxdepth=maxdepth, withdirs=True))
                out |= rec
            if p not in out and (recursive is False or self.exists(p)):
                # should only check once, for the root
                out.add(p)

        if not out:
            raise FileNotFoundError(path)
        return sorted(out)

    def cp_file(
            self,
            path1: str,
            path2: str,
            preserve_etag: Optional[bool] = None,
            managed_copy_threshold: Optional[int] = MANAGED_COPY_MAX_THRESHOLD,
            **kwargs: Any,
    ) -> None:
        """Copy file between locations on tos.

        Parameters
        ----------
        path1 : str
            The source path of the file to copy.
        path2 : str
            The destination path of the file to copy.
        preserve_etag : bool, optional
            Whether to preserve etag while copying. If the file is uploaded
            as a single part, then it will be always equivalent to the md5
            hash of the file hence etag will always be preserved. But if the
            file is uploaded in multi parts, then this option will try to
            reproduce the same multipart upload while copying and preserve
            the generated etag.
        managed_copy_threshold : int, optional
            The threshold size of the file to copy using managed copy. If the
            size of the file is greater than this threshold, then the file
            will be copied using managed copy (default is 5 * 2**30).
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileNotFoundError
            If the source file does not exist.
        ValueError
            If the destination is a versioned file.
        TosClientError
            If there is a client error while copying the file.
        TosServerError
            If there is a server error while copying the file.
        TosfsError
            If there is an unknown error while copying the file.

        """
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        if path1 == path2:
            logger.warning("Source and destination are the same: %s", path1)
            return

        if self.isdir(path1) and self.isdir(path2):
            return

        bucket, key, vers = self._split_path(path1)

        info = self.info(path1, bucket, key, version_id=vers)
        if not info:
            raise FileNotFoundError(f"Can not get information for path: {path1}")

        if info["type"] == "directory":
            logger.warning("Do not support copy directory %s.", path1)
            return

        size = info["size"]

        _, _, parts_suffix = info.get("ETag", "").strip('"').partition("-")
        if preserve_etag and parts_suffix:
            self._copy_etag_preserved(path1, path2, size, total_parts=int(parts_suffix))
        elif size <= min(
                MANAGED_COPY_MAX_THRESHOLD,
                (
                        managed_copy_threshold
                        if managed_copy_threshold
                        else MANAGED_COPY_MAX_THRESHOLD
                ),
        ):
            self._copy_basic(path1, path2, **kwargs)
        else:
            # if the preserve_etag is true, either the file is uploaded
            # on multiple parts or the size is lower than 5GB
            assert not preserve_etag

            # serial multipart copy
            self._copy_managed(path1, path2, size, **kwargs)

    def glob(
            self, path: str, maxdepth: Optional[int] = None, **kwargs: Any
    ) -> Collection[Any]:
        """Return list of paths matching a glob-like pattern.

        Parameters
        ----------
        path : str
            The path to search.
        maxdepth : int, optional
            The maximum depth to search to (default is None).
        **kwargs : Any, optional
            Additional arguments.

        """
        if path.startswith("*"):
            raise ValueError("Cannot traverse all of tosfs")

        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        import re

        seps = (os.path.sep, os.path.altsep) if os.path.altsep else (os.path.sep,)
        ends_with_sep = path.endswith(seps)  # _strip_protocol strips trailing slash
        path = self._strip_protocol(path)
        append_slash_to_dirname = ends_with_sep or path.endswith(
            tuple(sep + "**" for sep in seps)
        )

        idx_star = path.find("*") if path.find("*") >= 0 else len(path)
        idx_qmark = path.find("?") if path.find("?") >= 0 else len(path)
        idx_brace = path.find("[") if path.find("[") >= 0 else len(path)
        min_idx = min(idx_star, idx_qmark, idx_brace)

        detail = kwargs.pop("detail", False)

        if not has_magic(path):
            if self.exists(path, **kwargs):
                return {path: self.info(path, **kwargs)} if detail else [path]
            return {} if detail else []

        depth: Optional[int] = None
        root, depth = "", path[min_idx + 1 :].count("/") + 1
        if "/" in path[:min_idx]:
            min_idx = path[:min_idx].rindex("/")
            root = path[: min_idx + 1]

        if "**" in path:
            if maxdepth is not None:
                idx_double_stars = path.find("**")
                depth_double_stars = path[idx_double_stars:].count("/") + 1
                depth = depth - depth_double_stars + maxdepth
            else:
                depth = None

        allpaths = self.find(root, maxdepth=depth, withdirs=True, detail=True, **kwargs)
        pattern = re.compile(glob_translate(path + ("/" if ends_with_sep else "")))

        if isinstance(allpaths, dict):
            out = {
                p: info
                for p, info in sorted(allpaths.items())
                if pattern.match(
                    p + "/"
                    if append_slash_to_dirname and info["type"] == "directory"
                    else p
                )
            }
        else:
            out = {}

        return out if detail else list(out)

    def _rm(self, path: str) -> None:
        logger.info("Removing path: %s", path)
        bucket, key, _ = self._split_path(path)

        if path.endswith("/") or self.isdir(path):
            key = key.rstrip("/") + "/"

        try:
            retryable_func_executor(
                lambda: self.tos_client.delete_object(bucket, key),
                max_retry_num=self.max_retry_num,
            )
        except (TosClientError, TosServerError) as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    ########################  private methods  ########################

    def _list_and_batch_delete_objs(self, bucket: str, key: str) -> None:
        bucket_type = self._get_bucket_type(bucket)
        is_truncated = True
        continuation_token = ""
        all_results = []

        if bucket_type == TOS_BUCKET_TYPE_FNS:

            def _call_list_objects(
                    continuation_token: str = "",
            ) -> ListObjectType2Output:
                return retryable_func_executor(
                    lambda: self.tos_client.list_objects_type2(
                        bucket,
                        prefix=key.rstrip("/") + "/",
                        max_keys=LS_OPERATION_DEFAULT_MAX_ITEMS,
                        continuation_token=continuation_token,
                    ),
                    max_retry_num=self.max_retry_num,
                )

            while is_truncated:
                resp = _call_list_objects(continuation_token)
                is_truncated = resp.is_truncated
                continuation_token = resp.next_continuation_token
                all_results = resp.contents

                deleting_objects = [
                    DeletingObject(o.key if hasattr(o, "key") else o.prefix)
                    for o in all_results
                ]

                if deleting_objects:
                    self._delete_objects(bucket, deleting_objects)
        elif bucket_type == TOS_BUCKET_TYPE_HNS:
            all_results = self._list_and_collect_objects(
                bucket, bucket_type, key.rstrip("/") + "/"
            )
            if all_results:
                self._delete_objects(bucket, all_results)
        else:
            raise ValueError(f"Unsupported bucket type: {bucket_type}")

    def _delete_objects(
            self, bucket: str, deleting_objects: list[DeletingObject]
    ) -> None:
        bucket_type = self._get_bucket_type(bucket)
        if bucket_type == TOS_BUCKET_TYPE_FNS:
            delete_resp = retryable_func_executor(
                lambda: self.tos_client.delete_multi_objects(
                    bucket, deleting_objects, quiet=True
                ),
                max_retry_num=self.max_retry_num,
            )
            if delete_resp.error:
                for d in delete_resp.error:
                    logger.warning("Deleted object: %s failed", d)
        else:

            def _call_delete_object(obj: DeletingObject) -> None:
                retryable_func_executor(
                    lambda: self.tos_client.delete_object(bucket, obj.key),
                    max_retry_num=self.max_retry_num,
                )

            # Preferentially delete subpaths with longer keys
            for obj in sorted(deleting_objects, key=lambda x: len(x.key), reverse=True):
                _call_delete_object(obj)

    def _list_and_collect_objects(
            self,
            bucket: str,
            bucket_type: str,
            prefix: str,
            collected_objects: Optional[List[DeletingObject]] = None,
    ) -> List[DeletingObject]:

        if collected_objects is None:
            collected_objects = []

        collected_keys = {obj.key for obj in collected_objects}

        is_truncated = True
        continuation_token = ""

        while is_truncated:

            def _call_list_objects_type2(
                    continuation_token: str = continuation_token, prefix: str = prefix
            ) -> ListObjectType2Output:
                return self.tos_client.list_objects_type2(
                    bucket,
                    prefix=prefix,
                    max_keys=LS_OPERATION_DEFAULT_MAX_ITEMS,
                    continuation_token=continuation_token,
                    delimiter="/" if bucket_type == TOS_BUCKET_TYPE_HNS else None,
                )

            resp = retryable_func_executor(
                _call_list_objects_type2,
                max_retry_num=self.max_retry_num,
            )
            is_truncated = resp.is_truncated
            continuation_token = resp.next_continuation_token

            for obj in resp.contents:
                key = obj.key if hasattr(obj, "key") else obj.prefix
                if key not in collected_keys:
                    collected_objects.append(DeletingObject(key=key))
                    collected_keys.add(key)

            for common_prefix in resp.common_prefixes:
                key = common_prefix.prefix
                if key not in collected_keys:
                    collected_objects.append(DeletingObject(key=key))
                    collected_keys.add(key)
                if bucket_type == TOS_BUCKET_TYPE_HNS:
                    self._list_and_collect_objects(
                        bucket, bucket_type, common_prefix.prefix, collected_objects
                    )

        return collected_objects

    def _copy_basic(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy file between locations on tos.

        Not allowed where the origin is larger than 5GB.
        """
        buc1, key1, ver1 = self._split_path(path1)
        buc2, key2, ver2 = self._split_path(path2)
        if ver2:
            raise ValueError("Cannot copy to a versioned file!")

        retryable_func_executor(
            lambda: self.tos_client.copy_object(
                bucket=buc2,
                key=key2,
                src_bucket=buc1,
                src_key=key1,
                src_version_id=ver1,
            ),
            max_retry_num=self.max_retry_num,
        )

    def _copy_etag_preserved(
            self, path1: str, path2: str, size: int, total_parts: int, **kwargs: Any
    ) -> None:
        """Copy file as multiple-part while preserving the etag."""
        bucket1, key1, version1 = self._split_path(path1)
        bucket2, key2, version2 = self._split_path(path2)

        upload_id = None

        try:
            mpu = retryable_func_executor(
                lambda: self.tos_client.create_multipart_upload(bucket2, key2),
                max_retry_num=self.max_retry_num,
            )
            upload_id = mpu.upload_id

            parts = []
            brange_first = 0

            for i in range(1, total_parts + 1):
                part_size = min(size - brange_first, PART_MAX_SIZE)
                brange_last = brange_first + part_size - 1
                if brange_last > size:
                    brange_last = size - 1

                def _call_upload_part_copy(
                        i: int = i,
                        brange_first: int = brange_first,
                        brange_last: int = brange_last,
                ) -> UploadPartCopyOutput:
                    return self.tos_client.upload_part_copy(
                        bucket=bucket2,
                        key=key2,
                        part_number=i,
                        upload_id=upload_id,
                        src_bucket=bucket1,
                        src_key=key1,
                        copy_source_range_start=brange_first,
                        copy_source_range_end=brange_last,
                    )

                part = retryable_func_executor(
                    _call_upload_part_copy,
                    args=(i, brange_first, brange_last),
                    max_retry_num=self.max_retry_num,
                )
                parts.append(
                    PartInfo(
                        part_number=part.part_number,
                        etag=part.etag,
                        part_size=size,
                        offset=None,
                        hash_crc64_ecma=None,
                        is_completed=None,
                    )
                )
                brange_first += part_size

            retryable_func_executor(
                lambda: self.tos_client.complete_multipart_upload(
                    bucket2, key2, upload_id, parts
                ),
                max_retry_num=self.max_retry_num,
            )
        except Exception as e:
            retryable_func_executor(
                lambda: self.tos_client.abort_multipart_upload(
                    bucket2, key2, upload_id
                ),
                max_retry_num=self.max_retry_num,
            )
            raise TosfsError(f"Copy failed ({path1} -> {path2}): {e}") from e

    def _copy_managed(
            self,
            path1: str,
            path2: str,
            size: int,
            block: int = MANAGED_COPY_MAX_THRESHOLD,
            **kwargs: Any,
    ) -> None:
        """Copy file between locations on tos as multiple-part.

        block: int
            The size of the pieces, must be larger than 5MB and at
            most MANAGED_COPY_MAX_THRESHOLD.
            Smaller blocks mean more calls, only useful for testing.
        """
        if block < MANAGED_COPY_MIN_THRESHOLD or block > MANAGED_COPY_MAX_THRESHOLD:
            raise ValueError("Copy block size must be 5MB<=block<=5GB")

        bucket1, key1, version1 = self._split_path(path1)
        bucket2, key2, version2 = self._split_path(path2)

        upload_id = None

        try:
            mpu = retryable_func_executor(
                lambda: self.tos_client.create_multipart_upload(bucket2, key2),
                max_retry_num=self.max_retry_num,
            )
            upload_id = mpu.upload_id

            def _call_upload_part_copy(
                    i: int, brange_first: int, brange_last: int
            ) -> UploadPartCopyOutput:
                return self.tos_client.upload_part_copy(
                    bucket=bucket2,
                    key=key2,
                    part_number=i + 1,
                    upload_id=upload_id,
                    src_bucket=bucket1,
                    src_key=key1,
                    copy_source_range_start=brange_first,
                    copy_source_range_end=brange_last,
                )

            out = [
                retryable_func_executor(
                    _call_upload_part_copy,
                    args=(i, brange_first, brange_last),
                    max_retry_num=self.max_retry_num,
                )
                for i, (brange_first, brange_last) in enumerate(get_brange(size, block))
            ]

            parts = [
                PartInfo(
                    part_number=i + 1,
                    etag=o.etag,
                    part_size=size,
                    offset=None,
                    hash_crc64_ecma=None,
                    is_completed=None,
                )
                for i, o in enumerate(out)
            ]

            retryable_func_executor(
                lambda: self.tos_client.complete_multipart_upload(
                    bucket2, key2, upload_id, parts
                ),
                max_retry_num=self.max_retry_num,
            )
        except Exception as e:
            retryable_func_executor(
                lambda: self.tos_client.abort_multipart_upload(
                    bucket2, key2, upload_id
                ),
                max_retry_num=self.max_retry_num,
            )
            raise TosfsError(f"Copy failed ({path1} -> {path2}): {e}") from e

    def _find_file_dir(
            self, key: str, path: str, prefix: str, withdirs: bool, kwargs: Any
    ) -> List[dict]:
        out = self._ls_dirs_and_files(
            path,
            delimiter="",
            include_self=True,
            prefix=prefix,
            recursive=True,
        )
        if not out and key:
            try:
                out = [self.info(path)]
            except FileNotFoundError:
                out = []

        dirs = {
            self._parent(o["name"]): {
                "Key": self._parent(o["name"]).rstrip("/"),
                "Size": 0,
                "size": 0,
                "name": self._parent(o["name"]).rstrip("/"),
                "type": "directory",
            }
            for o in out
            if len(path) <= len(self._parent(o["name"]))
        }

        if withdirs:
            for dir_info in dirs.values():
                if dir_info not in out:
                    out.append(dir_info)
        else:
            out = [o for o in out if o["type"] == "file"]

        return sorted(out, key=lambda x: x["name"])

    def _open_remote_file(
            self,
            bucket: str,
            key: str,
            version_id: Optional[str],
            range_start: int,
            **kwargs: Any,
    ) -> Tuple[BinaryIO, int]:
        if "callback" in kwargs:
            kwargs.pop("callback")
        try:
            resp = retryable_func_executor(
                lambda: self.tos_client.get_object(
                    bucket,
                    key,
                    version_id=version_id,
                    range_start=range_start,
                    **kwargs,
                ),
                max_retry_num=self.max_retry_num,
            )
        except TosServerError as e:
            if e.status_code == INVALID_RANGE_CODE:
                obj_info = self._object_info(bucket=bucket, key=key)
                if obj_info and (
                        obj_info["size"] == 0 or range_start == obj_info["size"]
                ):
                    return io.BytesIO(), 0
            else:
                raise e

        return resp.content, resp.content_length

    def _bucket_info(self, bucket: str) -> dict:
        """Get the information of a bucket.

        Parameters
        ----------
        bucket : str
            The name of the bucket.

        Returns
        -------
        dict
            A dictionary containing the bucket information with the following keys:
            - 'Key': The bucket name.
            - 'Size': The size of the bucket (always 0).
            - 'StorageClass': The storage class of the bucket (always 'BUCKET').
            - 'size': The size of the bucket (always 0).
            - 'type': The type of the bucket (always 'directory').
            - 'name': The bucket name.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while accessing the bucket.
        tos.exceptions.TosServerError
            If there is a server error while accessing the bucket.
        FileNotFoundError
            If the bucket does not exist.
        TosfsError
            If there is an unknown error while accessing the bucket.

        """
        try:
            retryable_func_executor(
                lambda: self.tos_client.head_bucket(bucket),
                max_retry_num=self.max_retry_num,
            )
            return self._fill_bucket_info(bucket)
        except TosClientError as e:
            raise e
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                raise FileNotFoundError(bucket) from e
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _object_info(
            self, bucket: str, key: str, version_id: Optional[str] = None
    ) -> Optional[dict]:
        """Get the information of an object.

        Parameters
        ----------
        bucket : str
            The bucket name.
        key : str
            The object key.
        version_id : str, optional
            The version id of the object (default is None).

        Returns
        -------
        dict
            A dictionary containing the object information with the following keys:
            - 'ETag': The entity tag of the object.
            - 'LastModified': The last modified date of the object.
            - 'size': The size of the object in bytes.
            - 'name': The full path of the object.
            - 'type': The type of the object (always 'file').
            - 'StorageClass': The storage class of the object.
            - 'VersionId': The version id of the object.
            - 'ContentType': The content type of the object.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while accessing the object.
        tos.exceptions.TosServerError
            If there is a server error while accessing the object.
        TosfsError
            If there is an unknown error while accessing the object.

        """
        try:
            out = retryable_func_executor(
                lambda: self.tos_client.head_object(bucket, key, version_id=version_id),
                max_retry_num=self.max_retry_num,
            )
            return {
                "ETag": out.etag or "",
                "LastModified": out.last_modified or "",
                "size": out.content_length or 0,
                "name": "/".join((bucket, key)),
                "type": "file",
                "StorageClass": out.storage_class or "STANDARD",
                "VersionId": out.version_id or "",
                "ContentType": out.content_type or "",
            }
        except TosClientError as e:
            raise e
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND or (
                    self._get_bucket_type(bucket) == TOS_BUCKET_TYPE_HNS
                    and e.status_code == CONFLICT_CODE
                    and e.header._store["x-tos-ec"][1] == "0026-00000020"
            ):
                pass
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

        return None

    def _get_dir_info(self, bucket: str, key: str, fullpath: str) -> Optional[dict]:
        try:
            # We check to see if the path is a directory by attempting to list its
            # contexts. If anything is found, it is indeed a directory
            out = retryable_func_executor(
                lambda: self.tos_client.list_objects_type2(
                    bucket,
                    prefix=key.rstrip("/") + "/" if key else "",
                    delimiter="/",
                    max_keys=1,
                ),
                max_retry_num=self.max_retry_num,
            )

            if out.key_count > 0 or out.contents or out.common_prefixes:
                return {
                    "name": fullpath,
                    "Key": fullpath,
                    "Size": 0,
                    "size": 0,
                    "type": "directory",
                }

            return None
        except (TosClientError, TosServerError) as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _exists_bucket(self, bucket: str) -> bool:
        """Check if a bucket exists in the TOS.

        Parameters
        ----------
        bucket : str
            The name of the bucket to check for existence.

        Returns
        -------
        bool
            True if the bucket exists, False otherwise.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while checking the bucket.
        tos.exceptions.TosServerError
            If there is a server error while checking the bucket.
        TosfsError
            If there is an unknown error while checking the bucket.

        Examples
        --------
        >>> fs = TosFileSystem()
        >>> fs._exists_bucket("mybucket")
        True
        >>> fs._exists_bucket("nonexistentbucket")
        False

        """
        try:
            retryable_func_executor(
                lambda: self.tos_client.head_bucket(bucket),
                max_retry_num=self.max_retry_num,
            )
            return True
        except TosClientError as e:
            raise e
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                return False
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

    def _ls_buckets(self) -> List[dict]:
        """List all buckets in the account.

        Returns
        -------
        List[dict]
            A list of dictionaries,
            each containing information about a bucket with the following keys:
            - 'Key': The bucket name.
            - 'Size': The size of the bucket (always 0).
            - 'StorageClass': The storage class of the bucket (always 'BUCKET').
            - 'size': The size of the bucket (always 0).
            - 'type': The type of the bucket (always 'directory').
            - 'name': The bucket name.

        Raises
        ------
        tos.exceptions.TosClientError
            If there is a client error while listing the buckets.
        tos.exceptions.TosServerError
            If there is a server error while listing the buckets.
        TosfsError
            If there is an unknown error while listing the buckets.

        """
        try:
            resp = retryable_func_executor(
                lambda: self.tos_client.list_buckets(), max_retry_num=self.max_retry_num
            )
        except (TosClientError, TosServerError) as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

        return [self._fill_bucket_info(bucket.name) for bucket in resp.buckets]

    def _ls_dirs_and_files(
            self,
            path: str,
            max_items: int = LS_OPERATION_DEFAULT_MAX_ITEMS,
            delimiter: str = "/",
            prefix: str = "",
            include_self: bool = False,
            versions: bool = False,
            recursive: bool = False,
    ) -> List[dict]:
        bucket, key, _ = self._split_path(path)
        if not prefix:
            prefix = ""
        if key:
            prefix = key.lstrip("/") + "/" + prefix

        logger.debug("Get directory listing for %s", path)
        dirs = []
        files = []
        seen_names = set()

        for obj in self._ls_objects(
                bucket,
                max_items=max_items,
                delimiter=delimiter,
                prefix=prefix,
                include_self=include_self,
                versions=versions,
                recursive=recursive,
        ):
            if isinstance(obj, CommonPrefixInfo):
                dir_info = self._fill_dir_info(bucket, obj)
                dir_name = dir_info["name"]
                if dir_name not in seen_names:
                    dirs.append(dir_info)
                    seen_names.add(dir_name)
            elif obj.key.endswith("/"):
                dir_info = self._fill_dir_info(bucket, None, obj.key)
                dir_name = dir_info["name"]
                if dir_name not in seen_names:
                    dirs.append(dir_info)
                    seen_names.add(dir_name)
            else:
                file_info = self._fill_file_info(obj, bucket, versions)
                file_name = file_info["name"]
                if file_name not in seen_names:
                    files.append(file_info)
                    seen_names.add(file_name)
        files += dirs

        return files

    def _ls_objects(
            self,
            bucket: str,
            max_items: int = LS_OPERATION_DEFAULT_MAX_ITEMS,
            delimiter: str = "/",
            prefix: str = "",
            include_self: bool = False,
            versions: bool = False,
            recursive: bool = False,
    ) -> List[Union[CommonPrefixInfo, ListedObject, ListedObjectVersion]]:
        if versions:
            raise ValueError(
                "versions cannot be specified if the filesystem is "
                "not version aware."
            )

        bucket_type = self._get_bucket_type(bucket)
        all_results = []

        if recursive and bucket_type == TOS_BUCKET_TYPE_HNS:

            def _recursive_list(bucket: str, prefix: str) -> None:
                resp = retryable_func_executor(
                    lambda: self.tos_client.list_objects_type2(
                        bucket,
                        prefix=prefix,
                        delimiter="/",
                        max_keys=max_items,
                    ),
                    max_retry_num=self.max_retry_num,
                )

                all_results.extend(resp.contents + resp.common_prefixes)
                for common_prefix in resp.common_prefixes:
                    _recursive_list(bucket, common_prefix.prefix)

            _recursive_list(bucket, prefix)
        else:
            is_truncated = True

            continuation_token = ""
            while is_truncated:

                def _call_list_objects_type2(
                        continuation_token: str = continuation_token,
                ) -> ListObjectType2Output:
                    return self.tos_client.list_objects_type2(
                        bucket,
                        prefix,
                        start_after=prefix if not include_self else None,
                        delimiter=delimiter,
                        max_keys=max_items,
                        continuation_token=continuation_token,
                    )

                resp = retryable_func_executor(
                    _call_list_objects_type2,
                    args=(continuation_token,),
                    max_retry_num=self.max_retry_num,
                )
                is_truncated = resp.is_truncated
                continuation_token = resp.next_continuation_token

                all_results.extend(resp.contents + resp.common_prefixes)

        return all_results

    def _prefix_search_for_exists(self, bucket: str, key: str) -> bool:
        bucket_type = self._get_bucket_type(bucket)

        if bucket_type == TOS_BUCKET_TYPE_FNS:
            resp = retryable_func_executor(
                lambda: self.tos_client.list_objects_type2(
                    bucket,
                    key.rstrip("/") + "/",
                    start_after=key.rstrip("/") + "/",
                    max_keys=1,
                    ),
                max_retry_num=self.max_retry_num,
            )
            return len(resp.contents) > 0
        elif bucket_type == TOS_BUCKET_TYPE_HNS:

            def search_in_common_prefixes(bucket: str, prefix: str) -> bool:
                resp = retryable_func_executor(
                    lambda: self.tos_client.list_objects_type2(
                        bucket,
                        prefix,
                        delimiter="/",
                        max_keys=1,
                    ),
                    max_retry_num=self.max_retry_num,
                )
                if len(resp.contents) > 0:
                    return True
                for common_prefix in resp.common_prefixes:
                    if search_in_common_prefixes(bucket, common_prefix):
                        return True
                return False

            resp = retryable_func_executor(
                lambda: self.tos_client.list_objects_type2(
                    bucket,
                    key.rstrip("/") + "/",
                    delimiter="/",
                    max_keys=1,
                    ),
                max_retry_num=self.max_retry_num,
            )
            if len(resp.contents) > 0:
                return True
            return search_in_common_prefixes(bucket, key.rstrip("/") + "/")
        else:
            raise ValueError(f"Unsupported bucket type {bucket_type}")

    def _split_path(self, path: str) -> Tuple[str, str, Optional[str]]:
        """Normalise tos path string into bucket and key.

        Parameters
        ----------
        path : string
            Input path, like `tos://mybucket/path/to/file`

        Examples
        --------
        >>> self._split_path("tos://mybucket/path/to/file")
        ['mybucket', 'path/to/file', None]
        # pylint: disable=line-too-long
        >>> self._split_path("tos://mybucket/path/to/versioned_file?versionId=some_version_id")
        ['mybucket', 'path/to/versioned_file', 'some_version_id']

        """
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, "", None

        bucket, keypart = find_bucket_key(path)
        key, _, version_id = keypart.partition("?versionId=")

        if self.tag_enabled:
            self.bucket_tag_mgr.add_bucket_tag(bucket)

        return (
            bucket,
            key,
            version_id if self.version_aware and version_id else None,
        )

    def _get_bucket_type(self, bucket: str) -> str:
        bucket_type = retryable_func_executor(
            lambda: self.tos_client._get_bucket_type(bucket),
            max_retry_num=self.max_retry_num,
        )
        if not bucket_type:
            return TOS_BUCKET_TYPE_FNS

        return bucket_type

    def _is_hns_bucket(self, bucket: str) -> bool:
        return self._get_bucket_type(bucket) == TOS_BUCKET_TYPE_HNS

    def _is_fns_bucket(self, bucket: str) -> bool:
        return self._get_bucket_type(bucket) == TOS_BUCKET_TYPE_FNS

    def _init_tag_manager(self) -> None:
        auth = self.tos_client.auth
        if isinstance(auth, CredentialProviderAuth):
            credentials = auth.credentials_provider.get_credentials()
            self.bucket_tag_mgr = BucketTagMgr(
                credentials.get_ak(),
                credentials.get_sk(),
                credentials.get_security_token(),
                auth.region,
            )
        else:
            raise TosfsError(
                "Currently only support CredentialProviderAuth type, "
                "please check if you set (ak & sk) or (session token) "
                "correctly."
            )

    @staticmethod
    def _fill_dir_info(
            bucket: str, common_prefix: Optional[CommonPrefixInfo], key: str = ""
    ) -> dict:
        name = "/".join(
            [bucket, common_prefix.prefix[:-1] if common_prefix else key]
        ).rstrip("/")
        return {
            "name": name,
            "Key": name,
            "Size": 0,
            "size": 0,
            "type": "directory",
        }

    @staticmethod
    def _fill_file_info(obj: ListedObject, bucket: str, versions: bool = False) -> dict:
        result = {
            "Key": f"{bucket}/{obj.key}",
            "size": obj.size,
            "name": f"{bucket}/{obj.key}",
            "type": "file",
            "LastModified": obj.last_modified,
        }
        if (
                isinstance(obj, ListedObjectVersion)
                and versions
                and obj.version_id
                and obj.version_id != "null"
        ):
            result["name"] += f"?versionId={obj.version_id}"
        return result

    @staticmethod
    def _fill_bucket_info(bucket_name: str) -> dict:
        return {
            "Key": bucket_name,
            "Size": 0,
            "StorageClass": "BUCKET",
            "size": 0,
            "type": "directory",
            "name": bucket_name,
        }


class TosFile(AbstractBufferedFile):
    """File-like operations for TOS."""

    def __init__(
            self,
            fs: TosFileSystem,
            path: str,
            mode: str = "rb",
            block_size: Union[int, str] = "default",
            autocommit: bool = True,
            cache_type: str = "readahead",
            **kwargs: Any,
    ):
        """Instantiate a TOS file."""
        bucket, key, path_version_id = fs._split_path(path)

        super().__init__(
            fs,
            path,
            mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            **kwargs,
        )
        self.fs = fs
        self.bucket = bucket
        self.key = key
        self.version_id = path_version_id
        self.path = path
        self.mode = mode
        self.autocommit = autocommit
        self.append_block = False
        self.append_offset = 0
        self.buffer: Optional[io.BytesIO] = io.BytesIO()
        self.parts = []
        self.upload_id = None
        self.part_number = 1

        self._check_init_params(key, path, mode, block_size)

        if "a" in mode:
            self.append_block = True
            try:
                head = retryable_func_executor(
                    lambda: self.fs.tos_client.head_object(bucket, key),
                    max_retry_num=self.fs.max_retry_num,
                )
                self.append_offset = head.content_length
            except TosServerError as e:
                if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                    pass
                else:
                    raise e


    def _check_init_params(
            self, key: str, path: str, mode: str, block_size: Union[int, str]
    ) -> None:
        if not key:
            raise ValueError("Attempt to open non key-like path: %s" % path)

        if "r" not in mode and int(block_size) < MPU_PART_SIZE_THRESHOLD:
            raise ValueError(
                f"Block size must be >= {MPU_PART_SIZE_THRESHOLD // (2**20)}MB."
            )

    def _initiate_upload(self) -> None:
        """Create remote file/upload."""
        if self.autocommit and self.tell() < self.blocksize:
            # only happens when closing small file, use on-shot PUT
            return
        else:
            logger.debug("Initiate upload for %s", self)
            self.upload_id = retryable_func_executor(
                lambda: self.fs.tos_client.create_multipart_upload(
                    self.bucket, self.key
                ).upload_id,
                max_retry_num=self.fs.max_retry_num,
            )

    def _upload_chunk(self, final: bool = False) -> bool:
        """Write one part of a multi-block file upload.

        Parameters
        ----------
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.

        """
        bucket, key, _ = self.fs._split_path(self.path)
        if self.buffer:
            logger.debug(
                "Upload for %s, final=%s, loc=%s, buffer loc=%s",
                self,
                final,
                self.loc,
                self.buffer.tell(),
            )

        if self.append_block:
            self._append_chunk()
        else:
            if (
                    self.autocommit
                    and final
                    and self.tell()
                    < min(self.blocksize, self.fs.multipart_threshold)
            ):
                # only happens when closing small file, use one-shot PUT
                pass
            else:
                self.buffer.seek(0)
                content = self.buffer.read()
                part = retryable_func_executor(
                    lambda: self.fs.tos_client.upload_part(
                        self.bucket,
                        self.key,
                        self.upload_id,
                        self.part_number,
                        content=content,
                    ),
                    max_retry_num=self.fs.max_retry_num,
                )
                self.parts.append(
                    PartInfo(
                        part_number=self.part_number,
                        etag=part.etag,
                        part_size=len(content),
                        offset=None,
                        hash_crc64_ecma=None,
                        is_completed=None,
                    )
                )
                self.part_number += 1
                self.buffer = io.BytesIO()

            if self.autocommit and final:
                self.commit()

        return not final

    def _append_chunk(self) -> None:
        """Append or create to a file."""
        if self.buffer:
            self.buffer.seek(0)
            content = self.buffer.read()
            if content:
                resp = retryable_func_executor(
                    lambda: self.fs.tos_client.append_object(
                        self.bucket,
                        self.key,
                        offset=self.append_offset,
                        content=content,
                    ),
                )
                self.append_offset = resp.next_append_offset

    def _fetch_range(self, start: int, end: int) -> bytes:
        if start == end:
            logger.warning(
                "skip fetch for negative range - bucket=%s,key=%s,start=%d,end=%d",
                self.bucket,
                self.key,
                start,
                end,
            )
            return b""
        logger.debug("Fetch: %s/%s, %s-%s", self.bucket, self.key, start, end)

        def fetch() -> bytes:
            with io.BytesIO() as temp_buffer:
                for chunk in self.fs.tos_client.get_object(
                        self.bucket,
                        self.key,
                        self.version_id,
                        range_start=start,
                        range_end=end,
                ):
                    temp_buffer.write(chunk)
                return temp_buffer.getvalue()

        return retryable_func_executor(fetch, max_retry_num=self.fs.max_retry_num)

    def commit(self) -> None:
        """Complete multipart upload or PUT."""
        logger.debug("Commit %s", self)
        if self.tell() == 0:
            if self.buffer is not None:
                logger.debug("Empty file committed %s", self)
                retryable_func_executor(
                    lambda: self.fs.tos_client.abort_multipart_upload(
                        self.bucket, self.key, self.upload_id
                    ),
                    max_retry_num=self.fs.max_retry_num,
                )
                self.fs.touch(self.path, **self.kwargs)
        elif self.upload_id is None:
            logger.debug("One-shot upload of %s", self)
            self.buffer.seek(0)
            data = self.buffer.read()
            retryable_func_executor(
                lambda: self.fs.tos_client.put_object(
                    self.bucket, self.key, content=data
                ),
                max_retry_num=self.fs.max_retry_num,
            )
        else:
            logger.debug("Complete multi-part upload for %s ", self)
            retryable_func_executor(
                lambda: self.fs.tos_client.complete_multipart_upload(
                    self.bucket,
                    self.key,
                    upload_id=self.upload_id,
                    parts=self.parts,
                ),
                max_retry_num=self.fs.max_retry_num,
            )

        self.buffer = None

    def discard(self) -> None:
        """Close the file without writing."""
        if self.upload_id:
            retryable_func_executor(
                lambda: self.fs.tos_client.abort_multipart_upload(
                    self.bucket, self.key, self.upload_id
                ),
                max_retry_num=self.fs.max_retry_num,
            )
        self.buffer = None
