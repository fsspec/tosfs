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
import time
from glob import has_magic
from typing import Any, BinaryIO, Collection, Generator, List, Optional, Tuple, Union

import tos
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import setup_logging as setup_logger
from tos.exceptions import TosClientError, TosServerError
from tos.models import CommonPrefixInfo
from tos.models2 import (
    ListedObject,
    ListedObjectVersion,
    ListObjectType2Output,
    ListObjectVersionsOutput,
    PartInfo,
    UploadPartCopyOutput,
)

from tosfs.consts import (
    APPEND_OPERATION_SMALL_FILE_THRESHOLD,
    ENV_NAME_TOSFS_LOGGING_LEVEL,
    FILE_OPERATION_READ_WRITE_BUFFER_SIZE,
    GET_OBJECT_OPERATION_DEFAULT_READ_CHUNK_SIZE,
    LS_OPERATION_DEFAULT_MAX_ITEMS,
    MANAGED_COPY_MAX_THRESHOLD,
    MANAGED_COPY_MIN_THRESHOLD,
    MPU_PART_SIZE_THRESHOLD,
    PART_MAX_SIZE,
    PUT_OBJECT_OPERATION_SMALL_FILE_THRESHOLD,
    RETRY_NUM,
    TOS_SERVER_STATUS_CODE_NOT_FOUND,
    TOSFS_LOG_FORMAT,
)
from tosfs.exceptions import TosfsError
from tosfs.fsspec_utils import glob_translate
from tosfs.mpu import MultipartUploader
from tosfs.retry import retryable_func_executor
from tosfs.utils import find_bucket_key, get_brange

logger = logging.getLogger("tosfs")


def setup_logging() -> None:
    """Set up the logging configuration for TOSFS."""
    setup_logger(
        logger=logger,
        level=os.environ.get(ENV_NAME_TOSFS_LOGGING_LEVEL, "INFO"),
    )

    formatter = logging.Formatter(TOSFS_LOG_FORMAT)
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    # set and config tos client's logger
    tos.set_logger(
        name="tosclient",
        level=os.environ.get(ENV_NAME_TOSFS_LOGGING_LEVEL, "INFO"),
        log_handler=logging.StreamHandler(),
        format_string=TOSFS_LOG_FORMAT,
    )


setup_logging()

logger.warning(
    "The tosfs's log level is set to be %s", logging.getLevelName(logger.level)
)


class TosFileSystem(AbstractFileSystem):
    """Tos file system.

    It's an implementation of AbstractFileSystem which is an
    abstract super-class for pythonic file-systems.
    """

    protocol = ("tos", "tosfs")

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        key: str = "",
        secret: str = "",
        region: Optional[str] = None,
        max_retry_num: int = 20,
        max_connections: int = 1024,
        connection_time: int = 10,
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
        multipart_staging_buffer_size: int = 4 << 10,
        multipart_threshold: int = 10 << 20,
        **kwargs: Any,
    ) -> None:
        """Initialise the TosFileSystem.

        Parameters
        ----------
        endpoint_url : str, optional
            The endpoint URL of the TOS service.
        key : str
            The access key ID(ak) to access the TOS service.
        secret : str
            The secret access key(sk) to access the TOS service.
        region : str, optional
            The region of the TOS service.
        max_retry_num : int, optional
            The maximum number of retries for a failed request (default is 20).
        max_connections : int, optional
            The maximum number of HTTP connections that can be opened in the
            connection pool (default is 1024).
        connection_time : int, optional
            The time to keep a connection open (default is 10).
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
        multipart_staging_buffer_size : int, optional
            The max byte size which will buffer the staging data in-memory before
            flushing to the staging file. It will decrease the random write in local
            staging disk dramatically if writing plenty of small files.
            (default is 4096).
        multipart_threshold : int, optional
            The threshold which control whether enable multipart upload during
            writing data to the given object storage, if the write data size is less
            than threshold, will write data via simple put instead of multipart upload.
             (default is 10 MB).
        kwargs : Any, optional
            Additional arguments.

        """
        self.tos_client = tos.TosClientV2(
            key,
            secret,
            endpoint_url,
            region,
            max_retry_count=0,
            max_connections=max_connections,
            connection_time=connection_time,
            socket_timeout=socket_timeout,
            high_latency_log_threshold=high_latency_log_threshold,
            credentials_provider=credentials_provider,
            enable_crc=False,
            enable_verify_ssl=False,
            disable_encoding_meta=True,
        )
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
        self.multipart_staging_buffer_size = multipart_staging_buffer_size
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
        fill_cache: bool
            If seeking to new a part of the file beyond the current buffer,
            with this True, the buffer will be filled between the sections to
            best support random access. When reading only a few specific chunks
            out of a file, performance may be better if False.
        version_id : str
            Explicit version of the object to open.  This requires that the tos
            filesystem is version aware and bucket versioning is enabled on the
            relevant bucket.
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

        if not self.version_aware and version_id:
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
        path = self._strip_protocol(path).rstrip("/")
        if path in ["", "/"]:
            files = self._lsbuckets()
            return files if detail else sorted([o["name"] for o in files])

        files = self._lsdir(path, versions=versions)
        if not files and "/" in path:
            try:
                files = self._lsdir(self._parent(path), versions=versions)
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
        **kwargs: Union[str, bool, float, None],
    ) -> Generator[Union[dict, str], None, None]:
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
        if versions and not self.version_aware:
            raise ValueError(
                "versions cannot be specified if the filesystem "
                "is not version aware."
            )

        path = self._strip_protocol(path).rstrip("/")
        bucket, key, _ = self._split_path(path)
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
                    delimiter="/",
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

            for obj in results:
                if isinstance(obj, CommonPrefixInfo):
                    info = self._fill_dir_info(bucket, obj)
                elif obj.key.endswith("/"):
                    info = self._fill_dir_info(bucket, None, obj.key)
                else:
                    info = self._fill_file_info(obj, bucket, versions)

                yield info if detail else info["name"]

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

        if version_id is not None and not self.version_aware:
            raise ValueError(
                "version_id cannot be specified due to the "
                "filesystem is not support version aware."
            )

        if not key:
            return self._bucket_info(bucket)

        if info := self._object_info(bucket, key, version_id):
            return info

        return self._try_dir_info(bucket, key, path, fullpath)

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
        path = self._strip_protocol(path).rstrip("/") + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            raise TosfsError("Cannot remove a bucket using rmdir api.")

        if not self.exists(path):
            raise FileNotFoundError(f"Directory {path} not found.")

        if not self.isdir(path):
            raise NotADirectoryError(f"{path} is not a directory.")

        if (
            len(self._listobjects(bucket, max_items=1, prefix=key.rstrip("/") + "/"))
            > 0
        ):
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
                    self._list_and_batch_delete_objects(bucket, key)
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
        path = self._strip_protocol(path).rstrip("/") + "/"
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
        path = self._strip_protocol(path).rstrip("/") + "/"
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
        path = self._strip_protocol(path).rstrip("/") + "/"
        bucket, key, _ = self._split_path(path)
        if not key:
            return False

        key = key.rstrip("/") + "/"

        try:
            return retryable_func_executor(
                lambda: self.tos_client.head_object(bucket, key) or True,
                max_retry_num=self.max_retry_num,
            )
        except TosClientError as e:
            raise e
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                return False
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

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
            return retryable_func_executor(
                lambda: self.tos_client.head_object(bucket, key) or True,
                max_retry_num=self.max_retry_num,
            )
        except TosClientError as e:
            raise e
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                return False
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

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
            The remote path of the file to put.
        chunksize : int, optional
            The size of the chunks to read from the file (default is 5 * 2**20).
        **kwargs : Any, optional
            Additional arguments.

        Raises
        ------
        FileNotFoundError
            If the local file does not exist.
        IsADirectoryError
            If the local path is a directory.
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
            raise IsADirectoryError(f"{lpath} is a directory.")

        size = os.path.getsize(lpath)

        content_type = None
        if "ContentType" not in kwargs:
            content_type, _ = mimetypes.guess_type(lpath)

        if self.isfile(rpath):
            self.makedirs(self._parent(rpath), exist_ok=True)

        if self.isdir(rpath):
            rpath = os.path.join(rpath, os.path.basename(lpath))

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

        bucket, key, version_id = self._split_path(rpath)

        def _read_chunks(body: BinaryIO, f: BinaryIO) -> None:
            failed_reads = 0
            bytes_read = 0
            while True:
                try:
                    chunk = body.read(GET_OBJECT_OPERATION_DEFAULT_READ_CHUNK_SIZE)
                except TosClientError as e:
                    failed_reads += 1
                    if failed_reads >= RETRY_NUM:
                        raise e
                    try:
                        body.close()
                    except Exception as e:
                        logger.error(
                            "Failed to close the body when calling "
                            "get_file from %s to %s : %s",
                            rpath,
                            lpath,
                            e,
                        )

                    time.sleep(min(1.7**failed_reads * 0.1, 15))
                    body, _ = self._open_remote_file(
                        bucket, key, version_id, bytes_read, **kwargs
                    )
                    continue
                if not chunk:
                    break
                bytes_read += len(chunk)
                f.write(chunk)

        body, content_length = self._open_remote_file(
            bucket, key, version_id, range_start=0, **kwargs
        )
        try:
            with open(lpath, "wb") as f:
                _read_chunks(body, f)
        finally:
            try:
                body.close()
            except Exception as e:
                logger.error(
                    "Failed to close the body when calling "
                    "get_file from %s to %s: %s",
                    rpath,
                    lpath,
                    e,
                )

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
        bucket, key, vers = self._split_path(path1)

        info = self.info(path1, bucket, key, version_id=vers)
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

    def _list_and_batch_delete_objects(self, bucket: str, key: str) -> None:
        is_truncated = True
        continuation_token = ""
        all_results = []

        class DeletingObject:
            def __init__(self, key: str, version_id: Optional[str] = None):
                self.key = key
                self.version_id = version_id

        while is_truncated:

            def _call_list_objects_type2(
                continuation_token: str = continuation_token,
            ) -> ListObjectType2Output:
                return self.tos_client.list_objects_type2(
                    bucket,
                    prefix=key.rstrip("/") + "/",
                    delimiter="/",
                    max_keys=LS_OPERATION_DEFAULT_MAX_ITEMS,
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

        deleting_objects = [
            DeletingObject(o.key if hasattr(o, "key") else o.prefix)
            for o in all_results
        ]

        if deleting_objects:
            delete_resp = retryable_func_executor(
                lambda: self.tos_client.delete_multi_objects(
                    bucket, deleting_objects, quiet=True
                ),
                max_retry_num=self.max_retry_num,
            )
            if delete_resp.error:
                for d in delete_resp.error:
                    logger.warning("Deleted object: %s failed", d)

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
        out = self._lsdir(
            path, delimiter="", include_self=True, prefix=prefix, **kwargs
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
    ) -> dict:
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
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                pass
            else:
                raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

        return {}

    def _try_dir_info(self, bucket: str, key: str, path: str, fullpath: str) -> dict:
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
                    "type": "directory",
                    "size": 0,
                    "StorageClass": "DIRECTORY",
                }

            raise FileNotFoundError(path)
        except (TosClientError, TosServerError, FileNotFoundError) as e:
            raise e
        except Exception as e:
            raise TosfsError(f"Tosfs failed with unknown error: {e}") from e

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
            return retryable_func_executor(
                lambda: self.tos_client.head_object(bucket, key) or True,
                max_retry_num=self.max_retry_num,
            )
        except TosServerError as e:
            if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                try:
                    return retryable_func_executor(
                        lambda: self.tos_client.head_object(
                            bucket, key.rstrip("/") + "/"
                        )
                        or True,
                        max_retry_num=self.max_retry_num,
                    )
                except TosServerError as ex:
                    if e.status_code == TOS_SERVER_STATUS_CODE_NOT_FOUND:
                        return False
                    else:
                        raise ex
            else:
                raise e
        except Exception as ex:
            raise TosfsError(f"Tosfs failed with unknown error: {ex}") from ex

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

    def _lsbuckets(self) -> List[dict]:
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

    def _lsdir(
        self,
        path: str,
        max_items: int = LS_OPERATION_DEFAULT_MAX_ITEMS,
        delimiter: str = "/",
        prefix: str = "",
        include_self: bool = False,
        versions: bool = False,
    ) -> List[dict]:
        bucket, key, _ = self._split_path(path)
        if not prefix:
            prefix = ""
        if key:
            prefix = key.lstrip("/") + "/" + prefix

        logger.debug("Get directory listing for %s", path)
        dirs = []
        files = []
        for obj in self._listobjects(
            bucket,
            max_items=max_items,
            delimiter=delimiter,
            prefix=prefix,
            include_self=include_self,
            versions=versions,
        ):
            if isinstance(obj, CommonPrefixInfo):
                dirs.append(self._fill_dir_info(bucket, obj))
            elif obj.key.endswith("/"):
                dirs.append(self._fill_dir_info(bucket, None, obj.key))
            else:
                files.append(self._fill_file_info(obj, bucket, versions))
        files += dirs

        return files

    def _listobjects(
        self,
        bucket: str,
        max_items: int = LS_OPERATION_DEFAULT_MAX_ITEMS,
        delimiter: str = "/",
        prefix: str = "",
        include_self: bool = False,
        versions: bool = False,
    ) -> List[Union[CommonPrefixInfo, ListedObject, ListedObjectVersion]]:
        if versions and not self.version_aware:
            raise ValueError(
                "versions cannot be specified if the filesystem is "
                "not version aware."
            )

        all_results = []
        is_truncated = True

        if self.version_aware:
            key_marker, version_id_marker = None, None
            while is_truncated:

                def _call_list_object_versions(
                    key_marker: Optional[Any] = key_marker,
                    version_id_marker: Optional[Any] = version_id_marker,
                ) -> ListObjectVersionsOutput:
                    return self.tos_client.list_object_versions(
                        bucket,
                        prefix,
                        delimiter=delimiter,
                        max_keys=max_items,
                        key_marker=key_marker,
                        version_id_marker=version_id_marker,
                    )

                resp = retryable_func_executor(
                    _call_list_object_versions,
                    args=(key_marker, version_id_marker),
                    max_retry_num=self.max_retry_num,
                )
                is_truncated = resp.is_truncated
                all_results.extend(
                    resp.versions + resp.common_prefixes + resp.delete_markers
                )
                key_marker, version_id_marker = (
                    resp.next_key_marker,
                    resp.next_version_id_marker,
                )
        else:
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
        return (
            bucket,
            key,
            version_id if self.version_aware and version_id else None,
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
            "type": "directory",
        }

    @staticmethod
    def _fill_file_info(obj: ListedObject, bucket: str, versions: bool = False) -> dict:
        result = {
            "Key": f"{bucket}/{obj.key}",
            "size": obj.size,
            "name": f"{bucket}/{obj.key}",
            "type": "file",
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
        if not key:
            raise ValueError("Attempt to open non key-like path: %s" % path)

        if "r" not in mode and int(block_size) < MPU_PART_SIZE_THRESHOLD:
            raise ValueError(
                f"Block size must be >= {MPU_PART_SIZE_THRESHOLD // (2**20)}MB."
            )

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
        self.buffer: Optional[io.BytesIO] = io.BytesIO()

        self.multipart_uploader = MultipartUploader(
            fs=fs,
            bucket=bucket,
            key=key,
            part_size=fs.multipart_size,
            thread_pool_size=fs.multipart_thread_pool_size,
            staging_buffer_size=fs.multipart_staging_buffer_size,
            multipart_threshold=fs.multipart_threshold,
        )

        if "a" in mode and fs.exists(path):
            head = retryable_func_executor(
                lambda: self.fs.tos_client.head_object(bucket, key),
                max_retry_num=self.fs.max_retry_num,
            )
            loc = head.content_length

            if loc < APPEND_OPERATION_SMALL_FILE_THRESHOLD:
                # existing file too small for multi-upload: download
                self.write(self.fs.cat(self.path))
            else:
                self.append_block = True
            self.loc = loc

        if "w" in mode:
            # check the local staging dir if not exist, create it
            for staging_dir in fs.multipart_staging_dirs:
                if not os.path.exists(staging_dir):
                    os.makedirs(staging_dir)

    def _initiate_upload(self) -> None:
        """Create remote file/upload."""
        if self.autocommit and not self.append_block and self.tell() < self.blocksize:
            # only happens when closing small file, use on-shot PUT
            return
        logger.debug("Initiate upload for %s", self)

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

        if (
            self.autocommit
            and not self.append_block
            and final
            and self.tell()
            < max(self.blocksize, self.multipart_uploader.multipart_threshold)
        ):
            # only happens when closing small file, use one-shot PUT
            pass
        else:
            self.multipart_uploader.initiate_upload()
            self.multipart_uploader.upload_multiple_chunks(self.buffer)

        if self.autocommit and final:
            self.commit()

        return not final

    def _fetch_range(self, start: int, end: int) -> bytes:
        if start == end:
            logger.debug(
                "skip fetch for negative range - bucket=%s,key=%s,start=%d,end=%d",
                self.bucket,
                self.key,
                start,
                end,
            )
            return b""
        logger.debug("Fetch: %s/%s, %s-%s", self.bucket, self.key, start, end)

        def fetch() -> bytes:
            temp_buffer = io.BytesIO()
            for chunk in self.fs.tos_client.get_object(
                self.bucket, self.key, self.version_id, range_start=start, range_end=end
            ):
                temp_buffer.write(chunk)
            temp_buffer.seek(0)
            return temp_buffer.read()

        return retryable_func_executor(fetch, max_retry_num=self.fs.max_retry_num)

    def commit(self) -> None:
        """Complete multipart upload or PUT."""
        logger.debug("Commit %s", self)
        if self.tell() == 0:
            if self.buffer is not None:
                logger.debug("Empty file committed %s", self)
                self.multipart_uploader.abort_upload()
                self.fs.touch(self.path, **self.kwargs)
        elif not self.multipart_uploader.staging_files:
            if self.buffer is not None:
                logger.debug("One-shot upload of %s", self)
                self.buffer.seek(0)
                data = self.buffer.read()
                write_result = retryable_func_executor(
                    lambda: self.fs.tos_client.put_object(
                        self.bucket, self.key, content=data
                    ),
                    max_retry_num=self.fs.max_retry_num,
                )
            else:
                raise RuntimeError
        else:
            logger.debug("Complete multi-part upload for %s ", self)
            self.multipart_uploader._upload_staged_files()
            self.multipart_uploader.complete_upload()

        if self.fs.version_aware:
            self.version_id = write_result.version_id

        self.buffer = None

    def discard(self) -> None:
        """Close the file without writing."""
        self.multipart_uploader.abort_upload()
        self.buffer = None
