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

"""The module contains constants for the tosfs package."""

# Tos server response status codes
TOS_SERVER_STATUS_CODE_NOT_FOUND = 404

# tos bucket type (hns, fns)
TOS_BUCKET_TYPE_HNS = "hns"
TOS_BUCKET_TYPE_FNS = "fns"

MANAGED_COPY_MAX_THRESHOLD = 5 * 2**30
MANAGED_COPY_MIN_THRESHOLD = 5 * 2**20

RETRY_NUM = 5
PART_MIN_SIZE = 5 * 2**20
PART_MAX_SIZE = 5 * 2**30
MPU_PART_SIZE_THRESHOLD = 4 * 2**20  # 4MB
FILE_OPERATION_READ_WRITE_BUFFER_SIZE = 5 * 2**20  # 5MB
PUT_OBJECT_OPERATION_SMALL_FILE_THRESHOLD = 5 * 2**30  # 5GB
GET_OBJECT_OPERATION_DEFAULT_READ_CHUNK_SIZE = 2**16  # 64KB
APPEND_OPERATION_SMALL_FILE_THRESHOLD = 5 * 2**20  # 5MB

LS_OPERATION_DEFAULT_MAX_ITEMS = 1000

TOSFS_LOG_FORMAT = "%(asctime)s %(name)s [%(levelname)s] %(filename)s:%(lineno)d %(funcName)s : %(message)s"  # noqa: E501

# environment variable names
ENV_NAME_TOSFS_LOGGING_LEVEL = "TOSFS_LOGGING_LEVEL"
ENV_NAME_TOS_SDK_LOGGING_LEVEL = "TOS_SDK_LOGGING_LEVEL"
ENV_NAME_TOS_BUCKET_TAG_ENABLE = "TOS_BUCKET_TAG_ENABLE"
ENV_NAME_VOLC_REGION = "VOLC_REGION"
