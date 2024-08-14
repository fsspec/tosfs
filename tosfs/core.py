# ByteDance Volcengine EMR, Copyright 2022.
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
The core module of TOSFS.
"""
import logging
import os

from fsspec.utils import setup_logging as setup_logger

# environment variable names
ENV_NAME_TOSFS_LOGGING_LEVEL = "TOSFS_LOGGING_LEVEL"

logger = logging.getLogger("tosfs")


def setup_logging():
    """
    Set up the logging configuration for TOSFS.
    """
    setup_logger(
        logger=logger,
        level=os.environ.get(ENV_NAME_TOSFS_LOGGING_LEVEL, "INFO"),
    )


setup_logging()

logger.warning(
    "The tosfs's log level is set to be %s", logging.getLevelName(logger.level)
)
