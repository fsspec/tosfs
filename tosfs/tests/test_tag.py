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

import os
from time import sleep

import pytest

from tosfs.tag import TAGGED_BUCKETS_FILE


@pytest.fixture
def _prepare_tag_env():
    if os.path.exists(TAGGED_BUCKETS_FILE):
        os.remove(TAGGED_BUCKETS_FILE)
    yield
    if os.path.exists(TAGGED_BUCKETS_FILE):
        os.remove(TAGGED_BUCKETS_FILE)


@pytest.mark.usefixtures("_prepare_tag_env")
def test_bucket_tag_action(tosfs, bucket, temporary_workspace):
    tag_mgr = tosfs.bucket_tag_mgr
    if tag_mgr is None:
        return

    tag_mgr.cached_bucket_set = set()
    tag_mgr.add_bucket_tag(bucket, "https://example.com/tag-service")
    sleep(10)
    assert os.path.exists(TAGGED_BUCKETS_FILE)
    with open(TAGGED_BUCKETS_FILE, "r") as f:
        tagged_buckets = f.read()
        assert bucket in tagged_buckets

    assert tag_mgr.bucket_tag_service.get_bucket_tag(bucket)
