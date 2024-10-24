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

# require packages:
# pip install torchdata

import os

import fsspec
from torchdata.datapipes.iter import FSSpecFileLister, FSSpecFileOpener
from tos import EnvCredentialsProvider

fsspec.register_implementation("tos", "tosfs.TosFileSystem", )

if __name__ == '__main__':

    kwargs = {
        'endpoint_url': os.environ.get("TOS_ENDPOINT"),
        'credentials_provider' : EnvCredentialsProvider(),
        'region': 'cn-beijing'
    }

    # iterable-style datasets
    file_lister = FSSpecFileLister(root='tos://your-bucket/your-dataset/', **kwargs)
    iterable_dataset = FSSpecFileOpener(file_lister, mode="rb", **kwargs)

    for _, item in iterable_dataset:
        data = item.read()
