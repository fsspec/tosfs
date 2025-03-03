# tosfs

[![PyPI version](https://badge.fury.io/py/tosfs.svg)](https://pypi.python.org/pypi/tosfs/)
[![Status](https://img.shields.io/pypi/status/tosfs.svg)](https://pypi.org/project/tosfs/)
[![Python Version](https://img.shields.io/pypi/pyversions/tosfs.svg)](https://pypi.org/project/tosfs/)
[![License](https://img.shields.io/pypi/l/tosfs)](https://opensource.org/licenses/Apache-2.0)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Doc Status](https://readthedocs.org/projects/tosfs/badge/?version=latest)](https://tosfs.readthedocs.io/en/latest/?badge=latest)
![Build-FNS](https://github.com/fsspec/tosfs/workflows/CI-FNS/badge.svg)
![Build-HNS](https://github.com/fsspec/tosfs/workflows/CI-HNS/badge.svg)
![PyPI - Downloads](https://img.shields.io/pypi/dm/tosfs)
![PyPI Downloads](https://static.pepy.tech/badge/tosfs)


TOSFS builds on [Volcengine TOS Python SDK](https://github.com/volcengine/ve-tos-python-sdk) to provide a convenient Python filesystem interface for [TOS（Tinder Object Storage）](https://www.volcengine.com/docs/6349/74820).

## Features

* Excellent read/write performance than `s3fs` (optimized by multi-threading and multi-disk staging).
* Solid stability (fine-grained judgment on response codes for TOS services).
* Outstanding compatibility (cross-validation completed on the version matrix of four Python versions and two fsspec versions).
* TOS HNS (Hierarchical NameSpace) Bucket support(Beta).
* Native [append API](https://www.volcengine.com/docs/6349/74863) support.
* Extended features:
  * support assume role credentials; 
  * batch list iterate;
  * multiple thread walk;
  * support special characters in local path;

## Installation

You can install tosfs via [pip](https://pip.pypa.io/) from [PyPI](https://pypi.org/):

```shell
$ pip install tosfs
```

## Quick Start

### Init FileSystem

* Init via `ak/sk`

```python
from tosfs.core import TosFileSystem

tosfs = TosFileSystem(
    key='ak',
    secret='sk',
    endpoint_url='http://tos-cn-beijing.volces.com',
    region='cn-beijing',
)
```

* Init via system env

make sure these envs take effect:

```shell
export TOS_ACCESS_KEY=' your ak here '
export TOS_SECRET_KEY=' your sk here '
export TOS_ENDPOINT='http://tos-cn-beijing.volces.com'
export TOS_REGION='cn-beijing'
```
then init `TosFileSystem` by setting `credentials_provider` to `EnvCredentialsProvider`

```python
import os
from tosfs.core import TosFileSystem
from tos import EnvCredentialsProvider

tosfs = TosFileSystem(
    endpoint_url=os.environ.get("TOS_ENDPOINT"),
    region=os.environ.get("TOS_REGION"),
    credentials_provider=EnvCredentialsProvider, # must
)
```

* Init via assume role(temporary ak/sk and session token).

make sure you have an assume role credential generator, then you can init the `TosFileSystem` like this:

```python
from tosfs import TosFileSystem
from tosfs.certification import FileCredentialsProvider
fs = TosFileSystem(
    endpoint="http://tos-cn-beijing.volces.com",
    region="cn-beijing",
    credentials_provider=FileCredentialsProvider("/etc/secret/las"), # replace to real path
)
```

### Access FS and file operation APIs

After getting the instance of `TosFileSystem` by following the above guidance,
now we can access fs and file operation apis, e.g.

```python
# list
tosfs.ls("")
tosfs.ls("mybucket", detail=False)
tosfs.ls("mybucket/rootpath/", detail=False)

# file read/write
with tosfs.open('bucket/root/text.txt', mode='w') as f:
    f.write('hello tosfs!')
    
with tosfs.open('bucket/root/text.txt', mode='r') as f:
    content = f.read()
    print(content)
```

### integration usages

For more usage in ray, pyspark, pytorch and so on, please refer to the [examples](https://github.com/fsspec/tosfs/tree/main/examples) dir.

## Compatibility

The tosfs package is compatible with the following Python and fsspec versions:

* Python

| Version | Supported |
|---------|-----------|
| 3.9     | ✅         |
| 3.10    | ✅         |
| 3.11    | ✅         |
| 3.12    | ✅         |

* fsspec

| Version   | Supported |
|-----------|------|
| 2023.5.0  | ✅   |
| 2024.9.0  | ✅   |
| 2024.10.0 | ✅   |

## Contributing
Contributions are very welcome. To learn more, see the [Contributor Guide](https://github.com/fsspec/tosfs/blob/main/CONTRIBUTING.md).

## License
Distributed under the terms of the [Apache 2.0 license](https://github.com/fsspec/tosfs/blob/main/LICENSE), Tosfs is free and open source software.

## Issues
If you encounter any problems, please [file an issue](https://github.com/fsspec/tosfs/issues/new/choose) along with a detailed description.

## Privacy Statement
Welcome to use our open-source project [tosfs](https://github.com/fsspec/tosfs). We highly value your privacy and are committed to protecting your information. This privacy statement aims to inform you about how we collect, use, store, and protect your information within the project (we believe you have the right to know).

    Information Collection: 
      We may collect the names of the buckets you access in the TOS service 
      (and only the names of the buckets you access). 
      You can opt out of this collection by setting the environment variable 
      "TOS_BUCKET_TAG_ENABLE" to "false", and we will respect your decision.

    Information Usage: 
      We will only use the collected information to tag the source of access 
      and commit that this information will solely be used for the purpose of 
      tagging the source of access.

    Information Storage: 
      We will take reasonable security measures to protect the information 
      we collect from unauthorized access, disclosure, use, or destruction. 
      We commit that this information will not be stored on third-party service 
      providers' servers. Additionally, we will comply with applicable laws, 
      regulations, and security standards.

    Information Sharing: 
      We will not sell, rent, or share the collected information with any third parties.

If you need any further adjustments or additional details, feel free to let us know!
