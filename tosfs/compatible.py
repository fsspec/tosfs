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

"""The compatible module about AbstractFileSystem in fsspec."""
from typing import Any, Optional

from fsspec import AbstractFileSystem


class FsspecCompatibleFS(AbstractFileSystem):
    """A fsspec compatible file system.

    Used to be compatible with old version in some special methods.
    """

    def walk(  # noqa
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        on_error: str = "omit",
        **kwargs: Any,
    ) -> Any:
        """Return all files belows path.

        Copied from fsspec(2024.9.0) to fix fsspec(2023.5.0.)

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        When topdown is True, the caller can modify the dirnames list in-place (perhaps
        using del or slice assignment), and walk() will
        only recurse into the subdirectories whose names remain in dirnames;
        this can be used to prune the search, impose a specific order of visiting,
        or even to inform walk() about directories the caller creates or renames before
        it resumes walk() again.
        Modifying dirnames when topdown is False has no effect. (see os.walk)

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into
        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.
        topdown: bool (True)
            Whether to walk the directory tree from the top downwards or from
            the bottom upwards.
        on_error: "omit", "raise", a collable
            if omit (default), path with exception will simply be empty;
            If raise, an underlying exception will be raised;
            if callable, it will be called with a single OSError instance as argument
        kwargs: passed to ``ls``

        """
        # type: ignore
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        path = self._strip_protocol(path)
        full_dirs = {}
        dirs = {}
        files = {}

        detail = kwargs.pop("detail", False)
        try:
            listing = self.ls(path, detail=True, **kwargs)
        except (FileNotFoundError, OSError) as e:
            if on_error == "raise":
                raise
            elif callable(on_error):
                on_error(e)
            if detail:
                return path, {}, {}  # type: ignore
            return path, [], []  # type: ignore

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            pathname = info["name"].rstrip("/")  # type: ignore
            name = pathname.rsplit("/", 1)[-1]
            if info["type"] == "directory" and pathname != path:  # type: ignore
                # do not include "self" path
                full_dirs[name] = pathname
                dirs[name] = info
            elif pathname == path:
                # file-like with same name as give path
                files[""] = info
            else:
                files[name] = info

        if not detail:
            dirs = list(dirs)  # type: ignore
            files = list(files)  # type: ignore

        if topdown:
            # Yield before recursion if walking top down
            yield path, dirs, files

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                if not topdown:
                    yield path, dirs, files
                return

        for d in dirs:
            yield from self.walk(
                full_dirs[d],
                maxdepth=maxdepth,
                detail=detail,
                topdown=topdown,
                **kwargs,
            )

        if not topdown:
            # Yield after recursion if walking bottom up
            yield path, dirs, files

    def find(  # noqa #
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs: Any,  # type: ignore
    ) -> Any:
        """List all files below path.

        Copied from fsspec(2024.9.0) to fix fsspec(2023.5.0.)

        Like posix ``find`` command without conditions

        Parameters
        ----------
        path : str
        maxdepth: int or None
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        kwargs are passed to ``ls``.

        """
        # TODO: allow equivalent of -name parameter
        path = self._strip_protocol(path)
        out = {}

        # Add the root directory if withdirs is requested
        # This is needed for posix glob compliance
        if withdirs and path != "" and self.isdir(path):
            out[path] = self.info(path)

        for _, dirs, files in super().walk(path, maxdepth, detail=True, **kwargs):
            if withdirs:
                files.update(dirs)
            out.update({info["name"]: info for name, info in files.items()})
        if not out and self.isfile(path):
            # walk works on directories, but find should also return [path]
            # when path happens to be a file
            out[path] = {}
        names = sorted(out)
        if not detail:
            return names
        else:
            return {name: out[name] for name in names}
