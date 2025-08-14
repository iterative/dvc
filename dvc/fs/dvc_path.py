"""UPath implementation for DVCFileSystem.

This provides a `pathlib.Path` like interface to
work with DVCFileSystem.

Examples
--------

>>> from upath import UPath

>>> local = UPath("dvc://path/to/local/repo")
>>> https = UPath("dvc+https://github.com/iterative/example-get-started", rev="main")
>>> ssh = UPath("dvc+ssh://git@github.com:iterative/example-get-started.git")
"""

from urllib.parse import urlsplit

from upath import UPath  # ty: ignore[unresolved-import]


class DVCPath(UPath):
    @classmethod
    def _transform_init_args(cls, args, protocol, storage_options):
        if not args:
            args = ("/",)
        elif (
            args
            and "url" not in storage_options
            and protocol in {"dvc+http", "dvc+https", "dvc+ssh"}
        ):
            url, *rest = args
            url = urlsplit(str(url))
            proto = protocol.split("+")[1]
            if proto == "ssh":
                base_url = url.netloc + url.path
            else:
                base_url = url._replace(scheme=proto).geturl()
            storage_options["url"] = base_url
            # Assume the given path is a root url
            args = ("/", *rest)
        return super()._transform_init_args(args, "dvc", storage_options)

    def __str__(self):
        s = super().__str__()
        if url := self.storage_options.get("url"):
            return s.replace("dvc://", f"dvc+{url}", 1)
        return s

    def with_segments(self, *pathsegments):
        obj = super().with_segments(*pathsegments)
        # cache filesystem, as dvcfs does not cache filesystem
        # caveat: any joinpath operation will instantiate filesystem
        obj._fs_cached = self.fs
        return obj
