import os.path

from funcy import cached_property

from .http import RemoteHTTP
from dvc.scheme import Schemes
from dvc.path_info import HTTPURLInfo
from dvc.progress import Tqdm
from dvc.exceptions import HTTPError


class WebdavURLInfo(HTTPURLInfo):
    def __init__(self, url):
        super().__init__(url)

    @cached_property
    def url(self):
        return "{}://{}{}{}{}{}".format(
            self.scheme.replace("webdav", "http"),
            self.netloc,
            self._spath,
            (";" + self.params) if self.params else "",
            ("?" + self.query) if self.query else "",
            ("#" + self.fragment) if self.fragment else "",
        )

    def get_collections(self) -> list:
        def pcol(path):
            return "{}://{}{}".format(
                self.scheme.replace("webdav", "http"),
                self.netloc,
                path,
            )
        p = self.path.split("/")[1:-1]
        if not p:
            return []
        r = []
        for i in range(len(p)):
            r.append(pcol("/{}/".format("/".join(p[:i + 1]))))
        return r


class RemoteWEBDAV(RemoteHTTP):
    scheme = Schemes.WEBDAV
    path_cls = WebdavURLInfo

    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        def chunks():
            with open(from_file, "rb") as fd:
                with Tqdm.wrapattr(
                    fd,
                    "read",
                    total=None
                    if no_progress_bar
                    else os.path.getsize(from_file),
                    leave=False,
                    desc=to_info.url if name is None else name,
                    disable=no_progress_bar,
                ) as fd_wrapped:
                    while True:
                        chunk = fd_wrapped.read(self.CHUNK_SIZE)
                        if not chunk:
                            break
                        yield chunk

        self._create_collections(to_info)
        response = self._request("PUT", to_info.url, data=chunks())
        if response.status_code not in (200, 201):
            raise HTTPError(response.status_code, response.reason)

    def _create_collections(self, to_info):
        url_cols = to_info.get_collections()
        from_i = 0
        for i in reversed(range(len(url_cols) + 1)):
            from_i = i
            if bool(self._request("HEAD", url_cols[i - 1])):
                break
        for i in range(from_i, len(url_cols)):
            response = self._request("MKCOL", url_cols[i])
            if response.status_code not in (200, 201):
                raise HTTPError(response.status_code, response.reason)

    def gc(self):
        raise NotImplementedError

    def list_cache_paths(self, prefix=None, progress_callback=None):
        raise NotImplementedError

    def walk_files(self, path_info):
        raise NotImplementedError
