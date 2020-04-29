import os.path

from .http import RemoteHTTP
from dvc.scheme import Schemes
from dvc.progress import Tqdm
from dvc.exceptions import HTTPError
from dvc.path_info import WebdavURLInfo


class RemoteWEBDAV(RemoteHTTP):
    scheme = Schemes.WEBDAV
    path_cls = WebdavURLInfo
    REQUEST_TIMEOUT = 20

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
        from_idx = 0
        for idx in reversed(range(len(url_cols) + 1)):
            from_idx = idx
            if bool(self._request("HEAD", url_cols[idx - 1])):
                break
        for idx in range(from_idx, len(url_cols)):
            response = self._request("MKCOL", url_cols[idx])
            if response.status_code not in (200, 201):
                if bool(self._request("HEAD", url_cols[idx])):
                    continue
                raise HTTPError(response.status_code, response.reason)

    def remove(self, path_info):
        response = self._request("DELETE", path_info.url)
        if response.status_code not in (200, 201, 204):
            raise HTTPError(response.status_code, response.reason)

    def gc(self):
        return super(RemoteHTTP, self).gc()

    def list_cache_paths(self, prefix=None, progress_callback=None):
        raise NotImplementedError

    def walk_files(self, path_info):
        raise NotImplementedError
