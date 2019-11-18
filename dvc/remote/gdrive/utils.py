import os

from dvc.progress import Tqdm


FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class TrackFileReadProgress(object):
    def __init__(self, progress_name, fobj):
        self.progress_name = progress_name
        self.fobj = fobj
        file_size = os.fstat(fobj.fileno()).st_size
        self.tqdm = Tqdm(desc=self.progress_name, total=file_size)

    def read(self, size):
        self.tqdm.update(size)
        return self.fobj.read(size)

    def close(self):
        self.fobj.close()
        self.tqdm.close()

    def __getattr__(self, attr):
        return getattr(self.fobj, attr)
