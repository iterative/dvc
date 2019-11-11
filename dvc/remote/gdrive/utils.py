import os

from dvc.progress import Tqdm

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class TrackFileReadProgress(object):
    UPDATE_AFTER_READ_COUNT = 30

    def __init__(self, progress_name, fobj):
        self.progress_name = progress_name
        self.fobj = fobj
        self.file_size = os.fstat(fobj.fileno()).st_size
        self.tqdm = Tqdm(desc=self.progress_name, total=self.file_size)
        self.update_counter = 0

    def read(self, size):
        if self.update_counter == 0:
            self.tqdm.update_to(self.fobj.tell())
            self.update_counter = self.UPDATE_AFTER_READ_COUNT
        else:
            self.update_counter -= 1
        return self.fobj.read(size)

    def close(self):
        self.fobj.close()
        self.tqdm.close()

    def __getattr__(self, attr):
        return getattr(self.fobj, attr)
