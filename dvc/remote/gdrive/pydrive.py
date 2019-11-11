import os

from dvc.remote.gdrive.utils import FOLDER_MIME_TYPE
from dvc.remote.gdrive.utils import TrackFileReadProgress


class RequestBASE:
    def __init__(self, drive):
        self.drive = drive

    def execute(self):
        raise NotImplementedError


class RequestListFile(RequestBASE):
    def __init__(self, drive, query):
        super(RequestListFile, self).__init__(drive)
        self.query = query

    def execute(self):
        return self.drive.ListFile({
            "q": self.query,
            "maxResults": 1000
        }).GetList()


class RequestListFilePaginated(RequestBASE):
    def __init__(self, drive, query):
        super(RequestListFilePaginated, self).__init__(drive)
        self.query = query
        self.iter = None

    def execute(self):
        if not self.iter:
            self.iter = iter(
                self.drive.ListFile({
                    "q": self.query,
                    "maxResults": 1000
                }))
        return next(self.iter, None)


class RequestCreateFolder(RequestBASE):
    def __init__(self, args):
        super(RequestCreateFolder, self).__init__(args["drive"])
        self.title = args["title"]
        self.parent_id = args["parent_id"]

    def execute(self):
        item = self.drive.CreateFile({
            "title": self.title,
            "parents": [{
                "id": self.parent_id
            }],
            "mimeType": FOLDER_MIME_TYPE,
        })
        item.Upload()
        return item


class RequestUploadFile(RequestBASE):
    def __init__(self,
                 args,
                 no_progress_bar=True,
                 from_file="",
                 progress_name=""):
        super(RequestUploadFile, self).__init__(args["drive"])
        self.title = args["title"]
        self.parent_id = args["parent_id"]
        self.no_progress_bar = no_progress_bar
        self.from_file = from_file
        self.progress_name = progress_name

    def upload(self, item):
        with open(self.from_file, "rb") as from_file:
            if not self.no_progress_bar:
                from_file = TrackFileReadProgress(self.progress_name,
                                                  from_file)
            if os.stat(self.from_file).st_size:
                item.content = from_file
            item.Upload()

    def execute(self):
        item = self.drive.CreateFile({
            "title": self.title,
            "parents": [{
                "id": self.parent_id
            }]
        })
        self.upload(item)
        return item


class RequestDownloadFile(RequestBASE):
    def __init__(self, args):
        super(RequestDownloadFile, self).__init__(args["drive"])
        self.file_id = args["file_id"]
        self.to_file = args["to_file"]
        self.progress_name = args["progress_name"]
        self.no_progress_bar = args["no_progress_bar"]

    def execute(self):
        from dvc.progress import Tqdm

        gdrive_file = self.drive.CreateFile({"id": self.file_id})
        if not self.no_progress_bar:
            tqdm = Tqdm(desc=self.progress_name,
                        total=int(gdrive_file["fileSize"]))
        gdrive_file.GetContentFile(self.to_file)
        if not self.no_progress_bar:
            tqdm.close()
