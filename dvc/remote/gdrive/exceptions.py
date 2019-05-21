from dvc.remote.gdrive.utils import response_error_message


class GDriveError(Exception):
    pass


class GDriveHTTPError(GDriveError):
    def __init__(self, response):
        super(GDriveHTTPError, self).__init__(response_error_message(response))


class GDriveResourceNotFound(GDriveError):
    def __init__(self, path):
        super(GDriveResourceNotFound, self).__init__(
            "'{}' resource not found".format(path)
        )
