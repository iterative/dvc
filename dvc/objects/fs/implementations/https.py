from .http import HTTPFileSystem


class HTTPSFileSystem(HTTPFileSystem):  # pylint:disable=abstract-method
    protocol = "https"
