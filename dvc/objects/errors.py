from dvc.exceptions import DvcException


class ObjectError(DvcException):
    pass


class ObjectFormatError(ObjectError):
    pass


class ObjectDBError(DvcException):
    pass


class ObjectDBPermissionError(ObjectDBError):
    pass
