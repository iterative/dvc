from dvc.exceptions import DvcException


class TimeoutExpired(DvcException):
    def __init__(self, cmd, timeout):
        super().__init__(
            f"'{cmd}' did not complete before timeout '{timeout}'"
        )
        self.cmd = cmd
        self.timeout = timeout
