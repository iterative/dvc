import traceback


class DvcException(Exception):
    def __init__(self, msg, cause=None):
        self.cause = cause
        self.case_tb = None
        if cause:
            try:
                self.cause_tb = traceback.format_exc()
            except Exception:
                pass
        super(DvcException, self).__init__(msg)


class UnsupportedRemoteError(DvcException):
    def __init__(self, config, cause=None):
        msg = "Remote '{}' is not supported.".format(config)
        super(UnsupportedRemoteError, self).__init__(msg, cause=cause)
