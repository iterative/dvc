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
