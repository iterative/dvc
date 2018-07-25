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
    def __init__(self, config):
        msg = "Remote '{}' is not supported.".format(config)
        super(UnsupportedRemoteError, self).__init__(msg)


class OutputDuplicationError(DvcException):
    def __init__(self, output, stages):
        assert isinstance(output, str)
        assert isinstance(stages, list)
        assert all(isinstance(stage, str) for stage in stages)
        msg = "File '{}' is specified as an output in more than one stage: {}"
        super(OutputDuplicationError, self).__init__(msg.format(output,
                                                                str(stages)))
