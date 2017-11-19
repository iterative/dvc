from dvc.exceptions import DvcException


class CmdCommonError(DvcException):
    def __init__(self, msg):
        super(CmdCommonError, self).__init__('{}'.format(msg))
