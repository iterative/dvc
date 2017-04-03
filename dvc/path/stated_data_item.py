from dvc.path.data_item import DataItem


class StatedDataItem(DataItem):
    STATUS_UNTRACKED = '?'
    STATUS_DELETE = 'D'
    STATUS_MODIFIED = 'M'
    STATUS_TYPE_CHANGED = 'T'

    def __init__(self, state, data_file, git, config, cache_file=None):
        super(StatedDataItem, self).__init__(data_file, git, config, cache_file)
        self._status = state

    @property
    def status(self):
        return self._status

    def _check_status(self, status):
        return self._status.find(status) >= 0

    @property
    def is_removed(self):
        return self._check_status(self.STATUS_DELETE)

    @property
    def is_modified(self):
        return self._check_status(self.STATUS_MODIFIED) \
               or self._check_status(self.STATUS_TYPE_CHANGED)

    @property
    def is_new(self):
        return self._check_status(self.STATUS_UNTRACKED)

    @property
    def is_unusual(self):
        return self.is_new or self.is_modified or self.is_removed

    def __repr__(self):
        return u'({}, {})'.format(self.state, self.data_dvc_short)
