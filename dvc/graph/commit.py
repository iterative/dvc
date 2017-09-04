class Commit(object):
    TEXT_LIMIT = 30
    DVC_REPRO_PREFIX = 'DVC repro'
    COLLAPSED_TEXT = DVC_REPRO_PREFIX + '\n<< collapsed commits >>'

    def __init__(self, hash, parents, name, date, comment,
                 is_target=False,
                 target_metric=None,
                 branch_tips=None):
        self._hash = hash
        self._parent_hashes = set(parents.split())
        self._name = name
        self._date = date
        self._comment = comment
        self._is_target = is_target
        self._target_metric = target_metric
        self._branch_tips = [] if branch_tips is None else branch_tips

        self._is_collapsed = False

    @property
    def hash(self):
        return self._hash

    @property
    def parent_hashes(self):
        return self._parent_hashes

    def add_parents(self, parent_hashes):
        self._parent_hashes |= parent_hashes

    def remove_parent(self, hash):
        self._parent_hashes.remove(hash)

    @property
    def text(self):
        branch_text = ''
        if self._branch_tips:
            branch_text = 'Branch tips: {}\n'.format(', '.join(self._branch_tips))

        metric_text = ''
        if self._is_target and self._target_metric:
            metric_text = '\nTarget metric: {}'.format(self._target_metric)

        if self._is_collapsed:
            return branch_text + self.COLLAPSED_TEXT + metric_text

        return branch_text + self._comment[:self.TEXT_LIMIT] + '\n' + self.hash + metric_text

    @property
    def is_repro(self):
        return self._comment.startswith(self.DVC_REPRO_PREFIX)

    def make_colapsed(self):
        self._is_collapsed = True

    @property
    def has_target_metric(self):
        return self._is_target and self._target_metric is not None

    @property
    def target_metric(self):
        return self._target_metric

    def set_target_metric(self, value):
        self._target_metric = value
        self._is_target = True

    @property
    def branch_tips(self):
        return self._branch_tips

    def add_branch_typs(self, tips):
        for tip in tips:
            self._branch_tips.append(tip)
