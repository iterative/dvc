class Commit(object):
    TEXT_LIMIT = 30
    DVC_REPRO_PREFIX = 'DVC repro'
    COLLAPSED_TEXT = DVC_REPRO_PREFIX + ' <<collapsed>>'

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
        self._target_metric_delta = None
        self._branch_tips = [] if branch_tips is None else branch_tips
        self._added_commits_pairs = []

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

    def add_collapsed_commit(self, commit):
        self._added_commits_pairs.append((commit.hash, commit._comment))

    @property
    def text(self):
        branch_text = ''
        if self._branch_tips:
            branch_text = 'Branch tips: {}\n'.format(', '.join(self._branch_tips))

        return self._text_metrics_line() + branch_text + self._text_comment + self._text_added_comments

    def _text_hash(self):
        return 'Commit: ' + self.hash

    @property
    def _text_comment(self):
        text = self.COLLAPSED_TEXT if self._is_collapsed else self._limit_text(self._comment)
        return '{}: {}'.format(self.hash, text)

    def _limit_text(self, comment):
        if len(comment) < self.TEXT_LIMIT:
            return self._comment
        else:
            return comment[:self.TEXT_LIMIT-3] + '...'

    @property
    def _text_added_comments(self):
        res = ''
        for hash, commit in self._added_commits_pairs:
            res += '\n{}: {}'.format(hash, self._limit_text(commit))
        return res

    def _text_metrics_line(self):
        result = ''
        if self._is_target and self._target_metric:
            result = 'Target: {}'.format(self._target_metric)
            if self._target_metric_delta is not None:
                result += ' ({:+f})'.format(self._target_metric_delta)
            result += '\n'
        return result

    @property
    def is_repro(self):
        return self._comment.startswith(self.DVC_REPRO_PREFIX)

    def make_collapsed(self):
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

    def add_branch_tips(self, tips):
        for tip in tips:
            self._branch_tips.append(tip)

    def set_target_metric_delta(self, value):
        self._target_metric_delta = value

    @property
    def target_metric_delta(self):
        return self._target_metric_delta
