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
        self._collapsed_commits = []

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
        self._collapsed_commits.append(commit)

    @property
    def text(self):
        branch_text = ''
        if self._branch_tips:
            branch_text = 'Branch tips: {}\n'.format(', '.join(self._branch_tips))

        # text = self._text_comment(self.hash, self._comment, self._is_collapsed)
        text = ''
        return self._text_metrics_line() + branch_text + text + self._text_added_comments

    def _text_hash(self):
        return 'Commit: ' + self.hash

    # @property
    @staticmethod
    def _text_comment(hash, comment, is_collapsed):
        text = Commit.COLLAPSED_TEXT if is_collapsed else Commit._limit_text(comment)
        return '{}: {}'.format(hash, text)

    @staticmethod
    def _limit_text(comment):
        if len(comment) < Commit.TEXT_LIMIT:
            return comment
        else:
            return comment[:Commit.TEXT_LIMIT-3] + '...'

    @property
    def _text_added_comments(self):
        res = [self._text_comment(self.hash, self._comment, self._is_collapsed)]

        for commit in self._collapsed_commits:
            text = self._text_comment(commit.hash, commit._comment, commit.is_repro)
            res.append(text)

        max_len = max(map(lambda x: len(x), res))
        res_extended_len = map(lambda x: x.ljust(max_len), res)
        return '\n'.join(res_extended_len)

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
