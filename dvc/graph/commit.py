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
    def to_json(self):
        return {'hash': self.hash, 'text': Commit._limit_text(self._comment)}

    def get_to_show_commits(self, max_commits_to_show):
        assert max_commits_to_show > 0

        not_repro = [self] + filter(lambda c: not c.is_repro, self._collapsed_commits)
        commits_to_show = not_repro[:max_commits_to_show - 1]
        num_commits_to_collapse = 1 + len(self._collapsed_commits) - len(commits_to_show)

        return commits_to_show, num_commits_to_collapse

    def get_to_show_commits_json(self, max_commits_to_show):
        commits_to_show, num_commits_to_collapse = self.get_to_show_commits(max_commits_to_show)
        return map(lambda x: x.to_json, commits_to_show), num_commits_to_collapse

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
        self._collapsed_commits += commit._collapsed_commits

    def text(self, max_commits=100):
        branch_text = ''
        if self._branch_tips:
            branch_text = 'BRANCH TIPS: {}\n'.format(', '.join(self._branch_tips))

        return self._text_metrics_line() + branch_text + self._comments_text(max_commits)

    @staticmethod
    def _text_comment(commit):
        if commit._is_collapsed:
            text = Commit.COLLAPSED_TEXT
        else:
            text = Commit._limit_text(commit._comment)
        return '[{}] {}'.format(commit.hash, text)

    @staticmethod
    def _limit_text(comment):
        if len(comment) < Commit.TEXT_LIMIT:
            return comment
        else:
            return comment[:Commit.TEXT_LIMIT-3] + '...'

    def _comments_text(self, max_commits=100):
        commits_to_show, commits_not_to_show_num = self.get_to_show_commits(max_commits)
        res = map(lambda c: self._text_comment(c), commits_to_show)

        max_len = max(map(lambda x: len(x), res + [""]))
        res_extended_len = map(lambda x: x.ljust(max_len), res)

        was_collapsed = commits_not_to_show_num > 0
        if was_collapsed:
            res_extended_len.append('<<Collapsed commits>>')

        return '\n'.join(res_extended_len)

    def _text_metrics_line(self):
        result = ''
        if self._is_target and self._target_metric:
            result = 'TARGET: {}'.format(self._target_metric)
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
        return map(lambda s: s[5:] if s[:5] == 'refs/' else s, self._branch_tips)

    def add_branch_tips(self, tips):
        for tip in tips:
            self._branch_tips.append(tip)

    def set_target_metric_delta(self, value):
        self._target_metric_delta = value

    @property
    def target_metric_delta(self):
        return self._target_metric_delta
