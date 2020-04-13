from dvc.repo.plot import _load_from_revisions, WORKSPACE_REVISION_NAME


def test_load_no_revisions_clean(mocker):
    m = mocker.patch("dvc.repo.plot._load_from_revision")
    repo = mocker.MagicMock()
    repo.scm.is_dirty.return_value = False

    _load_from_revisions(repo, "datafile", [], False)

    assert m.call_count == 1
    assert m.call_args_list[0] == mocker.call(
        repo, "datafile", WORKSPACE_REVISION_NAME, default_plot=False
    )


def test_load_no_revisions_dirty(mocker):
    m = mocker.patch("dvc.repo.plot._load_from_revision")
    repo = mocker.MagicMock()
    repo.scm.is_dirty.return_value = True

    _load_from_revisions(repo, "datafile", [], False)

    assert m.call_count == 2
    assert m.call_args_list[0] == mocker.call(
        repo, "datafile", "HEAD", default_plot=False
    )
    assert m.call_args_list[1] == mocker.call(
        repo, "datafile", WORKSPACE_REVISION_NAME, default_plot=False
    )


def test_load_one(mocker):
    m = mocker.patch("dvc.repo.plot._load_from_revision")
    repo = mocker.MagicMock()
    repo.scm.is_dirty.return_value = True

    _load_from_revisions(repo, "datafile", ["tag1"], False)

    assert m.call_count == 2
    assert m.call_args_list[0] == mocker.call(
        repo, "datafile", "tag1", default_plot=False
    )
    assert m.call_args_list[1] == mocker.call(
        repo, "datafile", WORKSPACE_REVISION_NAME, default_plot=False
    )


def test_load_more(mocker):
    m = mocker.patch("dvc.repo.plot._load_from_revision")
    repo = mocker.MagicMock()
    repo.scm.is_dirty.return_value = True

    _load_from_revisions(repo, "datafile", ["tag1", "tag2"], False)

    assert m.call_count == 2
    assert m.call_args_list[0] == mocker.call(
        repo, "datafile", "tag1", default_plot=False
    )
    assert m.call_args_list[1] == mocker.call(
        repo, "datafile", "tag2", default_plot=False
    )
