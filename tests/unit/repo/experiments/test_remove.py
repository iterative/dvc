from dvc.repo.experiments.queue.base import QueueDoneResult


def test_remove_done_tasks(dvc, test_queue, scm, mocker):
    from funcy import concat

    failed_test = ["failed1", "failed2"]
    success_test = ["success1", "success2"]

    # create mock ref info
    ref_info_dict = {}
    for name in success_test:
        ref_info_dict[name] = mocker.Mock()
        ref_info_dict[name].name = name
    for name in failed_test:
        ref_info_dict[name] = None

    # create mock queue entry
    entry_dict = {}
    for name in concat(failed_test, success_test):
        entry_dict[name] = mocker.Mock(stash_rev=name)
        entry_dict[name].name = name

    done_iter = [
        QueueDoneResult(entry_dict[name], None)
        for name in concat(failed_test, success_test)
    ]

    mocker.patch.object(test_queue, "iter_done", return_value=done_iter)

    mocker.patch(
        "dvc.repo.experiments.utils.resolve_name",
        autospec=True,
        return_value=ref_info_dict,
    )

    remove_exp_refs = mocker.patch(
        "dvc.repo.experiments.utils.remove_exp_refs",
    )
    remove_tasks_mocker = mocker.patch(
        "dvc.repo.experiments.queue.remove.remove_tasks",
    )

    assert (
        dvc.experiments.remove(failed_test + success_test) == failed_test + success_test
    )

    remove_tasks_mocker.assert_called_once_with(
        test_queue,
        [entry_dict[name] for name in failed_test + success_test],
    )

    remove_exp_refs.assert_called_once_with(
        dvc.scm, [ref_info_dict[name] for name in success_test]
    )
