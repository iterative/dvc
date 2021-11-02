import pytest


@pytest.fixture
def run_copy_metrics(tmp_dir, run_copy):
    def run(
        file1,
        file2,
        commit=None,
        tag=None,
        single_stage=True,
        name=None,
        **kwargs,
    ):
        if name:
            single_stage = False

        stage = tmp_dir.dvc.run(
            cmd=f"python copy.py {file1} {file2}",
            deps=[file1],
            single_stage=single_stage,
            name=name,
            **kwargs,
        )

        if hasattr(tmp_dir.dvc, "scm"):
            files = [stage.path]
            files += [out.fs_path for out in stage.outs if not out.use_cache]
            tmp_dir.dvc.scm.add(files)
            if commit:
                tmp_dir.dvc.scm.commit(commit)
            if tag:
                tmp_dir.dvc.scm.tag(tag)
        return stage

    return run
