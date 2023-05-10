# pylint: disable=unused-argument
import shutil


def test_init(bench_dvc, tmp_dir, scm):
    def _cleanup_dir():
        for item in tmp_dir.iterdir():
            if item.is_dir():
                if item.name != ".git":
                    shutil.rmtree(item)
            else:
                item.unlink()

    bench_dvc("init", setup=_cleanup_dir, rounds=100, warmup_rounds=1)
