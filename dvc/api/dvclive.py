from multiprocessing import Process

from dvc.repo import Repo
from dvc.visualization import embed


def summary(path: str, debug: bool = False):
    def make_summary(path):
        import os

        if not debug and os.fork() != 0:
            return

        import builtins

        from dvc.visualization import metrics_embedding, plots_embeddings

        metrics_path = path + ".json"
        assert os.path.exists(path)
        assert os.path.exists(metrics_path)

        repo = Repo(Repo.find_root())

        metrics = repo.metrics.show(targets=[metrics_path])
        metrics_html = metrics_embedding(metrics)

        plots = repo.plots.show(targets=[path], recursive=True)
        embeddigns = plots_embeddings(plots)

        parts = [metrics_html, "<br>", *embeddigns]
        html = embed(parts)
        with builtins.open(path + ".html", "w") as fd:
            fd.write(html)

    if debug:
        make_summary(path)
    else:
        p = Process(target=make_summary, args=(path,), daemon=True)
        p.start()
