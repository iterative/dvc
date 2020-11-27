import os
from typing import List

from dvc.exceptions import NotDvcRepoError
from dvc.repo import Repo
from dvc.visualization import embed, metrics_embedding, plots_embeddings


def summary(path: str, revs: List[str] = None):
    if revs:
        revs = ["workspace", *revs]

    metrics_path = path + ".json"
    assert os.path.exists(path)
    assert os.path.exists(metrics_path)

    try:
        root = Repo.find_root()
    except NotDvcRepoError:
        root = os.getcwd()

    repo = Repo(root_dir=root, uninitialized=True)

    metrics = repo.metrics.show(targets=[metrics_path])
    metrics_html = metrics_embedding(metrics)

    plots = repo.plots.show(targets=[path], recursive=True, revs=revs)
    embeddigns = plots_embeddings(plots)

    parts = [metrics_html, "<br>", *embeddigns]
    html = embed(parts)

    with open(path + ".html", "w") as fd:
        fd.write(html)
