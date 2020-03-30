import logging

from dvc.visualization import Default1DArrayTemplate

logger = logging.getLogger(__name__)


def viz(repo, targets, typ="csv", viz_template=None):
    if typ == "csv":
        import csv

        with open(targets[0], "r") as fd:
            rdr = csv.reader(fd, delimiter=",")
            lines = list(rdr)
            assert len(lines) == 1
        values = lines[0]

    Default1DArrayTemplate(repo.dvc_dir).save(values)
