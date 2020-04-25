from dvc.dvcfile import Dvcfile
from dvc.exceptions import DvcException
from dvc.repo import locked


@locked
def modify(repo, path, delete=False):
    outs = repo.find_outs_by_path(path)
    assert len(outs) == 1
    out = outs[0]

    if out.scheme != "local":
        msg = "output '{}' scheme '{}' is not supported for metrics"
        raise DvcException(msg.format(out.path, out.path_info.scheme))

    if delete:
        out.metric = None

    out.verify_metric()

    dvcfile = Dvcfile(repo, out.stage.path)
    dvcfile.dump(out.stage)
