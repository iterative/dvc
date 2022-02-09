from dvc.ignore import destroy as destroy_dvcignore
from dvc.utils.fs import remove

from . import locked


@locked
def _destroy_stages(repo):
    for stage in repo.index.stages:
        stage.unprotect_outs()
        stage.dvcfile.remove(force=True)


# NOTE: not locking `destroy`, as `remove` will need to delete `.dvc` dir,
# which will cause issues on Windows, as `.dvc/lock` will be busy.
def destroy(repo):
    _destroy_stages(repo)
    repo.close()
    destroy_dvcignore(repo.root_dir)
    remove(repo.dvc_dir)
