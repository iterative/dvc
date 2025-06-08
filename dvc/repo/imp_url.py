import os
from typing import TYPE_CHECKING

from dvc.exceptions import InvalidArgumentError, OutputDuplicationError
from dvc.repo.scm_context import scm_context
from dvc.utils import relpath, resolve_output, resolve_paths
from dvc.utils.fs import path_isin

if TYPE_CHECKING:
    from . import Repo

from . import locked


@locked
@scm_context
def imp_url(  # noqa: PLR0913
    self: "Repo",
    url,
    out=None,
    erepo=None,
    frozen=True,
    no_download=False,
    no_exec=False,
    remote=None,
    to_remote=False,
    jobs=None,
    force=False,
    fs_config=None,
    version_aware: bool = False,
):
    out = resolve_output(url, out, force=force)
    path, wdir, out = resolve_paths(self, out, always_local=to_remote and not out)

    if to_remote and (no_exec or no_download or version_aware):
        raise InvalidArgumentError(
            "--no-exec/--no-download/--version-aware cannot be combined with "
            "--to-remote"
        )

    if not to_remote and remote:
        raise InvalidArgumentError("--remote can't be used without --to-remote")

    # NOTE: when user is importing something from within their own repository
    if (
        erepo is None
        and os.path.exists(url)
        and path_isin(os.path.abspath(url), self.root_dir)
    ):
        url = relpath(url, wdir)

    if version_aware:
        if fs_config is None:
            fs_config = {}
        fs_config["version_aware"] = True

    stage = self.stage.create(
        single_stage=True,
        validate=False,
        fname=path,
        wdir=wdir,
        deps=[url],
        outs=[out],
        erepo=erepo,
        fs_config=fs_config,
    )

    try:
        self.check_graph(stages={stage})
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(  # noqa: B904
            exc.output, set(exc.stages) - {stage}
        )

    if no_exec:
        stage.ignore_outs()
    elif to_remote:
        remote_odb = self.cloud.get_remote_odb(remote, "import-url")
        stage.outs[0].transfer(url, odb=remote_odb, jobs=jobs)
        stage.outs[0].ignore()
        stage.save_deps()
        stage.md5 = stage.compute_md5()
    else:
        if stage.deps[0].fs.version_aware:
            stage.outs[0].can_push = False
        stage.run(jobs=jobs, no_download=no_download)

    stage.frozen = frozen
    stage.dump()
    return stage
