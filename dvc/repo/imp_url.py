import os
from typing import TYPE_CHECKING

from dvc.repo.scm_context import scm_context
from dvc.utils import relpath, resolve_output, resolve_paths
from dvc.utils.fs import path_isin

from ..exceptions import InvalidArgumentError, OutputDuplicationError
from . import locked

if TYPE_CHECKING:
    from dvc.dvcfile import DVCFile


@locked
@scm_context
def imp_url(
    self,
    url,
    out=None,
    fname=None,
    erepo=None,
    frozen=True,
    no_download=False,
    no_exec=False,
    remote=None,
    to_remote=False,
    desc=None,
    type=None,  # pylint: disable=redefined-builtin
    labels=None,
    meta=None,
    jobs=None,
    fs_config=None,
    version_aware: bool = False,
):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage, restore_fields

    out = resolve_output(url, out)
    path, wdir, out = resolve_paths(
        self, out, always_local=to_remote and not out
    )

    if to_remote and (no_exec or no_download):
        raise InvalidArgumentError(
            "--no-exec/--no-download cannot be combined with --to-remote"
        )

    if not to_remote and remote:
        raise InvalidArgumentError(
            "--remote can't be used without --to-remote"
        )

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

    stage = create_stage(
        Stage,
        self,
        fname or path,
        wdir=wdir,
        deps=[url],
        outs=[out],
        erepo=erepo,
        fs_config=fs_config,
    )
    restore_fields(stage)

    out_obj = stage.outs[0]
    out_obj.annot.update(desc=desc, type=type, labels=labels, meta=meta)
    dvcfile: "DVCFile" = Dvcfile(self, stage.path)  # type: ignore
    dvcfile.remove()

    try:
        new_index = self.index.add(stage)
        new_index.check_graph()
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    if no_exec:
        stage.ignore_outs()
    elif to_remote:
        remote_odb = self.cloud.get_remote_odb(remote, "import-url")
        stage.outs[0].transfer(url, odb=remote_odb, jobs=jobs)
        stage.save_deps()
        stage.md5 = stage.compute_md5()
    else:
        stage.run(jobs=jobs, no_download=no_download)

    stage.frozen = frozen

    dvcfile.dump(stage)

    return stage
