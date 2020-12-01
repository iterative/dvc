import os

from dvc.repo.scm_context import scm_context
from dvc.utils import format_link, relpath, resolve_output, resolve_paths
from dvc.utils.fs import path_isin

from ..exceptions import DvcException, OutputDuplicationError
from . import locked


@locked
@scm_context
def imp_url(
    self,
    url,
    out=None,
    fname=None,
    erepo=None,
    frozen=True,
    no_exec=False,
    desc=None,
    glob=False,
):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage, restore_meta

    orig_out = out
    out = resolve_output(url, out)
    path, wdir, out = resolve_paths(self, out)

    # NOTE: when user is importing something from within their own repository
    if (
        erepo is None
        and os.path.exists(url)
        and path_isin(os.path.abspath(url), self.root_dir)
    ):
        url = relpath(url, wdir)

    if glob:
        from glob import glob

        abs_url = os.path.join(erepo["url"], url)
        expanded_targets = [
            entry.replace(erepo["url"] + os.path.sep, "")
            for entry in glob(abs_url, recursive=True)
        ]
        expanded_targets = [
            t for t in expanded_targets if not t.endswith(".dvc")
        ]

        if len(expanded_targets) != 1:
            msg = (
                "Cannot import multiple files or directories at once"
                "See {} for more information."
            ).format(
                format_link(
                    "https://dvc.org/doc/user-guide/"
                    "troubleshooting#import-wildcard"
                )
            )
            raise DvcException(msg)

        url = expanded_targets[0]

        out_target = resolve_output(url, orig_out)
        path, wdir, out = resolve_paths(self, out_target)

    stage = create_stage(
        Stage,
        self,
        fname or path,
        wdir=wdir,
        deps=[url],
        outs=[out],
        erepo=erepo,
    )
    restore_meta(stage)
    if stage.can_be_skipped:
        return None

    if desc:
        stage.outs[0].desc = desc

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.remove()

    try:
        self.check_modified_graph([stage])
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    if no_exec:
        stage.ignore_outs()
    else:
        stage.run()

    stage.frozen = frozen

    dvcfile.dump(stage)

    return stage
