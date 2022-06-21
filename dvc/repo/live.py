import logging
from typing import TYPE_CHECKING, Optional

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dvc.output import Output


def summary_fs_path(out: "Output") -> Optional[str]:
    from dvc.output import Output

    assert out.live
    has_summary = True
    if isinstance(out.live, dict):
        has_summary = out.live.get(Output.PARAM_LIVE_SUMMARY, True)
    if has_summary:
        return out.fs.path.with_suffix(out.fs_path, ".json")
    return None
