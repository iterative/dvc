from funcy import first
from pygtrie import Trie

from dvc.exceptions import OutputDuplicationError, OverlappingOutputPathsError


def build_outs_trie(stages):
    outs = Trie()

    for stage in filter(bool, stages):  # bug? not using it later
        for out in stage.outs:
            out_key = out.path_info.parts

            # Check for dup outs
            if out_key in outs:
                dup_stages = [stage, outs[out_key].stage]
                raise OutputDuplicationError(str(out), dup_stages)

            # Check for overlapping outs
            if outs.has_subtrie(out_key):
                parent = out
                overlapping = first(outs.values(prefix=out_key))
            else:
                parent = outs.shortest_prefix(out_key).value
                overlapping = out
            if parent and overlapping:
                msg = (
                    "Paths for outs:\n'{}'('{}')\n'{}'('{}')\n"
                    "overlap. To avoid unpredictable behaviour, "
                    "rerun command with non overlapping outs paths."
                ).format(
                    str(parent),
                    parent.stage.addressing,
                    str(overlapping),
                    overlapping.stage.addressing,
                )
                raise OverlappingOutputPathsError(parent, overlapping, msg)

            outs[out_key] = out

    return outs
