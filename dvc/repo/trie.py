from funcy import first
from pygtrie import Trie

from dvc.exceptions import OutputDuplicationError, OverlappingOutputPathsError


def build_outs_trie(stages):
    outs = Trie()

    for stage in stages:
        for out in stage.outs:
            out_key = out.fs.parts(out.fs_path)

            # Check for dup outs
            if out_key in outs:
                dup_stages = [stage, outs[out_key].stage]
                raise OutputDuplicationError(str(out), set(dup_stages))

            # Check for overlapping outs
            if outs.has_subtrie(out_key):
                parent = out
                overlapping = first(outs.values(prefix=out_key))
            else:
                parent = outs.shortest_prefix(out_key).value
                overlapping = out
            if parent and overlapping:
                msg = (
                    f"The output paths:\n'{parent!s}'('{parent.stage.addressing}')\n"
                    f"'{overlapping!s}'('{overlapping.stage.addressing}')\n"
                    "overlap and are thus in the same tracked directory.\n"
                    "To keep reproducibility, outputs should be in separate "
                    "tracked directories or tracked individually."
                )
                raise OverlappingOutputPathsError(parent, overlapping, msg)

            outs[out_key] = out

    return outs
