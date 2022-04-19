import shtab

BASH_PREAMBLE = """
# $1=COMP_WORDS[1]
_dvc_compgen_DVCFiles() {
  compgen -d -S '/' -- $1  # recurse into subdirs
  compgen -f -X '!*?.dvc' -- $1
  compgen -f -X '!*Dvcfile' -- $1
  compgen -f -X '!*dvc.yaml' -- $1
}

_dvc_compgen_stages() {
    local _dvc_stages=($(dvc stage list -q --names-only))
    compgen -W "${_dvc_stages[*]}" -- $1
}
_dvc_compgen_stages_and_files() {
    _dvc_compgen_DVCFiles $1
    _dvc_compgen_stages $1
}

_dvc_compgen_exps() {
    local _dvc_exps=($(dvc exp list -q --all-commits --names-only))
    compgen -W "${_dvc_exps[*]}" -- $1
}
"""

ZSH_PREAMBLE = """
_dvc_compadd_DVCFiles() {
    _files -g '(*?.dvc|Dvcfile|dvc.yaml)'
}
_dvc_compadd_stages() {
    # this will also show up the description of the stages
    _describe 'stages' "($(_dvc_stages_output))"
}

_dvc_stages_output() {
  dvc stage list -q | awk '{
    # escape possible `:` on the stage name
    sub(/:/, "\\\\\\\\:", $1);
    # read all of the columns except the first
    # reading `out` from $2, so as not to have a leading whitespace
    out=$2; for(i=3;i<=NF;i++){out=out" "$i};
    # print key, ":" and then single-quote the description
    # colon is a delimiter used by `_describe` to separate field/description
    print $1":""\\047"out"\\047"
    # single quote -> \\047
    }'
}

_dvc_compadd_stages_and_files() {
    _dvc_compadd_DVCFiles
    _dvc_compadd_stages
}

_dvc_compadd_exps() {
    _describe 'experiments' "($(dvc exp list -q --all-commits --names-only))"
}
"""

PREAMBLE = {
    "bash": BASH_PREAMBLE,
    "zsh": ZSH_PREAMBLE,
}

FILE = shtab.FILE
DIR = shtab.DIRECTORY
DVC_FILE = {"bash": "_dvc_compgen_DVCFiles", "zsh": "_dvc_compadd_DVCFiles"}
STAGE = {"bash": "_dvc_compgen_stages", "zsh": "_dvc_compadd_stages"}
DVCFILES_AND_STAGE = {
    "bash": "_dvc_compgen_stages_and_files",
    "zsh": "_dvc_compadd_stages_and_files",
}
EXPERIMENT = {"bash": "_dvc_compgen_exps", "zsh": "_dvc_compadd_exps"}
