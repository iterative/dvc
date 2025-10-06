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

_dvc_compgen_remotes() {
    local _dvc_remotes=($(dvc remote list | cut -d' ' -f1))
    compgen -W "${_dvc_remotes[*]}" -- $1
}

_dvc_compgen_config_vars() {
    compgen -W "${_dvc_config_vars[*]}" -- $1
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

_dvc_compadd_remotes() {
    _describe 'remotes' "($(dvc remote list | cut -d' ' -f1))"
}

_dvc_compadd_config_vars() {
    _describe 'config_vars' _dvc_config_vars
}
"""

FISH_PREAMBLE = """
function __fish_complete_dvc_files
    __fish_complete_path | string match -re '\\*?.dvc|Dvcfile|dvc\\.yaml'
end

function __fish_complete_dvc_stages
    for line in (dvc stage list -q)
        set -l parts (string split -m1 ' ' -- $line)
        set -l name $parts[1]
        set -l desc (string trim $parts[2])
        echo -e "$name	$desc"
    end
end

function __fish_complete_dvc_stages_and_files
    __fish_complete_dvc_stages
    __fish_complete_dvc_files
end

function __fish_complete_dvc_experiments
    dvc exp list -q --all-commits --names-only
end

function __fish_complete_dvc_remotes
    dvc remote list | cut -d' ' -f1
end
"""

PREAMBLE = {
    "bash": BASH_PREAMBLE,
    "zsh": ZSH_PREAMBLE,
    "fish": FISH_PREAMBLE,
}

FILE = shtab.FILE
DIR = shtab.DIRECTORY
DVC_FILE = {
    "bash": "_dvc_compgen_DVCFiles",
    "zsh": "_dvc_compadd_DVCFiles",
    "fish": "__fish_complete_dvc_files",
}
STAGE = {
    "bash": "_dvc_compgen_stages",
    "zsh": "_dvc_compadd_stages",
    "fish": "__fish_complete_dvc_stages",
}
DVCFILES_AND_STAGE = {
    "bash": "_dvc_compgen_stages_and_files",
    "zsh": "_dvc_compadd_stages_and_files",
    "fish": "__fish_complete_dvc_stages_and_files",
}
EXPERIMENT = {
    "bash": "_dvc_compgen_exps",
    "zsh": "_dvc_compadd_exps",
    "fish": "__fish_complete_dvc_experiments",
}
REMOTE = {
    "bash": "_dvc_compgen_remotes",
    "zsh": "_dvc_compadd_remotes",
    "fish": "__fish_complete_dvc_remotes",
}
CONFIG_VARS = {
    "bash": "_dvc_compgen_config_vars",
    "zsh": "_dvc_compadd_config_vars",
    "fish": "__fish_complete_dvc_config_vars",
}


def get_preamble() -> dict[str, str]:
    from dvc.config_schema import config_vars_for_completion

    ret: dict[str, str] = {}
    config_vars = list(config_vars_for_completion())

    nl = "\n\t".expandtabs(4)
    config_vars_arr = f"""
_dvc_config_vars=(
    {nl.join(config_vars)}
)
"""
    indent = "\t\t".expandtabs(4)  # 8 spaces
    lines = (
        "\n".join(
            f"{indent}{c} \\"
            for c in config_vars[:-1]  # all but last
        )
        + "\n"
        + f"{indent}{config_vars[-1]}"
    )  # last line without backslash
    config_vars_arr_fish = f"""
function __fish_complete_dvc_config_vars
    set -l _dvc_config_vars \\
{lines}
    printf %s\\n $_dvc_config_vars
end
"""
    for shell, preamble in PREAMBLE.items():
        if shell != "fish":
            ret[shell] = config_vars_arr + preamble
        else:
            ret[shell] = config_vars_arr_fish + preamble
    return ret
