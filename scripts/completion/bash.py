"""Automatically generate bash completion script"""
import io
from os import path

from dvc.cli import get_main_parser

GLOBAL_OPTIONS = ["-h", "--help", "-q", "--quiet", "-v", "--verbose"]
ROOT_PREFIX = "_dvc"
UNCOMPLETABLE_POSITIONALS = {
    "rev",
    "url",
    "args",
    "name",
    "option",
    "value",
    "command",
}


def print_bash(parser, prefix=ROOT_PREFIX, file=None):
    """Prints definitions in bash syntax for use in autocompletion scripts."""
    positionals = parser._get_positional_actions()
    commands = []

    if prefix == ROOT_PREFIX:  # skip root options
        pass
    else:
        opts = [
            opt for sub in positionals if sub.choices for opt in sub.choices
        ]
        opts += sum(
            (opt.option_strings for opt in parser._get_optional_actions()), []
        )
        # use list rather than set to maintain order
        opts = [i for i in opts if i not in GLOBAL_OPTIONS]
        opts = " ".join(opts)
        print(f"{prefix}='{opts}'", file=file)

    dest = []
    for sub in positionals:
        if sub.choices:
            for cmd in sorted(sub.choices):
                commands.append(cmd)
                print_bash(
                    sub.choices[cmd], f"{prefix}_{cmd.replace('-', '_')}", file
                )
        elif not any(i in sub.dest for i in UNCOMPLETABLE_POSITIONALS):
            dest.append(sub.dest)
    if dest:
        if not {"targets", "target"}.intersection(dest):
            print(f"{prefix}_COMPGEN=_dvc_compgen_files", file=file)
        else:
            print(f"{prefix}_COMPGEN=_dvc_compgen_DVCFiles", file=file)

    return commands


if __name__ == "__main__":
    parser = get_main_parser()
    bash = io.StringIO()
    commands = print_bash(parser, file=bash)

    print(
        """\
#!/usr/bin/env bash
# AUTOMATCALLY GENERATED from dvc/scripts/completion/bash.py
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/\
Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#   - https://stackoverflow.com/questions/12933362

_dvc_commands='"""
        + " ".join(commands)
        + """'

_dvc_options='-h --help -V --version'
_dvc_global_options='"""
        + " ".join(GLOBAL_OPTIONS)
        + """'

"""
        + bash.getvalue()
        + """
# $1=COMP_WORDS[1]
_dvc_compgen_DVCFiles() {
  compgen -f -X '!*?.dvc' -- $1
  compgen -d -S '/' -- $1  # recurse into subdirs
  # Note that the recurse into dirs is only for looking for DVC-files.
  # Since dirs themselves are not required, we need `-o nospace` at the bottom
  # unfortunately :(
}

# $1=COMP_WORDS[1]
_dvc_compgen_files() {
  compgen -f -- $1
  compgen -d -S '/' -- $1  # recurse into subdirs
}

# $1=COMP_WORDS[1]
_dvc_replace_hyphen() {
  echo $1 | sed 's/-/_/g'
}

# $1=COMP_WORDS[1]
_dvc_compgen_command() {
  local flags_list="_dvc_$(_dvc_replace_hyphen $1)"
  local args_gen="${flags_list}_COMPGEN"
  COMPREPLY=( $(compgen -W "$_dvc_global_options ${!flags_list}" -- "$word"; \
[ -n "${!args_gen}" ] && ${!args_gen} "$word") )
}

# $1=COMP_WORDS[1]
# $2=COMP_WORDS[2]
_dvc_compgen_subcommand() {
  local flags_list="_dvc_$(_dvc_replace_hyphen $1)_$(_dvc_replace_hyphen $2)"
  local args_gen="${flags_list}_COMPGEN"
  [ -n "${!args_gen}" ] && local opts_more="$(${!args_gen} "$word")"
  local opts="${!flags_list}"
  if [ -z "$opts$opts_more" ]; then
    _dvc_compgen_command $1
  else
    COMPREPLY=( $(compgen -W "$_dvc_global_options $opts" -- "$word"; \
[ -n "$opts_more" ] && echo "$opts_more") )
  fi
}

# Notes:
# `COMPREPLY` contains what will be rendered after completion is triggered
# `word` refers to the current typed word
# `${!var}` is to evaluate the content of `var`
# and expand its content as a variable
#       hello="world"
#       x="hello"
#       ${!x} ->  ${hello} ->  "world"
_dvc() {
  local word="${COMP_WORDS[COMP_CWORD]}"

  COMPREPLY=()

  if [ "${COMP_CWORD}" -eq 1 ]; then
    case "$word" in
      -*) COMPREPLY=($(compgen -W "$_dvc_options" -- "$word")) ;;
      *) COMPREPLY=($(compgen -W "$_dvc_commands" -- "$word")) ;;
    esac
  elif [ "${COMP_CWORD}" -eq 2 ]; then
    _dvc_compgen_command ${COMP_WORDS[1]}
  elif [ "${COMP_CWORD}" -ge 3 ]; then
    _dvc_compgen_subcommand ${COMP_WORDS[1]} ${COMP_WORDS[2]}
  fi

  return 0
}

complete -o nospace -F _dvc dvc""",
        file=open(path.join(path.dirname(__file__), "dvc.bash"), mode="w"),
    )
