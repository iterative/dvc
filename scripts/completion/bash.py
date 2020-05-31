"""Automatically generate bash completion script"""
import io
from os import path

from dvc.cli import get_main_parser

GLOBAL_OPTIONS = ["-h", "--help", "-q", "--quiet", "-v", "--verbose"]
ROOT_PREFIX = "_dvc"


def print_parser(parser, prefix=ROOT_PREFIX, file=None):
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
            for opt in sorted(sub.choices):
                if prefix == ROOT_PREFIX:
                    commands.append(opt)
                print_parser(
                    sub.choices[opt], f"{prefix}_{opt.replace('-', '_')}", file
                )
        elif not any(
            i in sub.dest.lower()
            for i in ("rev", "url", "args", "name", "command")
        ):  # "commit", "sha", "hash"
            dest.append(sub.dest)
    if dest:
        if "targets" in dest or "target" in dest:
            print(f"{prefix}_COMPGEN=_dvc_compgen_DVCFiles", file=file)
        elif prefix not in (
            "_dvc_config",
            "_dvc_remote_modify",
            "_dvc_remote_rename",
        ):
            print(f"{prefix}_COMPGEN=_dvc_compgen_files", file=file)

    return commands


if __name__ == "__main__":
    output = io.StringIO()
    commands = print_parser(get_main_parser(), file=output)

    print(
        """\
#!/usr/bin/env bash
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/\
Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#   - https://stackoverflow.com/questions/12933362

_dvc_commands='"""
        + " ".join(sorted(commands))
        + """'

_dvc_options='-h --help -V --version'
_dvc_global_options='"""
        + " ".join(GLOBAL_OPTIONS)
        + """'

"""
        + output.getvalue()
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
