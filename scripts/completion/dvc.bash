#!/usr/bin/env bash

#----------------------------------------------------------
# Repository:  https://github.com/iterative/dvc
#
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#----------------------------------------------------------

_dvc_commands='add checkout commit config destroy fetch get-url get gc \
              import-url import init install lock metrics move pipeline pull push \
              remote remove repro root run status unlock unprotect'

_dvc_options="-h --help -v --version"
_dvc_global_options="-h --help -q --quiet -V --verbose"

_dvc_add="-R --recursive -f --file --no-commit"
_dvc_checkout="$(compgen -G '*.dvc')"
_dvc_commit=""
_dvc_config="-u --unset --local --system --global"
_dvc_destroy="-f --force"
_dvc_fetch="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive"
_dvc_get-url=""
_dvc_get="-o --out --rev"
_dvc_gc="-a --all-branches -T --all-tags -c --cloud -r --remote -f --force -p --project"
_dvc_import-url="--resume -f --file"
_dvc_import="-o --out --rev"
_dvc_init="--no-scm -f --force"
_dvc_install=""
_dvc_lock="$(compgen -G '*.dvc')"
_dvc_metrics=""
_dvc_move="$(compgen -G '*')"
_dvc_pipeline=""
_dvc_pull="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -f --force -R --recursive"
_dvc_push="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive"
_dvc_remote=""
_dvc_remove="--dry -o --outs -p --purge --force"
_dvc_repro="--dry -f --force -s --single-item -c --cwd -m --metrics -i --interactive -p --pipeline -P --all-pipelines --ignore-build-cache --no-commit -s --single-item"
_dvc_root=""
_dvc_run="--no-exec -f --file -c --cwd -d --deps -o --outs -O --outs-no-cache -M --metrics-no-cache -y --yes --overwrite-dvc-file --ignore-build-cache --remove-outs --no-commit -w --wdir"
_dvc_status="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -c --cloud -q --quiet"
_dvc_unlock="$(compgen -G '*.dvc')"
_dvc_unprotect=""

# Notes:
#
# `COMPREPLY` contains what will be rendered after completion is triggered
#
# `word` refers to the current typed word
#
# `${!var}` is to evaluate the content of `var` and expand its content as a variable
#
#       hello="world"
#       x="hello"
#       ${!x} ->  ${hello} ->  "world"
#
_dvc () {
  local word="${COMP_WORDS[COMP_CWORD]}"

  COMPREPLY=()

  if [ "${COMP_CWORD}" -eq 1 ]; then
    case "$word" in
      -*) COMPREPLY=($(compgen -W "$_dvc_options" -- "$word")) ;;
      *)  COMPREPLY=($(compgen -W "$_dvc_commands" -- "$word")) ;;
    esac
  elif [ "${COMP_CWORD}" -eq 2 ]; then
    local options_list="_dvc_${COMP_WORDS[1]}"

    COMPREPLY=($(compgen -W "$_dvc_global_options ${!options_list}" -- "$word"))
  fi

  return 0
}

complete -F _dvc dvc
