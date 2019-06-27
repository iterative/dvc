#!/usr/bin/env bash

#----------------------------------------------------------
# Repository:  https://github.com/iterative/dvc
#
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#----------------------------------------------------------

_dvc_commands='add base cache checkout commit config daemon data_sync diff \
              destroy gc get get_url imp imp_url __init__ init install lock \
              metrics move pipeline remote remove repro root run status tag \
              unprotect version'

_dvc_options="-h --help -v --version"
_dvc_global_options="-h --help -q --quiet -v --verbose"

_dvc_add="-R --recursive -f --file --no-commit $(compgen -G '*')"
_dvc_cache=""
_dvc_cache_dir="--global --system --local -u --unset"
_dvc_checkout="-d --with-deps -R --recursive -f --force $(compgen -G '*.dvc')"
_dvc_commit="-d --with-deps -R --recursive -f --force $(compgen -G '*.dvc')"
_dvc_config="-u --unset --local --system --global"
_dvc_destroy="-f --force"
_dvc_diff="-t --target"
_dvc_fetch="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive $(compgen -G '*.dvc')"
_dvc_gc="-a --all-branches -T --all-tags -c --cloud -r --remote -f --force -p --projects -j --jobs"
_dvc_import="--resume -f --file"
_dvc_init="--no-scm -f --force"
_dvc_install=""
_dvc_lock="$(compgen -G '*.dvc')"
_dvc_metrics_add="-t --type -x --xpath $(compgen -G '*')"
_dvc_metrics=""
_dvc_metrics_modify="-t --type -x --xpath $(compgen -G '*')"
_dvc_metrics_remove="$(compgen -G '*')"
_dvc_metrics_show="-t --type -x --xpath -a --all-branches -T --all-tags -R --recursive $(compgen -G '*')"
_dvc_move="$(compgen -G '*')"
_dvc_pipeline_list=""
_dvc_pipeline=""
_dvc_pipeline_show="-c --commands -o --outs --ascii --dot --tree -l --locked $(compgen -G '*.dvc')" 
_dvc_pull="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -f --force -R --recursive $(compgen -G '*.dvc')"
_dvc_push="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive $(compgen -G '*.dvc')"
_dvc_remote_add="--global --system --local -d -default -f --force"
_dvc_remote_default="--global --system --local -u --unset"
_dvc_remote_list="--global --system --local"
_dvc_remote=""
_dvc_remote_modify="--global --system --local -u --unset"
_dvc_remote_remove="--global --system --local"
_dvc_remove="-o --outs -p --purge -f --force $(compgen -G '*.dvc')"
_dvc_repro="--dry -f --force -s --single-item -c --cwd -m --metrics -i --interactive -p --pipeline --ignore-build-cache --no-commit -s --single-item --downstream"
_dvc_root=""
_dvc_run="--no-exec -f --file -c --cwd -d --deps -o --outs -O --outs-no-cache -m metrics -M --metrics-no-cache -y --yes --overwrite-dvcfile --ignore-build-cache --remove-outs --no-commit -w --wdir"
_dvc_status="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -c --cloud $(compgen -G '*.dvc')"
_dvc_unlock="$(compgen -G '*.dvc')"
_dvc_unprotect="$(compgen -G '*.dvc')"
_dvc_version=""

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
