#!/usr/bin/env bash

#----------------------------------------------------------
# Repository:  https://github.com/iterative/dvc
#
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#----------------------------------------------------------

_dvc_commands='add cache checkout commit config destroy diff fetch get-url get gc \
  import-url import init install lock list metrics move pipeline pull push \
  remote remove repro root run status unlock unprotect update version'

_dvc_options='-h --help -V --version'
_dvc_global_options='-h --help -q --quiet -v --verbose'

_dvc_add='-R --recursive -f --file --no-commit $(compgen -f)'
_dvc_cache='dir'
_dvc_cache_dir=' --global --system --local -u --unset'
_dvc_checkout='-d --with-deps -R --recursive -f --force --relink $(compgen -f -X \!*?.dvc)'
_dvc_commit='-f --force -d --with-deps -R --recursive $(compgen -f -X \!*?.dvc)'
_dvc_config='-u --unset --local --system --global'
_dvc_destroy='-f --force'
_dvc_diff='-t --show-json --checksums'
_dvc_fetch='-j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive $(compgen -f -X \!*?.dvc)'
_dvc_get_url=''
_dvc_get='-o --out --rev --show-url'
_dvc_gc='-a --all-branches -T --all-tags -c --cloud -r --remote -f --force -p --projects -j --jobs'
_dvc_import_url='-f --file'
_dvc_import='-o --out --rev'
_dvc_init='--no-scm -f --force'
_dvc_install=''
_dvc_list='-R --recursive --outs-only --rev $(compgen -f)'
_dvc_lock='$(compgen -f -X \!*?.dvc)'
_dvc_metrics='add modify rmeove show'
_dvc_metrics_add='-t --type -x --xpath $(compgen -f)'
_dvc_metrics_show='$(-t --type -x --xpath -a --all-branches -T --all-tags -R --recursive $(compgen -f)'
_dvc_metrics_diff='--targets -t --type -x --xpath -R --show-json'
_dvc_metrics_modify='-t --type -x --xpath $(compgen -f)'
_dvc_metrics_remove='$(compgen -f)'
_dvc_move='$(compgen -f)'
_dvc_pipeline='list show'
_dvc_pipeline_list=''
_dvc_pipeline_show='-c --commands -o --outs --ascii --dot --tree -l --locked $(compgen -f -X \!*?.dvc)'
_dvc_pull='-j --jobs -r --remote -a --all-branches -T --all-tags -f --force -d --with-deps -R --recursive $(compgen -f -X \!*?.dvc)'
_dvc_push='-j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive $(compgen -f -X \!*?.dvc)'
_dvc_remote='add default list modify remove'
_dvc_remote_add='--global --system --local -d --default -f --force'
_dvc_remote_default='--global --system --local -u --unset'
_dvc_remote_list='--global --system --local'
_dvc_remote_modify='--global --system --local -u --unset'
_dvc_remote_remove='--global --system --local'
_dvc_remove='-o --outs -p --purge -f --force $(compgen -f -X \!*?.dvc)'
_dvc_repro='-f --force -s --single-item -c --cwd -m --metrics --dry -i --interactive -p --pipeline -P --all-pipelines --ignore-build-cache --no-commit -R --recursive --downstream $(compgen -f -X \!*?.dvc)'
_dvc_root=''
_dvc_run='--no-exec -f --file -c --cwd -d --deps -o --outs -O --outs-no-cache --outs-persist --outs-persist-no-cache -m --metrics -M --metrics-no-cache -y --yes --overwrite-dvcfile --ignore-build-cache --remove-outs --no-commit -w --wdir'
_dvc_status='-j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -c --cloud $(compgen -f -X \!*?.dvc)'
_dvc_unlock='$(compgen -f -X \!*?.dvc)'
_dvc_unprotect='$(compgen -f)'
_dvc_update='--rev $(compgen -f -X \!*?.dvc)'
_dvc_version=''

# Params
# $1 - COMP_WORDS[1]
_dvc_replace_hyphen() {
  echo $1 | sed 's/-/_/g'
}

# Params
# $1 - COMP_WORDS[1]
_dvc_comp_command() {
  local options_list="_dvc_$(_dvc_replace_hyphen $1)"

  COMPREPLY=( $(compgen -W "$_dvc_global_options ${!options_list}" -- "$word") )
}

# Params
# $1 - COMP_WORDS[1]
# $1 - COMP_WORDS[2]
_dvc_comp_subcommand() {
  local options_list="_dvc_$(_dvc_replace_hyphen $1)_$(_dvc_replace_hyphen $2)"
  local _dvc_opts="${!options_list}"
  if [ -z "$_dvc_opts" ]; then
    _dvc_comp_command $1
  else
    COMPREPLY=( $(compgen -W "$_dvc_global_options $_dvc_opts" -- "$word") )
  fi
}

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
_dvc() {
  local word="${COMP_WORDS[COMP_CWORD]}"

  COMPREPLY=()

  if [ "${COMP_CWORD}" -eq 1 ]; then
    case "$word" in
      -*) COMPREPLY=($(compgen -W "$_dvc_options" -- "$word")) ;;
      *) COMPREPLY=($(compgen -W "$_dvc_commands" -- "$word")) ;;
    esac
  elif [ "${COMP_CWORD}" -eq 2 ]; then
    _dvc_comp_command ${COMP_WORDS[1]}
  elif [ "${COMP_CWORD}" -eq 3 ]; then
    _dvc_comp_subcommand ${COMP_WORDS[1]} ${COMP_WORDS[2]}
  fi

  return 0
}

complete -F _dvc dvc
