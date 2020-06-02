#!/usr/bin/env bash
# AUTOMATCALLY GENERATED from dvc/scripts/completion/bash.py
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#   - https://stackoverflow.com/questions/12933362

_dvc_commands='add cache checkout commit completion config daemon destroy diff fetch freeze gc get get-url git-hook import import-url init install list metrics move params pipeline plots pull push remote remove repro root run status unfreeze unprotect update version'

_dvc_options='-h --help -V --version'
_dvc_global_options='-h --help -q --quiet -v --verbose'

_dvc_add='-R --recursive --no-commit -f --file'
_dvc_add_COMPGEN=_dvc_compgen_files
_dvc_cache='dir'
_dvc_cache_dir='--global --system --local -u --unset'
_dvc_cache_dir_COMPGEN=_dvc_compgen_files
_dvc_checkout='--summary -d --with-deps -R --recursive -f --force --relink'
_dvc_checkout_COMPGEN=_dvc_compgen_DVCFiles
_dvc_commit='-f --force -d --with-deps -R --recursive'
_dvc_commit_COMPGEN=_dvc_compgen_DVCFiles
_dvc_completion='-o --output'
_dvc_config='--global --system --local -u --unset'
_dvc_daemon='updater analytics'
_dvc_daemon_analytics=''
_dvc_daemon_analytics_COMPGEN=_dvc_compgen_DVCFiles #?
_dvc_daemon_updater=''
_dvc_destroy='-f --force'
_dvc_diff='--show-json --show-hash --show-md'
_dvc_fetch='-j --jobs -r --remote -a --all-branches -T --all-tags --all-commits -d --with-deps -R --recursive --run-cache'
_dvc_fetch_COMPGEN=_dvc_compgen_DVCFiles
_dvc_freeze=''
_dvc_freeze_COMPGEN=_dvc_compgen_DVCFiles
_dvc_gc='-w --workspace -a --all-branches -T --all-tags --all-commits -c --cloud -r --remote -f --force -j --jobs -p --projects'
_dvc_get='-o --out --rev --show-url'
_dvc_get_COMPGEN=_dvc_compgen_files
_dvc_get_url=''
_dvc_get_url_COMPGEN=_dvc_compgen_files
_dvc_git_hook='pre-commit post-checkout pre-push'
_dvc_git_hook_post_checkout=''
_dvc_git_hook_pre_commit=''
_dvc_git_hook_pre_push=''
_dvc_import='-o --out --rev'
_dvc_import_COMPGEN=_dvc_compgen_files
_dvc_import_url='-f --file'
_dvc_import_url_COMPGEN=_dvc_compgen_files
_dvc_init='--no-scm -f --force --subdir'
_dvc_install='--use-pre-commit-tool'
_dvc_list='-R --recursive --dvc-only --rev'
_dvc_list_COMPGEN=_dvc_compgen_files
_dvc_metrics='add show diff remove'
_dvc_metrics_add=''
_dvc_metrics_add_COMPGEN=_dvc_compgen_files
_dvc_metrics_diff='--targets -R --recursive --all --show-json --show-md --no-path --old'
_dvc_metrics_remove=''
_dvc_metrics_remove_COMPGEN=_dvc_compgen_files
_dvc_metrics_show='-a --all-branches -T --all-tags --all-commits -R --recursive --show-json'
_dvc_metrics_show_COMPGEN=_dvc_compgen_DVCFiles
_dvc_move=''
_dvc_move_COMPGEN=_dvc_compgen_files
_dvc_params='diff'
_dvc_params_diff='--all --show-json --show-md --no-path'
_dvc_pipeline='list show'
_dvc_pipeline_list=''
_dvc_pipeline_show='-c --commands -o --outs -l --locked --ascii --dot --tree'
_dvc_pipeline_show_COMPGEN=_dvc_compgen_DVCFiles
_dvc_plots='show diff modify'
_dvc_plots_diff='--targets -t --template -x -y --no-csv-header --title --xlab --ylab -o --out --show-vega'
_dvc_plots_modify='-t --template -x -y --no-csv-header --title --xlab --ylab --unset'
_dvc_plots_modify_COMPGEN=_dvc_compgen_DVCFiles #?
_dvc_plots_show='-t --template -x -y --no-csv-header --title --xlab --ylab -o --out --show-vega'
_dvc_plots_show_COMPGEN=_dvc_compgen_DVCFiles #?
_dvc_pull='-j --jobs -r --remote -a --all-branches -T --all-tags --all-commits -f --force -d --with-deps -R --recursive --run-cache'
_dvc_pull_COMPGEN=_dvc_compgen_DVCFiles
_dvc_push='-j --jobs -r --remote -a --all-branches -T --all-tags --all-commits -d --with-deps -R --recursive --run-cache'
_dvc_push_COMPGEN=_dvc_compgen_DVCFiles
_dvc_remote='add default modify list remove rename'
_dvc_remote_add='--global --system --local -d --default -f --force'
_dvc_remote_default='--global --system --local -u --unset'
_dvc_remote_list='--global --system --local'
_dvc_remote_modify='--global --system --local -u --unset'
_dvc_remote_remove='--global --system --local'
_dvc_remote_rename='--global --system --local'
_dvc_remove='-o --outs -p --purge -f --force'
_dvc_remove_COMPGEN=_dvc_compgen_DVCFiles
_dvc_repro='-f --force -s --single-item -c --cwd -m --metrics --dry -i --interactive -p --pipeline -P --all-pipelines -R --recursive --no-run-cache --force-downstream --no-commit --downstream'
_dvc_repro_COMPGEN=_dvc_compgen_DVCFiles
_dvc_root=''
_dvc_run='-d --deps -n --name -o --outs -O --outs-no-cache -p --params -m --metrics -M --metrics-no-cache --plots --plots-no-cache -f --file -w --wdir --no-exec --overwrite-dvcfile --no-run-cache --no-commit --outs-persist --outs-persist-no-cache --always-changed --single-stage'
_dvc_status='-j --jobs -c --cloud -r --remote -a --all-branches -T --all-tags --all-commits -d --with-deps -R --recursive'
_dvc_status_COMPGEN=_dvc_compgen_DVCFiles
_dvc_unfreeze=''
_dvc_unfreeze_COMPGEN=_dvc_compgen_DVCFiles
_dvc_unprotect=''
_dvc_unprotect_COMPGEN=_dvc_compgen_files
_dvc_update='--rev -R --recursive'
_dvc_update_COMPGEN=_dvc_compgen_DVCFiles
_dvc_version=''

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
  COMPREPLY=( $(compgen -W "$_dvc_global_options ${!flags_list}" -- "$word"; [ -n "${!args_gen}" ] && ${!args_gen} "$word") )
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
    COMPREPLY=( $(compgen -W "$_dvc_global_options $opts" -- "$word"; [ -n "$opts_more" ] && echo "$opts_more") )
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

complete -o nospace -F _dvc dvc
