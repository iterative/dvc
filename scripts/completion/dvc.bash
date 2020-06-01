#!/usr/bin/env bash
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#   - https://stackoverflow.com/questions/12933362

_dvc_commands='add cache checkout commit config dag destroy diff fetch freeze \
  get-url get gc import-url import init install list metrics move params \
  plots pull push remote remove repro root run status unfreeze unprotect \
  update version'

_dvc_options='-h --help -V --version'
_dvc_global_options='-h --help -q --quiet -v --verbose'

_dvc_add='-R --recursive -f --file --no-commit'
_dvc_add_COMPGEN=_dvc_compgen_files
_dvc_cache='dir'
_dvc_cache_dir=' --global --system --local -u --unset'
_dvc_checkout='-d --with-deps -R --recursive -f --force --relink --summary'
_dvc_checkout_COMPGEN=_dvc_compgen_DVCFiles
_dvc_commit='-f --force -d --with-deps -R --recursive'
_dvc_commit_COMPGEN=_dvc_compgen_DVCFiles
_dvc_config='-u --unset --local --system --global'
_dvc_dag='--dot --full'
_dvc_dag_COMPGEN=_dvc_compgen_DVCFiles
_dvc_destroy='-f --force'
_dvc_diff='-t --show-json --show-hash --show-md'
_dvc_fetch='-j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive'
_dvc_fetch_COMPGEN=_dvc_compgen_DVCFiles
_dvc_gc='-a --all-branches --all-commits -T --all-tags -w --workspace -c --cloud -r --remote -f --force -p --projects -j --jobs'
_dvc_get='-o --out --rev --show-url'
_dvc_get_url=''
_dvc_import='-o --out --rev'
_dvc_import_url='-f --file'
_dvc_init='--no-scm -f --force'
_dvc_install=''
_dvc_list='-R --recursive --dvc-only --rev'
_dvc_list_COMPGEN=_dvc_compgen_files
_dvc_freeze=''
_dvc_freeze_COMPGEN=_dvc_compgen_DVCFiles
_dvc_metrics='diff show'
_dvc_metrics_diff='--targets -t --type -x --xpath -R --show-json --show-md --no-path --old'
_dvc_metrics_show='-t --type -x --xpath -a --all-branches -T --all-tags -R --recursive'
_dvc_metrics_show_COMPGEN=_dvc_compgen_files
_dvc_move=''
_dvc_move_COMPGEN=_dvc_compgen_files
_dvc_params='diff'
_dvc_params_diff='--all --show-json --show-md --no-path'
_dvc_plots='show diff'
_dvc_plots_show='-t --template -o --out -x -y --show-json --no-csv-header --title --xlab --ylab'
_dvc_plots_diff='-t --template --targets -o --out -x -y --show-json --no-csv-header --title --xlab --ylab'
_dvc_pull='-j --jobs -r --remote -a --all-branches -T --all-tags -f --force -d --with-deps -R --recursive'
_dvc_pull_COMPGEN=_dvc_compgen_DVCFiles
_dvc_push='-j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive'
_dvc_push_COMPGEN=_dvc_compgen_DVCFiles
_dvc_remote='add default list modify remove rename'
_dvc_remote_add='--global --system --local -d --default -f --force'
_dvc_remote_default='--global --system --local -u --unset'
_dvc_remote_list='--global --system --local'
_dvc_remote_modify='--global --system --local -u --unset'
_dvc_remote_remove='--global --system --local'
_dvc_remote_rename='--global --system --local'
_dvc_remove='-o --outs -p --purge -f --force'
_dvc_remove_COMPGEN=_dvc_compgen_DVCFiles
_dvc_repro='-f --force -s --single-item -c --cwd -m --metrics --dry -i --interactive -p --pipeline -P --all-pipelines --no-run-cache --force-downstream --no-commit -R --recursive --downstream'
_dvc_repro_COMPGEN=_dvc_compgen_DVCFiles
_dvc_root=''
_dvc_run='--no-exec -f --file -d --deps -o --outs -O --outs-no-cache --outs-persist --outs-persist-no-cache -m --metrics -M --metrics-no-cache --overwrite-dvcfile --no-run-cache --no-commit -w --wdir'
_dvc_run_COMPGEN=_dvc_compgen_DVCFiles
_dvc_status='-j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -c --cloud'
_dvc_status_COMPGEN=_dvc_compgen_DVCFiles
_dvc_unfreeze_COMPGEN=_dvc_compgen_DVCFiles
_dvc_unprotect_COMPGEN=_dvc_compgen_files
_dvc_update='--rev'
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
# `${!var}` is to evaluate the content of `var` and expand its content as a variable
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
