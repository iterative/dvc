#!/usr/bin/env bash

#----------------------------------------------------------
# Repository:  https://github.com/iterative/dvc
#
# References:
#   - https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html
#   - https://opensource.com/article/18/3/creating-bash-completion-script
#----------------------------------------------------------

<<<<<<< HEAD
_dvc_commands='init get get_url destroy add remove move unprotect run \
              repro pull push fetch status gc import import_url config \
              checkout remote cache metrics install root lock unlock \
              pipeline commit diff version'
||||||| merged common ancestors
_dvc_commands='init destroy add import checkout run pull push fetch \
              status repro remove move unprotect gc config remote metrics \
              install root lock unlock pipeline commit'
=======
_dvc_commands='add checkout commit config destroy fetch get-url get gc \
              import-url import init install lock metrics move pipeline pull push \
              remote remove repro root run status unlock unprotect'
>>>>>>> a0233ce4a9920cda18f4a10948f74820cd7bc624

_dvc_options="-h --help -V --version"
_dvc_global_options="-h --help -q --quiet -v --verbose"

<<<<<<< HEAD
_dvc_add="-R --recursive -f --file --no-commit $(compgen -G '*')"
_dvc_cache=""
_dvc_checkout="-d --with-deps -R --recursive -f --force $(compgen -G '*.dvc')"
_dvc_commit="-d --with-deps -R --recursive -f --force $(compgen -G '*.dvc')"
||||||| merged common ancestors
_dvc_add="-R --recursive -f --file --no-commit"
_dvc_checkout="$(compgen -G '*.dvc')"
=======
_dvc_add="-R --recursive -f --file --no-commit"
_dvc_checkout="$(compgen -G '*.dvc')"
_dvc_commit=""
>>>>>>> a0233ce4a9920cda18f4a10948f74820cd7bc624
_dvc_config="-u --unset --local --system --global"
_dvc_destroy="-f --force"
<<<<<<< HEAD
_dvc_diff="-t --target"
_dvc_fetch="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive $(compgen -G '*.dvc')"
_dvc_gc="-a --all-branches -T --all-tags -c --cloud -r --remote -f --force -p --projects -j --jobs"
_dvc_get="-o --out --rev"
_dvc_get-url=""
_dvc_import="-o --out --rev"
_dvc_import-url="--resume -f --file"
||||||| merged common ancestors
_dvc_fetch="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive"
_dvc_gc="-a --all-branches -T --all-tags -c --cloud -r --remote -f --force -p --project"
_dvc_import="--resume"
=======
_dvc_fetch="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive"
_dvc_get-url=""
_dvc_get="-o --out --rev"
_dvc_gc="-a --all-branches -T --all-tags -c --cloud -r --remote -f --force -p --project"
_dvc_import-url="--resume -f --file"
_dvc_import="-o --out --rev"
>>>>>>> a0233ce4a9920cda18f4a10948f74820cd7bc624
_dvc_init="--no-scm -f --force"
_dvc_install=""
_dvc_lock="$(compgen -G '*.dvc')"
_dvc_metrics=""
_dvc_move="$(compgen -G '*')"
_dvc_pipeline=""
_dvc_pull="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -f --force -d --with-deps -R --recursive $(compgen -G '*.dvc')"
_dvc_push="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -R --recursive $(compgen -G '*.dvc')"
_dvc_remote=""
_dvc_remove="-o --outs -p --purge -f --force $(compgen -G '*.dvc')"
_dvc_repro="-f --force -s --single-item -c --cwd -m --metrics --dry -i --interactive -p --pipeline -P --all-pipelines --ignore-build-cache --no-commit -R --recursive --downstream"
_dvc_root=""
_dvc_run="--no-exec -f --file -c --cwd -d --deps -o --outs -O --outs-no-cache --outs-persist --outs-persist-no-cache -m --metrics -M --metrics-no-cache -y --yes --overwrite-dvcfile --ignore-build-cache --remove-outs --no-commit -w --wdir"
_dvc_status="--show-checksums -j --jobs -r --remote -a --all-branches -T --all-tags -d --with-deps -c --cloud $(compgen -G '*.dvc')"
_dvc_unlock="$(compgen -G '*.dvc')"
<<<<<<< HEAD
_dvc_unprotect="$(compgen -G '*.dvc')"
_dvc_version=""
||||||| merged common ancestors
=======
_dvc_unprotect=""
>>>>>>> a0233ce4a9920cda18f4a10948f74820cd7bc624

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
