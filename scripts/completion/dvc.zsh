#compdef dvc

#----------------------------------------------------------
# Repository:  https://github.com/iterative/dvc
#
# References:
#   - https://github.com/zsh-users/zsh-completions
#   - http://zsh.sourceforge.net/Doc/Release/Completion-System.html
#   - https://mads-hartmann.com/2017/08/06/writing-zsh-completion-scripts.html
#   - http://www.linux-mag.com/id/1106/
#----------------------------------------------------------

_dvc_commands() {
  local _commands=(
    "init:Initialize dvc over a directory (should already be a git dir)."
    "destroy:Destroy dvc. Will remove all project's information, data files and cache."
    "add:Add files/directories to dvc."
    "import:Import files from URL."
    "checkout:Checkout data files from cache."
    "run:Generate a stage file from a given command and execute the command."
    "pull:Pull data files from the cloud."
    "push:Push data files to the cloud."
    "fetch:Fetch data files from the cloud."
    "status:Show the project status."
    "repro:Reproduce DVC file. Default file name - 'Dvcfile'."
    "remove:Remove outputs of DVC file."
    "move:Move output of DVC file."
    "gc:Collect garbage."
    "config:Get or set config options."
    "remote:Manage set of tracked repositories."
    "metrics:Get metrics from all branches."
    "install:Install dvc hooks into the repository."
    "root:Relative path to project's directory."
    "lock:Lock DVC file."
    "unlock:Unlock DVC file."
    "pipeline:Manage pipeline."
  )

  _describe 'dvc commands' _commands
}

_dvc_global_options=(
  "(-)"{-h,--help}"[Show help message related to the command]"
  "(-)"{-q,--quiet}"[Be quiet.]"
  "(-)"{-V,--verbose}"[Be verbose.]"
)

_dvc_options=(
  "(-)"{-h,--help}"[Show this help message and exit]"
  "(-)"{-v,--version}"[Show program's version]"
)

_dvc_init=(
  "--no-scm[Initiate dvc in directory that is not tracket by any scm tool]"
  {-f,--force}"[Overwrite '.dvc' if it exists. Will remove all local cache.]"
)

_dvc_destroy=(
  {-f,--force}"[Overwrite '.dvc' if it exists. Will remove all local cache.]"
)

_dvc_add=(
  {-R,--recursive}"[Recursively add each file under the directory.]"
  "1:File:_files"
)

_dvc_import=(
  "1:URL:"
  "2:Output:"
)

_dvc_checkout=(
  "1:DVC files:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_run=(
  "--no-exec[Only create stage file without actually running it.]"
  {-f,--file}"[Specify name of the stage file.]:File:_files"
  {-c,--cwd}"[Directory to run your command and place state file in.]:CWD:_files -/"
  "*"{-d,--deps}"[Declare dependencies for reproducible cmd.]:Dependency:_files"
  "*"{-o,--outs}"[Declare output data file or data directory.]:Output data:_files"
  "*"{-O,--outs-no-cache}"[Declare output regular file or directory.]:Output regular:_files"
  "*"{-M,--metrics-no-cache}"[Declare output metric file or directory]:Metrics (no cache):_files"
  {-y,--yes}"[Automatic 'yes' answer to all prompts.]"
)

_dvc_pull=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
  "--show-checksums[Show checksums instead of file names]"
  {-j,--jobs}"[Number of jobs to run simultaneously]:Number of jobs:"
  {-r,--remote}"[Remote repository to pull from]:Remote repository:"
  {-a,--all-branches}"[Fetch cache for all branches]"
  {-T,--all-tags}"[Fetch cache for all tags]"
  {-d,--with-deps}"[Fetch cache for all dependencies of the specified target]"
)

_dvc_push=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
  "--show-checksums[Show checksums instead of file names]"
  {-j,--jobs}"[Number of jobs to run simultaneously]:Number of jobs:"
  {-r,--remote}"[Remote repository to pull from]:Remote repository:"
  {-a,--all-branches}"[Fetch cache for all branches]"
  {-T,--all-tags}"[Fetch cache for all tags]"
  {-d,--with-deps}"[Fetch cache for all dependencies of the specified target]"
)

_dvc_fetch=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
  "--show-checksums[Show checksums instead of file names]"
  {-j,--jobs}"[Number of jobs to run simultaneously]:Number of jobs:"
  {-r,--remote}"[Remote repository to pull from]:Remote repository:"
  {-a,--all-branches}"[Fetch cache for all branches]"
  {-T,--all-tags}"[Fetch cache for all tags]"
  {-d,--with-deps}"[Fetch cache for all dependencies of the specified target]"
)

_dvc_status=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
  "--show-checksums[Show checksums instead of file names]"
  {-j,--jobs}"[Number of jobs to run simultaneously]:Number of jobs:"
  {-r,--remote}"[Remote repository to pull from]:Remote repository:"
  {-a,--all-branches}"[Fetch cache for all branches]"
  {-T,--all-tags}"[Fetch cache for all tags]"
  {-d,--with-deps}"[Fetch cache for all dependencies of the specified target]"
  {-c,--cloud}"[Show status of a local cache compared to a remote repository]"
)

_dvc_repro=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
  "--dry[Only print the commands that would be executed without actually executing]"
  {-f,--force}"[Reproduce even if dependencies were not changed.]"
  {-s,--single-item}"[Reproduce only single data item without recursive dependencies check.]"
  {-c,--cwd}"[Directory to reproduce from]:CWD:_files -/"
  {-m,--metrics}"[Show metrics after reproduction.]"
  {-i,--interactive}"[Ask for confirmation before reproducing each stage.]"
  {-p,--pipeline}"[Reproduce the whole pipeline that the specified stage file belongs to.]"
  {-P,--all-pipelines}"[Reproduce all pipelines in the project.]"
)

_dvc_remove=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
  "--dry[Only print the commands that would be executed without actually executing]"
  {-o,--outs}"[Only remove DVC file outputs.(default)]"
  {-p,--purge}"[Remove DVC file and all its outputs]"
)

_dvc_move=(
  "1:Source:_files"
  "2:Destination:"
)

_dvc_gc=(
  {-a,--all-branches}"[Collect garbage for all branches]"
  {-T,--all-tags}"[Collect garbage for all tags]"
  {-c,--cloud}"[Collect garbage in remote repository]"
  {-r,--remote}"[Remote repository to collect garbage in]:Remote repository:"
)

_dvc_config=(
  {-u,--unset}"[Unset option.]"
  "--local[Unset option.]"
)

_dvc_remote=(
  "1:Task:(add remove modify list)"
)

_dvc_metrics=(
  "1:Task:(show add modify remove)"
)

_dvc_install=()

_dvc_root=()

_dvc_lock=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_unlock=(
  "*:DVC files:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_pipeline=(
  "1:Task:(show)"
)

typeset -A opt_args
local context state line curcontext="$curcontext"

_arguments \
  $_dvc_options \
  '1: :_dvc_commands' \
  '*::args:->args'

case $words[1] in
  init)     _arguments $_dvc_global_options   $_dvc_init     ;;
  destroy)  _arguments $_dvc_global_options   $_dvc_destroy  ;;
  add)      _arguments $_dvc_global_options   $_dvc_add      ;;
  import)   _arguments $_dvc_global_options   $_dvc_import   ;;
  checkout) _arguments $_dvc_global_options   $_dvc_checkout ;;
  run)      _arguments $_dvc_global_options   $_dvc_run      ;;
  pull)     _arguments $_dvc_global_options   $_dvc_pull     ;;
  push)     _arguments $_dvc_global_options   $_dvc_push     ;;
  fetch)    _arguments $_dvc_global_options   $_dvc_fetch    ;;
  status)   _arguments $_dvc_global_options   $_dvc_status   ;;
  repro)    _arguments $_dvc_global_options   $_dvc_repro    ;;
  remove)   _arguments $_dvc_global_options   $_dvc_remove   ;;
  move)     _arguments $_dvc_global_options   $_dvc_move     ;;
  gc)       _arguments $_dvc_global_options   $_dvc_gc       ;;
  config)   _arguments $_dvc_global_options   $_dvc_config   ;;
  remote)   _arguments $_dvc_global_options   $_dvc_remote   ;;
  metrics)  _arguments $_dvc_global_options   $_dvc_metrics  ;;
  install)  _arguments $_dvc_global_options   $_dvc_install  ;;
  root)     _arguments $_dvc_global_options   $_dvc_root     ;;
  lock)     _arguments $_dvc_global_options   $_dvc_lock     ;;
  unlock)   _arguments $_dvc_global_options   $_dvc_unlock   ;;
  pipeline) _arguments $_dvc_global_options   $_dvc_pipeline ;;
esac
