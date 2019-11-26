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
    "add:Take data files or directories under DVC control."
    "cache:Manage cache settings."
    "checkout:Checkout data files from cache."
    "commit:Save changed data to cache and update DVC-files."
    "config:Get or set config settings."
    "destroy:Remove DVC-files, local DVC config and data cache."
    "diff:Show a diff of a DVC controlled data file or a directory."
    "fetch:Fetch data files from a DVC remote storage."
    "get-url:Download or copy files from URL."
    "get:Download data from DVC repository."
    "gc:Collect unused data from DVC cache or a remote storage."
    "import-url:Download or copy file from URL and take it under DVC control."
    "import:Download data from DVC repository and take it under DVC control."
    "init:Initialize DVC in the current directory."
    "install:Install DVC git hooks into the repository."
    "lock:Lock DVC-file."
    "metrics:Commands to add, manage, collect and display metrics."
    "move:Rename or move a DVC controlled data file or a directory."
    "pipeline:Manage pipelines."
    "pull:Pull data files from a DVC remote storage."
    "push:Push data files to a DVC remote storage."
    "remote:Manage remote storage configuration."
    "remove:Remove outputs of DVC-file."
    "repro:Check for changes and reproduce DVC-file and dependencies."
    "root:Relative path to project's directory."
    "run:Generate a stage file from a command and execute the command."
    "status:Show changed stages, compare local cache and a remote storage."
    "unlock:Unlock DVC-file."
    "unprotect:Unprotect data file/directory."
    "update:Update data artifacts imported from other DVC repositories."
    "version:Show DVC version and system/environment information."
  )

  _describe 'dvc commands' _commands
}

_dvc_options=(
  "(-)"{-h,--help}"[Show help message.]"
  "(-)"{-V,--version}"[Show program's version]"
)

_dvc_global_options=(
  "(-)"{-h,--help}"[Show help message related to the command.]"
  "(-)"{-q,--quiet}"[Be quiet.]"
  "(-)"{-v,--verbose}"[Be verbose.]"
)

_dvc_add=(
  {-R,--recursive}"[Recursively add each file under the directory.]"
  "--no-commit[Don't put files/directories into cache.]"
  {-f,--file}"[Specify name of the DVC-file it generates.]:File:_files"
  "1:File:_files"
)

_dvc_cache=(
  "1:Sub command:(dir)"
)

_dvc_checkout=(
  {-d,--with-deps}"[Checkout all dependencies of the specified target.]"
  {-f,--force}"[Do not prompt when removing working directory files.]"
  {-R,--recursive}"[Checkout all subdirectories of the specified directory.]"
  "1:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_commit=(
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
  {-f,--force}"[Commit even if checksums for dependencies/outputs changed.]"
  {-d,--with-deps}"[Commit all dependencies of the specified target.]"
  {-R,--recursive}"[Commit cache for subdirectories of the specified directory.]"
)

_dvc_config=(
  "--global[Use global config.]"
  "--system[Use system config.]"
  "--local[Use local config.]"
  {-u,--unset}"[Unset option.]"
)

_dvc_destroy=(
  {-f,--force}"[Force destruction.]"
)

_dvc_diff=(
  {-t,--target}"[Source path to a data file or directory.]:Target files:"
)

_dvc_fetch=(
  {-j,--jobs}"[Number of jobs to run simultaneously.]:Number of jobs:"
  {-r,--remote}"[Remote repository to fetch from.]:Remote repository:"
  {-a,--all-branches}"[Fetch cache for all branches.]"
  {-T,--all-tags}"[Fetch cache for all tags.]"
  {-d,--with-deps}"[Fetch cache for all dependencies of the specified target.]"
  {-R,--recursive}"[Fetch cache for subdirectories of specified directory.]"
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_geturl=(
  "1:URL:"
  "2:Output:"
)

_dvc_get=(
  {-o,--out}"[Destination path to put data to.]:OUT:_files -/"
  "--rev[DVC repository git revision.]:Commit hash:"
  "1:URL:"
  "2:Path:"
)

_dvc_gc=(
  {-a,--all-branches}"[Keep data files for the tips of all git branches.]"
  {-T,--all-tags}"[Keep data files for all git tags.]"
  {-c,--cloud}"[Collect garbage in remote repository.]"
  {-r,--remote}"[Remote storage to collect garbage in.]:Remote repository:"
  {-f,--force}"[Force garbage collection - automatically agree to all prompts.]:Repos:_files"
  {-j,--jobs}"[Number of jobs to run simultaneously.]:Number of jobs:"
  {-p,--projects}"[Keep data files required by these projects in addition to the current one.]:Repos:_files"
)

_dvc_importurl=(
  {-f,--file}"[Specify name of the DVC-file it generates.]:File:_files"
  "1:URL:"
  "2:Output:"
)

_dvc_import=(
  {-o,--out}"[Destination path to put data to.]:OUT:_files -/"
  "--rev[DVC repository git revision.]:Commit hash:"
  "1:URL:"
  "2:Path:"
)

_dvc_init=(
  "--no-scm[Initiate dvc in directory that is not tracked by any scm tool.]"
  {-f,--force}"[Overwrite existing '.dvc' directory. This operation removes local cache.]"
)

_dvc_install=()

_dvc_lock=(
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_metrics=(
  "1:Sub command:(show add modify remove)"
)

_dvc_move=(
  "1:Source:_files"
  "2:Destination:"
)

_dvc_pipeline=(
  "1:Sub command:(show list)"
)

_dvc_pull=(
  {-j,--jobs}"[Number of jobs to run simultaneously.]:Number of jobs:"
  {-r,--remote}"[Remote repository to pull from.]:Remote repository:"
  {-a,--all-branches}"[Fetch cache for all branches.]"
  {-T,--all-tags}"[Fetch cache for all tags.]"
  {-d,--with-deps}"[Fetch cache for all dependencies of the specified target.]"
  {-f,--force}"[Do not prompt when removing working directory files.]"
  {-R,--recursive}"[Pull cache for subdirectories of the specified directory.]"
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_push=(
  {-j,--jobs}"[Number of jobs to run simultaneously.]:Number of jobs:"
  {-r,--remote}"[Remote repository to push to.]:Remote repository:"
  {-a,--all-branches}"[Push cache for all branches.]"
  {-T,--all-tags}"[Push cache for all tags.]"
  {-d,--with-deps}"[Push cache for all dependencies of the specified target.]"
  {-R,--recursive}"[Push cache from subdirectories of specified directory.]"
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_remote=(
  "1:Sub command:(add default remove modify list)"
)

_dvc_remove=(
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
  "--dry[Only print the commands that would be executed without actually executing]"
  {-o,--outs}"[Only remove DVC-file outputs. (Default)]"
  {-p,--purge}"[Remove DVC-file and all its outputs.]"
  {-f,--force}"[Force purge.]"
)

_dvc_repro=(
  {-f,--force}"[Reproduce even if dependencies were not changed.]"
  {-s,--single-item}"[Reproduce only single data item without recursive dependencies check.]"
  {-c,--cwd}"[Directory within your repo to reproduce from.]:CWD:_files -/"
  {-m,--metrics}"[Show metrics after reproduction.]"
  "--dry[Only print the commands that would be executed without actually executing]"
  {-i,--interactive}"[Ask for confirmation before reproducing each stage.]"
  {-p,--pipeline}"[Reproduce the whole pipeline that the specified stage file belongs to.]"
  {-P,--all-pipelines}"[Reproduce all pipelines in the repo.]"
  {-R,--recursive}"[Reproduce all stages in the specified directory.]"
  "--ignore-build-cache[Reproduce all descendants of a changed stage even if their direct dependencies didn't change.]"
  "--no-commit[Don't put files/directories into cache.]"
  "--downstream[Start from the specified stages when reproducing pipelines.]"
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_root=()

_dvc_run=(
  "*"{-d,--deps}"[Declare dependencies for reproducible cmd.]:Dependency:_files"
  "*"{-o,--outs}"[Declare output file or directory.]:Output data:_files"
  "*"{-O,--outs-no-cache}"[Declare output file or directory (do not put into DVC cache).]:Output regular:_files"
  "*"{-m,--metrics}"[Declare output metric file or directory.]:Metrics:_files"
  "*"{-M,--metrics-no-cache}"[Declare output metric file or directory (do not put into DVC cache).]:Metrics (no cache):_files"
  {-f,--file}"[Specify name of the DVC-file it generates.]:File:_files"
  {-c,--cwd}"[Deprecated, use -w and -f instead.]:CWD:_files -/"
  {-w,--wdir}"[Directory within your repo to run your command in.]:WDIR:_files -/"
  "--no-exec[Only create stage file without actually running it.]"
  {-y,--yes}"[Deprecated, use --overwrite-dvcfile instead]"
  "--overwrite-dvcfile[Overwrite existing DVC-file without asking for confirmation.]"
  "--ignore-build-cache[Run this stage even if it has been already ran with the same command/dependencies/outputs/etc before.]"
  "--remove-outs[Deprecated, this is now the default behavior]"
  "--no-commit[Don't put files/directories into cache.]"
  "--outs-persist[Declare output file or directory that will not be removed upon repro.]:Output persistent:_files"
  "--outs-persist-no-cache[Declare output file or directory that will not be removed upon repro (do not put into DVC cache).]:Output persistent regular:_files"
)

_dvc_status=(
  {-j,--jobs}"[Number of jobs to run simultaneously.]:Number of jobs:"
  "--show-checksums[Show checksums instead of file names.]"
  {-q,--quiet}"[Suppresses all output. Exit with 0 if pipelines are up to date, otherwise 1.]"
  {-c,--cloud}"[Show status of a local cache compared to a remote repository.]"
  {-r,--remote}"[Remote repository to compare local cache to.]:Remote repository:"
  {-a,--all-branches}"[Show status of a local cache compared to a remote repository for all branches.]"
  {-T,--all-tags}"[Show status of a local cache compared to a remote repository for all tags.]"
  {-d,--with-deps}"[Show status for all dependencies of the specified target.]"
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_unlock=(
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

_dvc_unprotect=(
  "*:Data files:_files"
)

_dvc_update=(
  "*:Stages:_files -g '(*.dvc|Dvcfile)'"
)

typeset -A opt_args
local context state line curcontext="$curcontext"

_arguments \
  $_dvc_options \
  '1: :_dvc_commands' \
  '*::args:->args'

case $words[1] in
  add)        _arguments $_dvc_global_options   $_dvc_add       ;;
  cache)      _arguments $_dvc_global_options   $_dvc_cache     ;;
  checkout)   _arguments $_dvc_global_options   $_dvc_checkout  ;;
  commit)     _arguments $_dvc_global_options   $_dvc_commit    ;;
  config)     _arguments $_dvc_global_options   $_dvc_config    ;;
  destroy)    _arguments $_dvc_global_options   $_dvc_destroy   ;;
  diff)       _arguments $_dvc_global_options   $_dvc_diff      ;;
  fetch)      _arguments $_dvc_global_options   $_dvc_fetch     ;;
  get-url)    _arguments $_dvc_global_options   $_dvc_geturl    ;;
  get)        _arguments $_dvc_global_options   $_dvc_get       ;;
  gc)         _arguments $_dvc_global_options   $_dvc_gc        ;;
  import-url) _arguments $_dvc_global_options   $_dvc_importurl ;;
  import)     _arguments $_dvc_global_options   $_dvc_import    ;;
  init)       _arguments $_dvc_global_options   $_dvc_init      ;;
  install)    _arguments $_dvc_global_options   $_dvc_install   ;;
  lock)       _arguments $_dvc_global_options   $_dvc_lock      ;;
  metrics)    _arguments $_dvc_global_options   $_dvc_metrics   ;;
  move)       _arguments $_dvc_global_options   $_dvc_move      ;;
  pipeline)   _arguments $_dvc_global_options   $_dvc_pipeline  ;;
  pull)       _arguments $_dvc_global_options   $_dvc_pull      ;;
  push)       _arguments $_dvc_global_options   $_dvc_push      ;;
  remote)     _arguments $_dvc_global_options   $_dvc_remote    ;;
  remove)     _arguments $_dvc_global_options   $_dvc_remove    ;;
  repro)      _arguments $_dvc_global_options   $_dvc_repro     ;;
  root)       _arguments $_dvc_global_options   $_dvc_root      ;;
  run)        _arguments $_dvc_global_options   $_dvc_run       ;;
  status)     _arguments $_dvc_global_options   $_dvc_status    ;;
  unlock)     _arguments $_dvc_global_options   $_dvc_unlock    ;;
  unprotect)  _arguments $_dvc_global_options   $_dvc_unprotect ;;
  update)     _arguments $_dvc_global_options   $_dvc_update    ;;
esac
