import os

# Increasing fd ulimit for tests
if os.name == "nt":
    import win32file
    import subprocess

    win32file._setmaxstdio(2048)

    # Workaround for two bugs:
    #
    # 1) gitpython-developers/GitPython#546
    # GitPython leaves git cat-file --batch/--batch-check processes that are
    # not cleaned up correctly, so Popen._active list has their defunct
    # process handles, that it is not able to cleanup because of bug 2)
    #
    # 2) https://bugs.python.org/issue37380
    # subprocess.Popen._internal_poll on windows is getting
    #
    # 	OSError: [WinError 6] The handle is invalid
    #
    # exception, which it doesn't ignore and so Popen is not able to cleanup
    # old processes and that prevents it from creating any new processes at
    # all, which results in our tests failing whenever they try to use Popen.

    def noop():
        pass

    subprocess._cleanup = noop
else:
    import resource

    resource.setrlimit(resource.RLIMIT_NOFILE, (2048, 2048))

    nproc_soft, nproc_hard = resource.getrlimit(resource.RLIMIT_NPROC)
    resource.setrlimit(resource.RLIMIT_NPROC, (nproc_hard, nproc_hard))
