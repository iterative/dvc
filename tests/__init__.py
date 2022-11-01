import os

# Increasing fd ulimit for tests
if os.name == "nt":
    try:
        import win32file  # pylint: disable=import-error
    except ImportError:
        pass
    else:
        win32file._setmaxstdio(4096)
else:
    import resource  # pylint: disable=import-error

    resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))

    nproc_soft, nproc_hard = resource.getrlimit(resource.RLIMIT_NPROC)
    resource.setrlimit(resource.RLIMIT_NPROC, (nproc_hard, nproc_hard))
