import os

# Increasing fd ulimit for tests
if os.name == "nt":
    import win32file

    win32file._setmaxstdio(2048)
else:
    import resource

    resource.setrlimit(resource.RLIMIT_NOFILE, (2048, 2048))

    nproc_soft, nproc_hard = resource.getrlimit(resource.RLIMIT_NPROC)
    resource.setrlimit(resource.RLIMIT_NPROC, (nproc_hard, nproc_hard))
