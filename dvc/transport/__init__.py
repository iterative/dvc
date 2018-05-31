def Transport(path):
    pass

def exists(path):
    return Transport(path).exists(path)


def copy(inp, outp):
    return Transport(inp).copy(inp, outp)


def is_file(path):
    return Transport(path).is_file(path)


def is_dir(path):
    return Transport(path).is_dir(path)


def is_empty(path):
    return Transport(path).is_empty(path)
