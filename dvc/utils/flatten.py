import flatten_dict


def flatten(d):
    return flatten_dict.flatten(d, reducer="dot")


def unflatten(d):
    return flatten_dict.unflatten(d, splitter="dot")
