from __future__ import unicode_literals


# just simple check for Nones and emtpy strings
def compact(args):
    return list(filter(bool, args))
