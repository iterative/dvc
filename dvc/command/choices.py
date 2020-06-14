import shtab


class Required(shtab.Required):
    DVC_FILE = [shtab.Choice("DVCFile", required=True)]


class Optional(shtab.Optional):
    DVC_FILE = [shtab.Choice("DVCFile", required=False)]
