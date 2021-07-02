from .base import ObjectDB


class OSSObjectDB(ObjectDB):
    """
    The oss remote is migrated to ossfs recently
    add some additional verification for the data
    """

    DEFAULT_VERIFY = True
