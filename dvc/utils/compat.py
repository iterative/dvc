"""Handle impor compatibility between Python 2 and Python 3"""

try:
    from urlparse import urlparse, urljoin  # noqa: F401
except ImportError:
    from urllib.parse import urlparse, urljoin  # noqa: F401
