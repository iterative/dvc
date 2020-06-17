from .base import Base


class HTTP(Base):
    @staticmethod
    def get_url(port):
        return f"http://127.0.0.1:{port}"
