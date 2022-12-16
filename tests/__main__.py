if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-n=auto", *sys.argv[1:]]))
