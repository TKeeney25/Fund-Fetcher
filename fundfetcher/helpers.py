import os


def get_root_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))