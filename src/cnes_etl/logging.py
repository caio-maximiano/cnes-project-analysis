import logging
import sys

_DEF_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

def setup_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_DEF_FORMAT))
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        root.addHandler(handler)