from __future__ import annotations

import logging
from pathlib import Path


class Log:

    def __init__(self, logDirectory: Path) -> None:
        format = '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s [%(filename)s:%(lineno)d]'
        datefmt = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(fmt=format, datefmt=datefmt)
        self.logging = logging.getLogger()
        self.logging.setLevel(logging.INFO)
        fh = logging.FileHandler(filename=str(logDirectory))
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        self.logging.addHandler(fh)
