from __future__ import annotations

import time
from pathlib import Path
from typing import Dict

import yaml


class Memory:

    def __init__(self, memoryDirectory: Path) -> None:
        """A missing memoryDirectory means no run has been recorded yet and starts
        empty, rather than raising -- a fresh checkout has no memory file at all.
        """
        self.memoryDirectory = memoryDirectory

        try:
            with open(memoryDirectory) as file:
                self.memory: Dict[str, float] = yaml.load(file, Loader=yaml.FullLoader) or {}
        except FileNotFoundError:
            self.memory = {}


    def recordRun(self, job: str) -> None:
        self.memory[job] = time.time()

        with open(self.memoryDirectory, 'w') as file:
            yaml.dump(self.memory, file)
