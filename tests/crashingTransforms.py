"""Transformers that end the worker process running them, for testing how the
runner survives a worker that dies mid-job. Referenced by module path, since a
worker process resolves transformers by importing them.
"""
import os


def exitAbruptly(value):
    """What the kernel's OOM killer looks like from the parent: the process is
    simply gone, with no exception and no outcome.
    """
    os._exit(9)
