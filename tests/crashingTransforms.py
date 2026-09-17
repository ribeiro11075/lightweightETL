"""Transformers that misbehave -- exit, hang, dawdle -- for testing how the
runner copes with a job that does. Referenced by module path, since a job
process resolves transformers by importing them.
"""
import os


def exitAbruptly(value):
    """What the kernel's OOM killer looks like from the parent: the process is
    simply gone, with no exception and no outcome.
    """
    os._exit(9)


def slowly(value):
    """Long enough for jobs started together to overlap."""
    import time
    time.sleep(0.5)
    return value


def hang(value):
    """A query that never returns, as far as the runner can tell."""
    import time
    time.sleep(3600)
    return value
