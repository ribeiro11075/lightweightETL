"""A masking strategy defined outside the package, as a policy can name one."""
from understudy_data.masking import Strategy


class Initials(Strategy):
    """Keeps a name's initials, keyed so the rest is unrecoverable."""

    OPTIONS = {'separator': str}

    def mask(self, value):
        separator = self.options.get('separator', '.')
        return separator.join(word[0] for word in str(value).split()) + separator + self.keyedHash.digest(str(value).encode()).hex()[:4]


class NotAStrategy:
    pass
