"""Documentation utilities."""


def inherit_docs(cls):
    """Class decorator that makes it inherit function doc strings."""
    for name in dir(cls):
        member = getattr(cls, name)
        if member.__doc__ is not None:
            continue

        for parent in cls.mro()[1:]:
            if hasattr(parent, name) and getattr(parent, name).__doc__:
                try:
                    member.__doc__ = getattr(parent, name).__doc__
                except AttributeError:
                    pass

    return cls
