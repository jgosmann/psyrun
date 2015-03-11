def dict_concat(args):
    keys = set()
    for a in args:
        keys = keys.union(a.keys())
    return {k: [a.get(k, None) for a in args] for k in keys}
