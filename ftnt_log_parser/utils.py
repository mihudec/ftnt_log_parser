import collections.abc

# https://stackoverflow.com/a/3233356
def recursive_dict_update(orig: dict, update: dict, inplace: bool = False):
    ret = orig if inplace else dict(orig)
    for k, v in update.items():
        if isinstance(v, collections.abc.Mapping):
            ret[k] = recursive_dict_update(ret.get(k, {}), v)
        else:
            ret[k] = v
    return ret

# https://stackoverflow.com/a/69572347
def dict_update_path(orig, path, value, inplace: bool = False):
    ret = orig if inplace else dict(orig)
    ret_sub = ret
    *path, last = path.split(".")
    for bit in path:
        ret_sub = ret_sub.setdefault(bit, {})
    ret_sub[last] = value
    return ret


