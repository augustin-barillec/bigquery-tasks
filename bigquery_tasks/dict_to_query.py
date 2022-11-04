from datetime import datetime


def is_string_date(s):
    try:
        datetime.strptime(s, '%Y-%m-%d')
        return True
    except (TypeError, ValueError):
        return False


def is_string_timestamp(s):
    try:
        datetime.strptime(s[:19], '%Y-%m-%d %H:%M:%S')
        return True
    except (TypeError, ValueError):
        return False


def dict_to_cols(d):
    res = ''
    visited = set()
    for k in d:
        if d[k] is None:
            res += 'cast(null as string)'
        elif is_string_date(d[k]):
            res += f"date('{d[k]}')"
        elif is_string_timestamp(d[k]):
            res += f"timestamp('{d[k]}')"
        elif type(d[k]) == dict:
            res += f'struct({dict_to_cols(d[k])})'
        else:
            res += f'{repr(d[k])}'
        res += f' as {k}'
        visited.add(k)
        if visited != d.keys():
            res += ', '
    return res


def dict_to_query(d):
    return 'select ' + dict_to_cols(d)
