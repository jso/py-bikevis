"""
Utility functions for the creation/traversal of tree-like data structures.
"""

def get(data, *keys, **kwargs):
    """
    Stupid-dumb traversal of a tree-like data structure given the keys.
    Just returns whatever is at that point in the tree.

    By default, "default" argument is a dictionary, which means that whatever
    key-path is specified will be inserted into the tree, and worst case
    will return a dictionary stored in the tree that can be modified.
    (To do this, we use setdefault.)

    The "default" argument needs to be a callable,
    to return a new instance on each call.
    """

    default = kwargs.get("default", dict)

    # a few options on how to handle the final key...
    default_leaf = default
    if "leaf" in kwargs:
        leaf_val = kwargs["leaf"]
        if not callable(leaf_val):
            default_leaf = lambda: leaf_val
        else:
            default_leaf = leaf_val

    # whether or not to create the nodes.
    create = kwargs.get("create", True)

    for k in keys[:-1]:
        if create:
            data = data.setdefault(k, default())
        else:
            data = data.get(k, default())

    if create:
        data = data.setdefault(keys[-1], default_leaf())
    else:
        data = data.get(keys[-1], default_leaf())

    return data

def custom(cfunc, afunc):
    """
    Return a tree-structure creator that uses the specified functions
    for creating new leaves and updating leaf values.

    Args:
        cfunc: takes no args, returns the "empty" element
        afunc: takes 2 args (an object created by cfunc, value to add), returns the new/updated object
    """

    def func(data, *keysAndVal, **kwargs):
        """
        Generate a tree-like data structure from a dictionary ("data").

        By default:
            Builds a list for each tuple keysAndVal[:-1]
            and appends keysAndVal[-1] to the list
        """

        dkeys = keysAndVal[:-2] # keys that will store a dictionary
        lkey = keysAndVal[-2] # key that will store a list
        val = keysAndVal[-1] # value to add

        d = data
        for k in dkeys:
            if k not in d: d[k] = {}
            d = d[k]
        if lkey not in d: d[lkey] = cfunc()
        d[lkey] = afunc(d[lkey], val, **kwargs)

    return func

put_list = custom(lambda: list(), lambda lst, obj: (lst.append(obj) or lst))
put_set = custom(lambda: set(), lambda st, obj: (st.add(obj) or st))
put_val = custom(lambda: None, lambda st, obj: obj)
put_add = custom(lambda: 0, lambda st, obj: st + obj)

def make_put_cdf(res):
    from cdf import cdf
    return custom(
        lambda: cdf(res), 
        lambda cdfObj, val: (cdfObj.insert(val) or cdfObj))

def traverse(data, *levels):
    """
    Traverse the tree-like data structure generated with the "put"
    method defined above.

    Call with the dictionary, and names for each tier in the dictionary.

    Functions as a generator, returning:
        dictionary of keys->vals, the list of values 
    """

    stack = [ [data, {}, levels] ]
    while stack:
        d, params, levelsLeft = stack.pop()

        if not levelsLeft:
            yield params, d
            continue

        thisLevel = levelsLeft[0]
        rest = levelsLeft[1:]

        for key in d:
            newParams = dict(params)
            newParams[thisLevel] = key

            stack.append( [d[key], newParams, rest] )

def combine(aggr, add_data, nlevels, **kwargs):
    cfunc = kwargs.get("cfunc", lambda: None)
    afunc = kwargs.get("afunc", lambda st, obj: obj)

    my_put = custom(cfunc, afunc)

    levels = [str(x) for x in range(nlevels)]
    for k, v in traverse(add_data, *levels):
        this_args = [k[x] for x in levels] + [v]
        my_put(aggr, *this_args)

    return aggr
