from collections import OrderedDict as _OrderedDict


try:
    from threading import get_ident as _get_ident
except ImportError:
    from dummy_threading import get_ident as _get_ident


class ListDict(_OrderedDict):
    """Ordered dict with insert methods

    From https://gist.github.com/jaredks/6276032
    """
    def __init__(self, *args, **kwds):
        try:
            self.__insertions_running
        except AttributeError:
            self.__insertions_running = {}
        super(ListDict, self).__init__(*args, **kwds)

    def __setitem__(self, key, value, dict_setitem=dict.__setitem__):
        if _get_ident() in self.__insertions_running:
            self.__insertions_running[_get_ident()] = key, value
        else:
            super(ListDict, self).__setitem__(key, value, dict_setitem)

    def __insertion(self, link_prev, key_value):
        self.__insertions_running[_get_ident()] = 1
        self.__setitem__(*key_value)
        key, value = self.__insertions_running.pop(_get_ident())
        if link_prev[2] != key:
            if key in self:
                del self[key]
            link_next = link_prev[1]
            self._OrderedDict__map[key] = link_prev[1] = link_next[0] = [link_prev, link_next, key]
        dict.__setitem__(self, key, value)

    def insert_after(self, existing_key, key_value):
        self.__insertion(self._OrderedDict__map[existing_key], key_value)

    def insert_before(self, existing_key, key_value):
        self.__insertion(self._OrderedDict__map[existing_key][0], key_value)