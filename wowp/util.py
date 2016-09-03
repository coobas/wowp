from __future__ import absolute_import, division, print_function, unicode_literals
from collections import OrderedDict as _OrderedDict
import warnings
import six
import warnings as _warnings
import os as _os
import sys
from tempfile import mkdtemp

try:
    if six.PY3:
        from threading import get_ident as _get_ident
    else:
        from thread import get_ident as _get_ident
except ImportError:
    from dummy_threading import get_ident as _get_ident
# import a pickling module
import pickle
my_pickle = pickle
# try dill
try:
    import dill
    _IS_DILL = True
    my_pickle = dill
except ImportError:
    _IS_DILL = False
# cludpickle is default
try:
    import cloudpickle
    _IS_CLOUDPICKLE = True
    my_pickle = cloudpickle
except ImportError:
    _IS_CLOUDPICKLE = False


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

    def __setitem__(self, key, value):
        if _get_ident() in self.__insertions_running:
            self.__insertions_running[_get_ident()] = key, value
        else:
            super(ListDict, self).__setitem__(key, value)

    def __insertion(self, link_prev, key_value):
        self.__insertions_running[_get_ident()] = 1
        self.__setitem__(*key_value)
        key, value = self.__insertions_running.pop(_get_ident())
        if link_prev[2] != key:
            if key in self:
                del self[key]
            link_next = link_prev[1]
            self._OrderedDict__map[key] = link_prev[1] = link_next[0] = [
                link_prev, link_next, key
            ]
        dict.__setitem__(self, key, value)

    def insert_after(self, existing_key, key_value):
        self.__insertion(self._OrderedDict__map[existing_key], key_value)

    def insert_before(self, existing_key, key_value):
        self.__insertion(self._OrderedDict__map[existing_key][0], key_value)


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emmitted
    when the function is used."""

    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)

    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func


class ConstructorWrapper(object):
    """This can be used as deferred construction call."""

    def __init__(self, klass, *args, **kwargs):
        self.klass = klass
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        return self.klass(*self.args, **self.kwargs)


class TemporaryDirectory(object):
    """Create and return a temporary directory.  This has the same
    behavior as mkdtemp but can be used as a context manager.  For
    example:

        with TemporaryDirectory() as tmpdir:
            ...

    Upon exiting the context, the directory and everything contained
    in it are removed.
    """

    def __init__(self, suffix="", prefix="tmp", dir=None):
        self._closed = False
        self.name = None  # Handle mkdtemp raising an exception
        self.name = mkdtemp(suffix, prefix, dir)

    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def __enter__(self):
        return self.name

    def cleanup(self, _warn=False):
        if self.name and not self._closed:
            try:
                self._rmtree(self.name)
            except (TypeError, AttributeError) as ex:
                # Issue #10188: Emit a warning on stderr
                # if the directory could not be cleaned
                # up due to missing globals
                if "None" not in str(ex):
                    raise
                print("ERROR: {!r} while cleaning up {!r}".format(ex, self,),
                      file=sys.stderr)
                return
            self._closed = True
            if _warn:
                self._warn("Implicitly cleaning up {!r}".format(self))

    def __exit__(self, exc, value, tb):
        self.cleanup()

    def __del__(self):
        # Issue a ResourceWarning if implicit cleanup needed
        self.cleanup(_warn=True)

    # XXX (ncoghlan): The following code attempts to make
    # this class tolerant of the module nulling out process
    # that happens during CPython interpreter shutdown
    # Alas, it doesn't actually manage it. See issue #10188
    _listdir = staticmethod(_os.listdir)
    _path_join = staticmethod(_os.path.join)
    _isdir = staticmethod(_os.path.isdir)
    _islink = staticmethod(_os.path.islink)
    _remove = staticmethod(_os.remove)
    _rmdir = staticmethod(_os.rmdir)
    _warn = _warnings.warn

    def _rmtree(self, path):
        # Essentially a stripped down version of shutil.rmtree.  We can't
        # use globals because they may be None'ed out at shutdown.
        for name in self._listdir(path):
            fullname = self._path_join(path, name)
            try:
                isdir = self._isdir(fullname) and not self._islink(fullname)
            except OSError:
                isdir = False
            if isdir:
                self._rmtree(fullname)
            else:
                try:
                    self._remove(fullname)
                except OSError:
                    pass
        try:
            self._rmdir(path)
        except OSError:
            pass


def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type(str('Enum'), (), enums)


MPI_TAGS = enum('READY', 'DONE', 'EXIT', 'START')


def dumps(obj):
    return my_pickle.dumps(obj)


def loads(obj):
    return my_pickle.loads(obj)


def dump(obj, file):
    if isinstance(file, six.string_types):
        file = open(file, 'wb')
    return my_pickle.dump(obj, file)


def load(file):
    if isinstance(file, six.string_types):
        file = open(file, 'rb')
    return my_pickle.load(file)


def abstractmethod(method):
    def default_abstract_method(*args, **kwargs):
        raise NotImplementedError('call to abstract method ' + repr(method))

    default_abstract_method.__name__ = method.__name__

    return default_abstract_method
