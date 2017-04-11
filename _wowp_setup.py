"""
Tools to help with setup.py
Bootstrapped from https://github.com/jakevdp/mpld3.
Much of this is based on tools in the IPython project:
http://github.com/ipython/ipython
"""

import os


def get_version():
    """Get the version info from the package without importing it"""
    with open(os.path.join("wowp", "__about__.py"), "r") as init_file:
        exec(compile(init_file.read(), 'wowp/__about__.py', 'exec'), globals())
    try:
        return __version__
    except NameError:
        raise ValueError("version could not be located")
