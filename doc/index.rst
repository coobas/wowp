.. wowp documentation master file, created by
   sphinx-quickstart on Mon Jun  8 10:09:34 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to wowp's documentation!
================================

wowp, or WOW:-P,  stands for a WOrkfloW framework in Python.

Our goal
--------

Wowp enables flow based programming in Python via actors and workflows. We prefer having a clean, straightforward API
for creating and connecting actors and workflows. The target coding style should be as simple as

.. code:: python

    # connect two actors
    actor2.inports['x'] += actor1.outports['y']
    # get the workflow
    workflow = actor2.get_workflow()
    # run the workflow
    workflow(x=1)


Tutorials
=========

.. toctree::
   :maxdepth: 2

   Tutorial/basics
   Tutorial/graph_drawing
   Tutorial/julia
   Tutorial/Matlab
   Tutorial/Generator actor


Reference
=========

.. toctree::
   :maxdepth: 2

   apidoc/wowp


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

