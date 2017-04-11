WOWP
====

**WOWP** (A **WO**\ rkflo\ **W** Framework in **P**\ ython) is a modern,
light-weight framework for integrated simulations in science.

Our goal
--------

Wowp enables flow based programming in Python via actors and workflows.
We prefer having a clean, straightforward API for creating and
connecting actors and workflows. The target coding style should be as
simple as

.. code:: python

    # connect two actors
    actor2.inports['x'] += actor1.outports['y']
    # get the workflow
    workflow = actor2.get_workflow()
    # run the workflow
    workflow(x=1)

Installation
------------

Using pip + latest development version:

.. code:: bash

    pip install hg+https://urbanj@bitbucket.org/urbanj/wowp

Using pypi (may be outdated)

.. code:: bash

    pip install wowp

Dependencies
++++++++++++

* decorator
* future
* networkx
* nose
* six
* click
  
Various parallel schedulers require at least one of

* `ipyparallel <https://github.com/ipython/ipyparallel>`_ 
* `distributed <https://github.com/dask/distributed>`_ 
* `mpi4py <http://pythonhosted.org/mpi4py/>`_ 

Documentation
-------------

See http://pythonic.eu/wowp

*Copyright (c) 2015-2017 Jakub Urban, Jan Pipek under The MIT License (MIT)*
