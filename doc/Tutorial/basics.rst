
Basic wowp usage
================

Turning functions into actors
-----------------------------

1. Define a function
~~~~~~~~~~~~~~~~~~~~

We will define a simple ``times2`` function. Annotations (see `PEP
3107 <https://www.python.org/dev/peps/pep-3107/>`__) will be used for
output port names.

.. code:: python

    def times2(x) -> ('y'):
        '''Multiplies the input by 2
        '''
        return x * 2

2. Turn the function into an *actor*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from wowp.actors import FuncActor
    
    times2_actor = FuncActor(times2)

3. Inspect actor's input and output ports
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Input / output ports are accessible via ``inports`` and ``outports``
properties.

.. code:: python

    print('input  ports: {}'.format(times2_actor.inports.keys()))
    print('output ports: {}'.format(times2_actor.outports.keys()))


.. parsed-literal::

    input  ports: ['x']
    output ports: ['y']
    

4. FuncActor is callable
~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    x = 3
    print('times2({}) = {}'.format(x, times2(x)))
    print('times2_actor({}) = {}'.format(x, times2_actor(x)))
    assert times2(x) == times2_actor(x)


.. parsed-literal::

    times2(3) = 6
    times2_actor(3) = 6
    

Simple workflows
----------------

1. Workflows are created by connecting actor ports (input ports to
   output ports).
2. Ports get connected using the **``+=``** operator
   (``inport += outport``).

\*Better workflow creation will be implemented soon.
``Actor.get_workflow`` will create a workflow *automagically*. It will
also be possible to create wokflows *explicitely*, e.g. in cases when
``get_workflow`` cannot be used.\*

Two actors chained together
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's try something like ``x -> actor1 -> actor2 -> out``.

.. code:: python

    # create two FuncActors
    actor1 = FuncActor(lambda x: x * 2)
    actor2 = FuncActor(lambda x: x + 1)
    
    # chain the actors
    # FuncActor output port is by default called out
    actor2.inports['x'] += actor1.outports['out']

Provide an input value using the ``put`` method of input port(s).

.. code:: python

    # put an input value
    x = 4
    actor1.inports['x'].put(x)
    # the workflow should have finished, now get the output
    y = actor2.outports['out'].pop()
    print('The result for x = {} is {}'.format(x, y))
    # check the results
    assert y == x * 2 + 1


.. parsed-literal::

    The result for x = 4 is 9
    

Creating a custom actor
-----------------------

.. code:: python

    from wowp import Actor

Every actor must implement ``on_input`` and ``fire`` methods. \*
``on_input`` is called whenever a new input arrives (on any port). \*
``on_input`` must invoke ``self.run()`` when the actor is ready to run.
\* The ``fire`` method gets inputs from input ports using ``pop``. \*
The result of ``fire`` must be a ``dict`` (like) object, whose keys are
output port names.

.. code:: python

    class StrActor(Actor):
        def __init__(self, *args, **kwargs):
            super(StrActor, self).__init__(*args, **kwargs)
            # specify input port
            self.inports.append('input')
            # and output ports
            self.outports.append('output')
        def on_input(self):
            # call run if any input is available
            self.run()
        def fire(self):
            # get input value(s) using .pop()
            value = self.inports['input'].pop()
            # return a dictionary with port names as keys
            res = {'output': str(value)}
            return res

.. code:: python

    actor = StrActor(name='str_actor')

.. code:: python

    # we can call the actor directly -- see what's output
    value = 123
    print(actor(input=value))
    # and check that the output is as expected
    assert actor(input=value)['output'] == str(value)


.. parsed-literal::

    {'output': '123'}
    
