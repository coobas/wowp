
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

*Better workflow creation will be implemented soon.
``Actor.get_workflow`` will create a workflow *\ automagically\ *. It
will also be possible to create wokflows *\ explicitely\ *, e.g. in
cases when ``get_workflow`` cannot be used.*

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

Get the resulting workflow.

.. code:: python

    wf = actor1.get_workflow()

Execute the workflow just like an actor.

.. code:: python

    wf(x=3)




.. parsed-literal::

    {'out': deque([7])}



Creating a custom actor
-----------------------

.. code:: python

    from wowp import Actor

Every actor must implement ``on_input`` and ``fire`` methods. \*
``can_run`` is called whenever a new input arrives (on any port). \* The
``run`` method gets inputs from input ports using ``pop``. \* The result
of ``run`` must be a ``dict`` (like) object, whose keys are output port
names.

.. code:: python

    class StrActor(Actor):
    
        def __init__(self, *args, **kwargs):
            super(StrActor, self).__init__(*args, **kwargs)
            # specify input port
            self.inports.append('input')
            # and output ports
            self.outports.append('output')
    
        def can_run(self):
            # can run if an input value is provided
            return not self.inports['input'].isempty()
    
        def run(self):
            # get input value(s) using .pop()
            value = self.inports['input'].pop()
            # return a dictionary with port names as keys
            res = {'output': str(value)}
            return res

Create an instance.

.. code:: python

    actor = StrActor(name='str_actor')

Test the actor by direct call.

.. code:: python

    # we can call the actor directly -- see what's output
    value = 123
    print(actor(input=value))
    # and check that the output is as expected
    assert actor(input=value)['output'] == str(value)


.. parsed-literal::

    {'output': '123'}

