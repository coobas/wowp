from __future__ import absolute_import, division, print_function, unicode_literals
from wowp import Actor
from wowp.actors import Sink
import nose
from random import randint
from nose.tools import assert_raises


def test_linear():
    a1 = Actor(name='actor 1')
    a1.outports.append('out_port')
    a2 = Actor(name='actor 2')
    a2.inports.append('in_port')

    a2.inports['in_port'] += a1.outports['out_port']

    assert a1.outports['out_port'] in a2.inports['in_port'].connections
    assert a2.inports['in_port'] in a1.outports['out_port'].connections

    assert a1.outports['out_port'].is_connected_to(a2.inports['in_port'])


def test_frozen_port():
    from wowp.components import FrozenInPort
    actor = Sink()
    actor.inports.append('in_port', port_class=FrozenInPort)
    # the port should be empty now
    assert actor.inports['in_port'].isempty()
    assert_raises(IndexError, actor.inports.in_port.pop)
    value = randint(-100, 100)
    actor.inports.in_port.put(value)
    assert actor.inports.in_port.pop() == value
    # the value should still be there
    assert actor.inports.in_port.pop() == value
    # and this should evaluate to True
    assert not actor.inports['in_port'].isempty()
    # put is allowed only once
    assert_raises(IndexError, actor.inports.in_port.put, value)


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
