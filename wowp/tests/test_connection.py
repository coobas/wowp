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


def test_persistent_port():
    actor = Sink()
    actor.inports.append('in_port', persistent=True)
    assert actor.inports['in_port'].persistent
    # the port should be empty now
    assert not actor.inports['in_port']
    value = randint(-100, 100)
    actor.inports.in_port.put(value)
    assert actor.inports.in_port.pop() == value
    # the value should still be there
    assert actor.inports.in_port.pop() == value
    # and this should evaluate to True
    assert actor.inports['in_port']
    # try to switch off the persistence
    actor.inports['in_port'].persistent = False
    assert_raises(IndexError, actor.inports.in_port.pop)
    assert not actor.inports['in_port']


def test_default_value_port():
    actor = Sink()
    actor.inports.append('in_port')
    assert_raises(AttributeError, getattr, actor.inports.in_port, 'default')
    value = randint(-100, 100)
    actor.inports.in_port.default = value
    assert actor.inports.in_port.default == value
    assert actor.inports.in_port.pop() == value


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
