from wowp import Actor
import nose


def test_linear():
    a1 = Actor(name='actor 1')
    a1.outports.append('out_port')
    a2 = Actor(name='actor 2')
    a2.inports.append('in_port')

    a2.inports['in_port'] += a1.outports['out_port']

    assert a1.outports['out_port'] in a2.inports['in_port'].connections
    assert a2.inports['in_port'] in a1.outports['out_port'].connections

    assert a1.outports['out_port'].is_connected_to(a2.inports['in_port'])


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
