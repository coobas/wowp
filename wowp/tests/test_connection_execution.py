from wowp import Actor
from wowp.actors import FuncActor
import nose


def test_two_connected():
	"""Two connected function actors.

    --func1(.)--func2(.)--
	"""
	def func1(x) -> ('a'):
		return x * 2

	def func2(a) -> ('y'):
		return a + 3

	actor1 = FuncActor(func1)
	actor2 = FuncActor(func2)

	actor2.inports['a'] += actor1.outports['a']

	for in_value in range(10):
		actor1.inports['x'].put(in_value)
		assert func2(func1(in_value)) == actor2.outports['y'].pop()


def test_two_to_one_connected():
	"""Two converging lines.

	--func1(.)--+
                |
                +-func2(.,.)--
                |
    --func1(.)--+

	"""
	def func1(x) -> ('a'):
		return x * 2

	def func2(x, y) -> ('a'):
		return x + y

	in_actor1 = FuncActor(func1)
	in_actor2 = FuncActor(func1)

	out_actor = FuncActor(func2)

	out_actor.inports['x'] += in_actor1.outports['a']
	out_actor.inports['y'] += in_actor2.outports['a']

	in_value1 = 1
	in_value2 = 2

	in_actor2.inports['x'].put(in_value2)
	in_actor1.inports['x'].put(in_value1)

	assert func2(func1(in_value1), func1(in_value2)) == out_actor.outports['a'].pop()


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
