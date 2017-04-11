from wowp.actors import FuncActor
import math

sin = FuncActor(math.sin)
asin = FuncActor(math.asin)

asin.inports['inp'] += sin.outports['out']

WORKFLOW = sin.get_workflow()

INPUTS = {
    'inp': math.pi / 2
}
