import logging

logging.basicConfig(level=logging.DEBUG)


class Actor(object):
    """Actor base class"""

    count = 0

    def __init__(self, name=None):
        super(Actor, self).__init__()
        self.__class__.count += 1
        self.id = "%s/%i" % (self.__class__.__name__, self.__class__.count)
        self.name = name if name is not None else self.id
        self.logger = logging.getLogger(self.name)
        self.connections = {}
        self.inputs = {}

    def put_input(self, port, value):
        self.logger.debug('put_input')
        self.inputs[port].append(value)
        fire = self.eval_inputs()
        if fire:
            # notice supervisor here
            self.fire()

    def setup_input_ports(self, ports):
        # setup dynamic input ports
        self.inputs = {port: [] for port in ports}

    def clear_inputs(self):
        self.logger.debug('clear inputs')
        for port in self.inputs:
            self.inputs[port] = []


class ActorA(Actor):
    """docstring for ActorA"""

    # fixed input ports as class attributes

    def __init__(self, name, config, in_ports=('input_1', )):
        super(ActorA, self).__init__(name=name)
        self.config = config
        self.setup_input_ports(in_ports)

    def eval_inputs(self):
        # input complete logic
        self.logger.debug('eval_inputs')
        return bool(self.inputs.get('input_1'))

    def fire(self, input_1=None):
        # keyword arguments enable direct calls
        self.logger.debug('inside fire')
        if input_1 is None:
            input_1 = self.inputs['input_1']
        # for distributed runs a flag should be used for
        # signaling input acceptance
        self.clear_inputs()
        results = {}
        res = 0
        for vv in input_1:
            res += self.config * vv
        results['result_1'] = res
        self.logger.debug('results = %s' % results)
        # clear inputs before results are published
        for result, connections in self.connections.iteritems():
            for connection in connections:
                self.logger.debug('%s --> %s.%s' %
                                  (result, connection['actor'].name,
                                   connection['port']))
                connection['actor'].put_input(connection['port'], results[result])

        self.logger.debug('fire ends')


class ActorB(object):
    """docstring for ActorA"""

    count = 0

    def __init__(self, name, config, in_ports=('input_1', )):
        super(ActorB, self).__init__()
        self.__class__.count += 1
        self.id = "%s/%i" % (self.__class__.__name__, self.__class__.count)
        self.logger = logging.getLogger(name)
        self.name = name
        self.config = config
        self.in_ports = list(in_ports)
        # list of connected ports
        # results_id -> [target1, target2, ...]
        self.connections = {}
        self.inputs = {}
        self.clear_inputs()

    def put_input(self, port, value):
        self.logger.debug('received input %s' % str(value))
        self.logger.debug('put_input')
        self.inputs[port].append(value)
        self.eval_inputs()

    def eval_inputs(self):
        # input complete logic
        self.logger.debug('eval_inputs: %s' % str(self.inputs))
        if self.inputs.get('input_1'):
            self.fire()

    def clear_inputs(self):
        self.logger.debug('clear inputs')
        for port in self.in_ports:
            self.inputs[port] = []

    def fire(self):
        self.logger.debug('inside fire')
        results = {}
        res = 0
        for i, v in self.inputs.iteritems():
            for vv in v:
                res += self.config * vv

        # test simple branching
        if abs(res) < 100:
            results['result_1'] = res
        else:
            results['result_2'] = res
        self.logger.debug('results = %s' % results)

        # do this with a decorator
        self.apply_outputs(results)

    def apply_outputs(self, results):
        '''Apply outputs (results) to output ports
        '''
        # clear inputs before results are published
        self.logger
        self.clear_inputs()
        for result, connections in self.connections.iteritems():
            for connection in connections:
                if result in results:
                    self.logger.debug('%s --> %s.%s' %
                                      (result, connection['actor'].name,
                                       connection['port']))
                    connection['actor'].put_input(connection['port'], results[result])

        self.logger.debug('fire ends')


if __name__ == '__main__':
    a1 = ActorA('a1', 2)
    a2 = ActorA('a2', -2)
    a1.connections['result_1'] = [{'actor': a2, 'port': 'input_1'}]

    b1 = ActorB('b1', 1)
    a2.connections['result_1'] = [{'actor': b1, 'port': 'input_1'}]
    b1.connections['result_1'] = [{'actor': a1, 'port': 'input_1'}]

    a1.put_input('input_1', 2)  # keep a.input_1 = 3 ???
