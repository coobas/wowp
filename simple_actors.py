import logging

logging.basicConfig(level=logging.DEBUG)


class ActorA(object):
    """docstring for ActorA"""

    count = 0

    def __init__(self, name, config, in_ports=('input_1', )):
        super(ActorA, self).__init__()
        self.__class__.count += 1
        self.id = "%s/%i" % (self.__class__.__name__, self.__class__.count)
        self.logger = logging.getLogger(name)
        self.name = name
        self.config = config
        self.in_ports = list(in_ports)
        # list of connected ports
        # results_id -> [target1, target2, ...]
        self.connections = {}
        results = {}
        self.inputs = {}
        self.clear_inputs()

    def put_input(self, port, value):
        self.logger.debug('put_input')
        self.inputs[port].append(value)
        self.eval_inputs()

    def eval_inputs(self):
        # input complete logic
        self.logger.debug('eval_inputs')
        if self.inputs.get('input_1'):
            self.fire()

    def clear_inputs(self):
        self.logger.debug('clear inputs')
        for port in self.in_ports:
            self.inputs[port] = []

    def fire(self, input_1=None):
        self.logger.debug('inside fire')
        results = {}
        res = 0
        for i, v in self.inputs.iteritems():
            for vv in v:
                res += self.config * vv
        results['result_1'] = res
        self.logger.debug('results = %s' % results)
        # clear inputs before results are published
        self.clear_inputs()
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
        results = {}
        self.inputs = {}
        self.clear_inputs()

    def put_input(self, port, value):
        self.logger.debug('received input %s' % str(value))
        self.logger.debug('put_input')
        self.inputs[port].append(value)
        self.eval_inputs()

    def eval_inputs(self):
        # input complete logic
        self.logger.debug('eval_inputs')
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

        if abs(res) < 100:
            results['result_1'] = res
        else:
            results['result_2'] = res

        self.logger.debug('results = %s' % results)
        # clear inputs before results are published
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
