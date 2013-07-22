import logging

logging.basicConfig(level=logging.DEBUG)


class ActorA(object):
    """docstring for ActorA"""

    count = 0

    def __init__(self, name, config, in_ports=('input_1', ), out_ports=('output_1', )):
        super(ActorA, self).__init__()
        self.__class__.count += 1
        self.id = "%s/%i" % (self.__class__.__name__, self.__class__.count)
        self.logger = logging.getLogger(name)
        self.name = name
        self.config = config
        self.in_ports = list(in_ports)
        self.out_ports = list(out_ports)
        # list of connected ports
        # results_id -> [target1, target2, ...]
        self.connections = {}
        self.results = {}
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
        self.results['result_1'] = 0
        for i, v in self.inputs.iteritems():
            for vv in v:
                self.results['result_1'] += self.config * vv
        self.logger.debug('results = %s' % self.results)
        # clear inputs before results are published
        self.clear_inputs()
        for result, connections in self.connections.iteritems():
            for connection in connections:
                self.logger.debug('%s --> %s.%s' %
                                  (result, connection['actor'].name,
                                   connection['port']))
                connection['actor'].put_input(connection['port'], self.results[result])


if __name__ == '__main__':
    a = ActorA('a', 3)
    b = ActorA('b', -2)
    a.connections['result_1'] = [{'actor': b, 'port': 'input_1'}]
    a.put_input('input_1', 3)  # keep a.input_1 = 3 ???
