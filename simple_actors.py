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
        self.logger = logging.getLogger(self.id)
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

    def setup_output_ports(self, ports):
        # setup dynamic input ports
        self.connections = {port: [] for port in ports}

    def input_ports(self):
        # TODO make it a property with setter
        return self.inputs.keys()

    def output_ports(self):
        # TODO similar to input_ports
        return self.connections.keys()

    def clear_inputs(self):
        self.logger.debug('clear inputs')
        for port in self.inputs:
            self.inputs[port] = []

    def connect_to(self, source_port, dest_actor, dest_port):
        if source_port not in self.output_ports():
            raise Exception('output port %s is not defined' % source_port)
        if dest_port not in dest_actor.input_ports():
            raise Exception('input port %s not defined in %s' % (dest_port, dest_actor.id))
        self.connections[source_port].append({'actor': dest_actor, 'port': dest_port})


class ActorA(Actor):
    """docstring for ActorA"""

    # fixed input ports as class attributes

    def __init__(self, name, config, in_ports=('input_1', )):
        super(ActorA, self).__init__(name=name)
        self.config = config
        self.setup_input_ports(in_ports)
        self.setup_output_ports(('result_1', ))

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


class ActorB(Actor):
    """docstring for ActorA"""

    def __init__(self, name, config, in_ports=('input_1', )):
        super(ActorB, self).__init__(name=name)
        self.config = config
        self.setup_input_ports(in_ports)
        self.setup_output_ports(('result_1', 'result_2'))

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
    # a1.connections['result_1'] = [{'actor': a2, 'port': 'input_1'}]
    a1.connect_to('result_1', a2, 'input_1')

    b1 = ActorB('b1', 1)
    # a2.connections['result_1'] = [{'actor': b1, 'port': 'input_1'}]
    a2.connect_to('result_1', b1, 'input_1')
    # b1.connections['result_1'] = [{'actor': a1, 'port': 'input_1'}]
    b1.connect_to('result_1', a1, 'input_1')

    a1.put_input('input_1', 2)  # keep a.input_1 = 3 ???
