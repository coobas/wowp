class ActorA(object):
    """docstring for ActorA"""
    def __init__(self, config, in_ports=('input_1', ), out_ports=('output_1', )):
        super(ActorA, self).__init__()
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
        self.inputs[port].append(value)
        self.eval_inputs()

    def eval_inputs(self):
        # input complete logic
        if self.inputs.get('input_1'):
            self.fire()

    def clear_inputs(self):
        print('clear inputs')
        for port in self.in_ports:
            self.inputs[port] = []

    def fire(self, input_1=None):
        print('inside fire')
        self.results['result_1'] = 0
        for i, v in self.inputs.iteritems():
            for vv in v:
                self.results['result_1'] += self.config * vv
        self.clear_inputs()
        print('result = %s' % self.results)
        # for connection, value_key in self._output_1:
        #     connection = self.results['value_key']


if __name__ == '__main__':
    a = ActorA(3)
    a.put_input('input_1', 3)  # keep a.input_1 = 3 ???

    b = ActorA(-2)
    a.connections['result_1'] = [{'actor': b, 'port': 'port_1'}]

