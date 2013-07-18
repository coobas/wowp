class ActorA(object):
    """docstring for ActorA"""
    def __init__(self, config):
        super(ActorA, self).__init__()
        self.config = config
        self._input_1 = None
        # list of connected ports
        # results_id -> [target1, target2, ...]
        self.connections = {}
        self.results = {}

    @property
    def input_1(self):
        print('input1 getter')
        if len(self._input_1) > 1:
            return self._input_1
        elif len(self._input_1) > 0:
            return self._input_1[0]
        return None

    @input_1.setter
    def input_1(self, value):
        print('input1 setter')
        if self._input_1 is not None:
            self._input_1.append(value)
        else:
            self._input_1 = [value]
        self.eval_inputs()

    @input_1.deleter
    def input_1(self):
        print('input1 deleter')
        self._input_1 = []

    def eval_inputs(self):
        # input complete logic
        if self._input_1 is not None:
            self.fire()

    def clear_inputs(self):
        print('clear inputs')
        del self.input_1

    def fire(self, input_1=None):
        print('inside fire')
        if input_1 is None:
            input_1 = self._input_1
        self.results['result_1'] = [self.config * i for i in input_1]
        self.clear_inputs()
        print('result = %s' % self.results)
        # for connection, value_key in self._output_1:
        #     connection = self.results['value_key']


if __name__ == '__main__':
    a = ActorA(3)
    a.input_1 = 3

