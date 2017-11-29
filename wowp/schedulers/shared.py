"""Common classes for various scheduler mechanisms."""
from __future__ import absolute_import, division, print_function, unicode_literals

from wowp.components import Composite


class ActorRunner(object):
    """Base class for objects that run actors and process their results.

    It is ok, if the runner is a scheduler at the same time. The separation
    of concepts exists only for the cases when a scheduler needs to run
    actors in parallel (such as ThreadedScheduler).
    """

    def on_outport_put_value(self, outport):
        '''
        Propagates values put into an output port.

        Must be called after outport.put
        :param outport: output port
        :return: None
        '''
        if outport.connections:
            value = outport.pop()
            for inport in outport.connections:
                self.put_value(inport, value)

    def run_actor(self, actor):
        # print("Run actor")
        # TODO replace by an attribute / method call
        if isinstance(actor, Composite):
            self.run_workflow(actor)
        else:
            actor.scheduler = self
            args, kwargs = actor.get_run_args()
            result = actor.run(*args, **kwargs)
            # print("Result: ", result)
            if not result:
                return
            else:
                out_names = actor.outports.keys()
                if not hasattr(result, 'items'):
                    raise ValueError('The execute method must return '
                                     'a dict-like object with items method')
                for name, value in result.items():
                    if name in out_names:
                        outport = actor.outports[name]
                        outport.put(value)
                        self.on_outport_put_value(outport)
                    else:
                        raise ValueError("{} not in output ports".format(name))

    def run_workflow(self, workflow, **kwargs):
        inport_names = tuple(port.name for port in workflow.inports)
        if workflow.scheduler is not None:
            # TODO this seems a bit strange
            scheduler = workflow.scheduler
        else:
            scheduler = self
        for key, value in kwargs.items():
            if key not in inport_names:
                raise ValueError('{} is not an inport name'.format(key))
            inport = workflow.inports[key]
            # put values to connected ports
            scheduler.put_value(inport, kwargs[inport.name])
        # TODO can this be run inside self.execute itsef?
        scheduler.execute()

    def reset(self):
        """Reset the scheduler
        """
        # by default, this method does nothing
        pass

    def shutdown(self):
        pass
