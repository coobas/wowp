"""OMFIT actors
"""

from wowp.components import Actor
import six
import os
from wowp.logger import logger


def _shell_run(command,
               workdir,
               files_in=(),
               files_out=(),
               timeout=None,
               cleanup=False,
               shell='/bin/bash',
               print_output=True,
               binary_mode=False):
    import subprocess
    import tempfile
    import sys
    from tempfile import mkdtemp
    from glob import glob
    import time

    if workdir is None:
        # TODO handle creating new workdirs
        raise NotImplementedError()

    tmpdir = mkdtemp(dir=workdir)
    print('tmpdir: {}'.format(tmpdir))

    try:
        # put everything into try - finally to clean up tmpdir
        try:
            # link all files in workdir --> should be a cheap and safe operation
            # TODO os.link available only on Unix in 2.7 (the scipt below as well anyway)
            # link input files
            if isinstance(files_in, six.string_types):
                files_in = (files_in, )
            for glob_in in files_in:
                print('glob_in: {}'.format(glob_in))
                if isinstance(glob_in, six.string_types):
                    # use glob in this case, do not rename files
                    # for in_file_name in glob(os.path.join(workdir, glob_in)):
                    for in_file_name in glob(glob_in):
                        print('in_file_name: {}'.format(in_file_name))
                        # source = os.path.join(workdir, os.path.basename(in_file_name))
                        source = os.path.abspath(in_file_name)
                        target = os.path.join(tmpdir, os.path.basename(in_file_name))
                        print('link({}, {})'.format(source, target))
                        os.link(source, target)
                else:
                    # do not use glob if (source, target) filenames are provided
                    # source = os.path.join(workdir, os.path.basename(glob_in[0]))
                    source = os.path.abspath(glob_in[0])
                    target = os.path.join(tmpdir, os.path.basename(glob_in[1]))
                    print('link("{}", "{}")'.format(source, target))
                    os.link(source, target)

        except Exception:
            logger.error('Error linking input files')
            raise

        # construct the full command
        full_command = ('cd {}\n{}\n').format(tmpdir, command)

        if print_output:
            print('run command:\n{}'.format(full_command))

        if binary_mode:
            mode = "w+b"
        else:
            mode = "w+t"

        with tempfile.NamedTemporaryFile(mode=mode,
                                         prefix='stdout',
                                         dir=tmpdir) as fout, tempfile.NamedTemporaryFile(
                                             mode=mode,
                                             prefix='stderr',
                                             dir=tmpdir) as ferr:

            if not isinstance(shell, six.string_types):
                executable = None
            else:
                executable = shell

            if timeout is None:
                result = subprocess.call(full_command,
                                         stdout=fout,
                                         stderr=ferr,
                                         executable=executable,
                                         shell=True)
            else:
                maxtime = time.time() + timeout
                proc = subprocess.Popen(full_command,
                                        executable=executable,
                                        stdout=fout,
                                        stderr=ferr,
                                        shell=True)
                while proc.poll() is None:
                    time.sleep(timeout * 0.01)
                    if time.time() > maxtime:
                        proc.terminate()
                        if print_output:
                            fout.seek(0)
                            ferr.seek(0)
                            cout = fout.read()
                            cerr = ferr.read()
                            sys.stdout.write(cout)
                            sys.stderr.write(cerr)
                        raise Exception('time out expired in {}'.format(full_command))

                result = proc.returncode

            fout.seek(0)
            ferr.seek(0)
            cout = fout.read()
            cerr = ferr.read()

        if print_output:
            sys.stdout.write(cout)
            sys.stderr.write(cerr)

    finally:
        # cleanup the temp directory
        if cleanup:
            for fname in os.listdir(tmpdir):
                if fname not in files_out:
                    os.remove(os.path.join(tmpdir, fname))

    res = {'ret': result, 'stdout': cout, 'stderr': cerr, 'tmpdir': tmpdir}

    return res


class FileCommand(Actor):
    """Use shell command to process files

    Args:
        name (str): the actor name
        command (str): the command to be executed
        input_files: a list of (file_name, port_name) mappings
        output_files: a list of (file_name, port_name) mappings
        workdir (str): where temporary directories will be created
        shell_res: add shell_res output port with shell results and text outputs
        single_out: join outputs into a single dict
        shell(str): the shell path [/bin/bash]
        print_output (bool): print the std out/err [True]
        timeout (float): maximum run time for the executable
        cleanup (bool): cleanup temporary directories
        raise_error (bool): raise and exception in case of an error in the shell process [True]
    """

    def __init__(self,
                 name,
                 command,
                 input_files=(),
                 output_files=(),
                 workdir=None,
                 shell_res=False,
                 single_out=False,
                 shell='/bin/bash',
                 print_output=True,
                 timeout=None,
                 cleanup=False,
                 raise_error=True):
        super(FileCommand, self).__init__(name=name)

        # use input and output file names as ports
        if isinstance(input_files, six.string_types):
            input_files = (input_files, )
        if isinstance(output_files, six.string_types):
            output_files = (output_files, )
        # map of port name -> file name
        self.inports_map = {}
        self.outports_map = {}
        for in_file in input_files:
            if isinstance(in_file, six.string_types):
                file_name, port_name = in_file, in_file
            else:
                file_name, port_name = in_file
            self.inports_map[port_name] = file_name
            self.inports.append(port_name)
        if single_out:
            self.outports.append('out')
        for out_file in output_files:
            if isinstance(out_file, six.string_types):
                file_name, port_name = out_file, out_file
            else:
                file_name, port_name = out_file
            self.outports_map[port_name] = file_name
            if not single_out:
                self.outports.append(port_name)

        if shell_res and not single_out:
            self.outports.append('shell_res')

        self.command = command
        self.workdir = workdir
        self.single_out = single_out
        self.shell = shell
        self.timeout = timeout
        self.cleanup = bool(cleanup)
        self.shell_res = bool(shell_res)
        self.raise_error = bool(raise_error)

    def get_run_args(self):
        # get the input file names
        files_in = []
        files_out = []
        # for inport in self.inports:
        #     files_in.append((os.path.abspath(inport.pop()), self.inports_map[inport.name]))
        # for outport in self.outports:
        #     files_out.append(self.outports_map[outport.name])
        for port_name, file_name in self.inports_map.items():
            files_in.append((os.path.abspath(self.inports[port_name].pop()), file_name))
        for port_name, file_name in self.outports_map.items():
            files_out.append(file_name)
        args = ()
        kwargs = {'files_in': files_in,
                  'outports_map': self.outports_map,
                  'workdir': self.workdir,
                  'command': self.command,
                  'shell': self.shell,
                  'single_out': self.single_out,
                  'timeout': self.timeout,
                  'raise_error': self.raise_error,
                  'shell_res': self.shell_res,
                  'cleanup': self.cleanup}

        return args, kwargs

    @staticmethod
    def run(*args, **kwargs):

        shell_res = _shell_run(kwargs['command'],
                               kwargs['workdir'],
                               files_in=kwargs['files_in'],
                               files_out=kwargs['outports_map'].values(),
                               timeout=kwargs['timeout'],
                               shell=kwargs['shell'],
                               print_output=True,
                               cleanup=kwargs['cleanup'],
                               binary_mode=False)

        # possibly look at shell_res here (for error etc)

        if shell_res['ret'] != 0:
            if kwargs['raise_error']:
                raise Exception('error running toq:\n{}\n{}'.format(shell_res['stdout'],
                                                                    shell_res['stderr']))

        # puth output file names into output ports
        res = {}

        for port_name, file_name in kwargs['outports_map'].items():
            res[port_name] = os.path.join(shell_res['tmpdir'], file_name)
        if kwargs['shell_res']:
            res['shell_res'] = shell_res
        if kwargs['single_out']:
            res = {'out': res}
        return res


def how_to_test():
    from tempfile import mkdtemp
    workdir = mkdtemp()
    print('running in {}'.format(workdir))
    input_file_name_tmp = 'my_input_file_tmp.txt'
    input_file_name = 'my_input_file.txt'
    input_port_name = 'my_input'
    output_file_name = 'my_output_file.txt'
    output_port_name = 'my_output'
    open(
        os.path.join(workdir, input_file_name_tmp),
        'w').write('This is my input file :-P')

    from wowp.actors.omfit import FileCommand

    command = 'echo "I am TOQ" > {output}; cat {input} >> {output}; sleep 1'.format(
        input=input_file_name,
        output=output_file_name)

    toq = FileCommand('toq_actor',
                      command=command,
                      input_files=((input_file_name, input_port_name), ),
                      output_files=((output_file_name, output_port_name), ),
                      workdir=workdir,
                      timeout=None,
                      cleanup=False)

    kwargs = {input_port_name: os.path.join(workdir, input_file_name_tmp)}
    res = toq(**kwargs)
    print('output file content:')
    print(open(res[output_port_name]).read())
