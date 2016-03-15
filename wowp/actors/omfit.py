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
               shell='/bin/bash',
               print_output=True,
               binary_mode=False):
    import subprocess
    import tempfile
    import sys
    from tempfile import mkdtemp
    from glob import glob

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

        # construct the full command, including timeout if provided
        if timeout is not None:
            # TODO use Pythonic timeout rather than spawn
            full_command = ('''
    cd {}
    expect << EOF
    set timeout {}
    spawn {}
    expect eof
    EOF
    ''').format(tmpdir, timeout, command)
        else:
            full_command = ('cd {}\n{}\n').format(tmpdir, command)

        if print_output:
            print('run command:\n{}'.format(full_command))

        if binary_mode:
            mode = "w+b"
        else:
            mode = "w+t"

        with tempfile.TemporaryFile(mode=mode) as fout, tempfile.TemporaryFile(
                mode=mode) as ferr:

            if not isinstance(shell, six.string_types):
                executable = None
            else:
                executable = shell

            result = subprocess.call(full_command,
                                     stdout=fout,
                                     stderr=ferr,
                                     executable=executable,
                                     shell=True)
            fout.seek(0)
            ferr.seek(0)
            cout = fout.read()
            cerr = ferr.read()

        if print_output:
            sys.stdout.write(cout)
            sys.stderr.write(cerr)

        # get output file names (abspaths)
        if isinstance(files_out, six.string_types):
            files_out = (files_out, )
        output_file_names = [os.path.join(tmpdir, fout) for fout in files_out]

    finally:
        # TODO clean up tmpdir ???
        pass

    res = {'ret': result,
           'stdout': cout,
           'stderr': cerr,
           'output_file_names': output_file_names}

    return res


class FileCommand(Actor):
    def __init__(self,
                 name=None,
                 command='false',
                 input_files=(),
                 output_files=(),
                 workdir=None,
                 shell='/bin/bash',
                 print_output=True,
                 timeout=None):
        super(FileCommand, self).__init__(name=name)

        # use input and output file names as ports
        if isinstance(input_files, six.string_types):
            input_files = (input_files, )
        if isinstance(output_files, six.string_types):
            output_files = (output_files, )
        # TODO this fails for file names that are not valid identifier strings (e.g. contain .)
        for in_file in input_files:
            self.inports.append(in_file)
        for out_file in output_files:
            self.outports.append(out_file)

        self.command = command
        self.workdir = workdir
        self.shell = shell
        self.timeout = timeout

    def get_run_args(self):
        # get the input file name
        files_in = []
        files_out = []
        for inport in self.inports:
            files_in.append((os.path.abspath(inport.pop()), inport.name))
        for outport in self.outports:
            files_out.append(outport.name)
        args = ()
        kwargs = {'files_in': files_in,
                  'files_out': files_out,
                  'workdir': self.workdir,
                  'command': self.command,
                  'shell': self.shell,
                  'timeout': self.timeout}

        return args, kwargs

    @staticmethod
    def run(*args, **kwargs):

        shell_res = _shell_run(kwargs['command'],
                               kwargs['workdir'],
                               files_in=kwargs['files_in'],
                               files_out=kwargs['files_out'],
                               timeout=kwargs['timeout'],
                               shell=kwargs['shell'],
                               print_output=True,
                               binary_mode=False)

        # possibly look at shell_res here (for error etc)

        if shell_res['ret'] != 0:
            raise Exception('error running toq')

        res = {}
        for name, path in zip(kwargs['files_out'], shell_res['output_file_names']):
            res[name] = path
        return res


def how_to_test():
    from tempfile import mkdtemp
    workdir = mkdtemp()
    print('running in {}'.format(workdir))
    input_file_name = 'my_input_file'
    output_file_name = 'my_output_file'
    open(os.path.join(workdir, input_file_name), 'w').write('This is my input file :-P')

    from wowp.actors.omfit import FileCommand

    command = 'echo "I am TOQ" > {output}; cat {input} >> {output}'.format(
        input=input_file_name,
        output=output_file_name)

    toq = FileCommand(command=command,
                      input_files=input_file_name,
                      output_files=output_file_name,
                      workdir=workdir,
                      timeout=None)

    kwargs = {input_file_name: os.path.join(workdir, input_file_name)}
    res = toq(**kwargs)
    print('output file content:')
    print(open(res[output_file_name]).read())
