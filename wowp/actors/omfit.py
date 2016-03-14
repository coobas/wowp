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

        # link back output files
        if isinstance(files_out, six.string_types):
            files_out = (files_out, )
        # output_file_names = []
        # for out_glob in files_out:
        #     for out_file_name in glob(os.path.join(tmpdir, out_glob)):
        #         # TODO what about subdirectories?
        #         source = os.path.join(tmpdir, os.path.basename(out_file_name))
        #         target = os.path.join(workdir, os.path.basename(out_file_name))
        #         if os.path.exists(target):
        #             # rename output file if it already exists
        #             # this means there was a CONFLICT
        #             target = target + '_' + os.path.basename(tmpdir)
        #         os.link(source, target)
        #         output_file_names.append(target)
        output_file_names = [os.path.join(tmpdir, fout) for fout in files_out]


    finally:
        # TODO clean up tmpdir ???
        pass

    res = {'ret': result,
           'stdout': cout,
           'stderr': cerr,
           'output_file_names': output_file_names}

    return res


class TOQ(Actor):
    def __init__(self,
                 name='TOQ',
                 command='toq.x',
                 input_files=(),
                 output_files=(),
                 workdir=None,
                 shell='/bin/bash',
                 print_output=True,
                 timeout=None):
        super(TOQ, self).__init__(name=name)

        # use input and output file names as ports
        if isinstance(input_files, six.string_types):
            input_files = (input_files, )
        if isinstance(output_files, six.string_types):
            output_files = (output_files, )
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
        # assume working directory is the one that contains the input file
        # if files_in and os.path.isdir(os.path.dirname(files_in[0][0])):
        #     workdir = os.path.abspath(os.path.dirname(files_in[0][0]))
        # else:
        #     # this will create a new workdir
        #     workdir = None
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
                               files_out='toq_out',
                               timeout=kwargs['timeout'],
                               shell=kwargs['shell'],
                               print_output=True,
                               binary_mode=False)

        # possibly look at shell_res

        if shell_res['ret'] != 0:
            raise Exception('error running toq')

        # res = {'toq_out': os.path.join(kwargs['workdir'], 'toq_out')}
        res = {'toq_out': shell_res['output_file_names']}
        return res


class ELITE(Actor):
    def __init__(self, name=None, elite_params=None):
        super(Rand, self).__init__(name=name)
        self.inports.append('toq_out')
        self.outports.append('elite_out')
