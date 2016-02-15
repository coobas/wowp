#!/usr/bin/env python
from setuptools import setup, find_packages
from wowp import __version__

setup(
        name='wowp',
        version=__version__,
        packages=find_packages(),
        license='MIT',
        description='Data-flow-actors-based workflow framework',
        long_description=open('README.txt').read(),
        author='Jakub Urban, Jan Pipek',
        author_email='coobas at gmail dt com',
        url='http://pythonic.eu/wowp/',
        install_requires=['decorator',
                          'future',
                          'networkx',
                          'nose',
                          'six',
                          ],
        extras_require = {'parallel': ['ipyparallel']},
        entry_points={
            # 'console_scripts' : [
            #    'wowp = wowp:function_that_does_it_all'
            # ]
        },
        classifiers=[
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3.4",
            "Programming Language :: Python :: 3.5",
            "Development Status :: 3 - Alpha",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Intended Audience :: Developers",
            "Intended Audience :: Information Technology",
            "Intended Audience :: Science/Research",
            "Topic :: Scientific/Engineering",
            "Topic :: Software Development :: Libraries :: Python Modules"
        ],
        test_suite='nose.collector',
        tests_require='nose>=1.0'
)
