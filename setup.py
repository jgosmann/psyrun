#!/usr/bin/env python

from setuptools import setup

setup(
    name='psyrun',
    version='0.1',
    author='Jan Gosmann',
    author_email='jan@hyper-world.de',
    packages=['psyrun'],
    provides=['psyrun'],
    install_requires=['configparser', 'doit', 'numpy', 'tables'],
    scripts=['scripts/psy-merge'],  # FIXME make entry point
    entry_points={
        'console_scripts': [
            'psy = psyrun.main:psy_main'
        ]
    },
)
