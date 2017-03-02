#!/usr/bin/env python

import imp
import os.path

try:
    from setuptools import find_packages, setup
except ImportError:
    raise ImportError(
        "'setuptools' is required, but not installed. "
        "See https://packaging.python.org/installing/")


version_mod = imp.load_source(
    'version',
    os.path.join(os.path.dirname(__file__), 'psyrun', 'version.py'))

with open('README.rst') as f:
    long_description = f.read()
with open('CHANGES.rst') as f:
    long_description += '\n\n' + f.read()

setup(
    name="psyrun",
    version=version_mod.version,
    author="Jan Gosmann",
    author_email="jan@hyper-world.de",
    url='https://github.com/jgosmann/psyrun',
    license="MIT",
    description="Easy parameter space evaluation and serial farming.",
    long_description=long_description,

    packages=find_packages(),
    provides=['psyrun'],

    install_requires=['six'],
    extras_require={
        'h5': ['numpy', 'tables'],
        'npz': ['numpy'],
        'parallel_map': ['joblib'],
    },

    entry_points={
        'console_scripts': [
            'psy = psyrun.main:psy_main'
        ]
    },

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
)
