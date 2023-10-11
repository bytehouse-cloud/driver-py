"""
This is the MIT license: http://www.opensource.org/licenses/mit-license.php

Copyright (c) 2017 by Konstantin Lebedev.

Copyright 2022- 2023 Bytedance Ltd. and/or its affiliates

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
import re
from codecs import open

from setuptools import setup, find_packages
from distutils.extension import Extension

try:
    from Cython.Build import cythonize
except ImportError:
    USE_CYTHON = False
else:
    USE_CYTHON = True

CYTHON_TRACE = bool(os.getenv('CYTHON_TRACE', False))

here = os.path.abspath(os.path.dirname(__file__))


def read_version():
    regexp = re.compile(r'^VERSION\W*=\W*\(([^\(\)]*)\)')
    init_py = os.path.join(here, 'bytehouse_driver', '__init__.py')
    with open(init_py, encoding='utf-8') as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1).replace(', ', '.')
        else:
            raise RuntimeError(
                'Cannot find version in bytehouse_driver/__init__.py'
            )


with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Prepare extensions.
ext = '.pyx' if USE_CYTHON else '.c'
extensions = [
    Extension(
        'bytehouse_driver.bufferedreader',
        ['bytehouse_driver/bufferedreader' + ext]
    ),
    Extension(
        'bytehouse_driver.bufferedwriter',
        ['bytehouse_driver/bufferedwriter' + ext]
    ),
    Extension(
        'bytehouse_driver.columns.largeint',
        ['bytehouse_driver/columns/largeint' + ext]
    ),
    Extension(
        'bytehouse_driver.varint',
        ['bytehouse_driver/varint' + ext]
    )
]

if USE_CYTHON:
    compiler_directives = {'language_level': '3'}
    if CYTHON_TRACE:
        compiler_directives['linetrace'] = True

    extensions = cythonize(extensions, compiler_directives=compiler_directives)

github_url = 'https://github.com/bytehouse-cloud/driver-py'

setup(
    name='bytehouse-driver',
    version=read_version(),

    description='Python driver with native interface for ByteHouse',
    long_description=long_description,
    long_description_content_type='text/markdown',

    url=github_url,

    author='Sayan Dutta Chowdhury',
    author_email='sayan.chowdhury@bytedance.com',

    license='MIT',

    classifiers=[
        'Development Status :: 4 - Beta',


        'Environment :: Console',


        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',


        'License :: OSI Approved :: MIT License',


        'Operating System :: OS Independent',


        'Programming Language :: SQL',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: Implementation :: PyPy',

        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],

    keywords='ByteHouse db database cloud analytics',

    project_urls={
        'Documentation': github_url,
        'Changes': github_url + '/blob/main/CHANGELOG.md'
    },
    packages=find_packages('.', exclude=['tests*']),
    python_requires='>=3.6, <4',
    install_requires=[
        'pytz',
        'tzlocal'
    ],
    ext_modules=extensions,
    extras_require={
        'lz4': [
            'lz4<=3.0.1; implementation_name=="pypy"',
            'lz4; implementation_name!="pypy"',
            'clickhouse-cityhash>=1.0.2.1'
        ],
        'zstd': ['zstd', 'clickhouse-cityhash>=1.0.2.1'],
        'numpy': ['numpy>=1.12.0', 'pandas>=0.24.0']
    },
    test_suite='pytest'
)
