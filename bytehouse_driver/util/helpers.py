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

from itertools import islice, tee


def chunks(seq, n):
    # islice is MUCH slower than slice for lists and tuples.
    if isinstance(seq, (list, tuple)):
        i = 0
        item = seq[i:i+n]
        while item:
            yield list(item)
            i += n
            item = seq[i:i+n]

    else:
        it = iter(seq)
        item = list(islice(it, n))
        while item:
            yield item
            item = list(islice(it, n))


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def column_chunks(columns, n):
    for column in columns:
        if not isinstance(column, (list, tuple)):
            raise TypeError(
                'Unsupported column type: {}. list or tuple is expected.'
                .format(type(column))
            )

    # create chunk generator for every column
    g = [chunks(column, n) for column in columns]

    while True:
        # get next chunk for every column
        item = [next(column, []) for column in g]
        if not any(item):
            break
        yield item


# from paste.deploy.converters
def asbool(obj):
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError('String is not true/false: %r' % obj)
    return bool(obj)
