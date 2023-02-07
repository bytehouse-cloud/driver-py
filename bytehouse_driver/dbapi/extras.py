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

import re
from collections import namedtuple
from functools import lru_cache

from .cursor import Cursor


class DictCursor(Cursor):
    """
    A cursor that generates results as :class:`dict`.

    ``fetch*()`` methods will return dicts instead of tuples.
    """

    def fetchone(self):
        rv = super(DictCursor, self).fetchone()
        if rv is not None:
            rv = dict(zip(self._columns, rv))
        return rv

    def fetchmany(self, size=None):
        rv = super(DictCursor, self).fetchmany(size=size)
        return [dict(zip(self._columns, x)) for x in rv]

    def fetchall(self):
        rv = super(DictCursor, self).fetchall()
        return [dict(zip(self._columns, x)) for x in rv]


class NamedTupleCursor(Cursor):
    """
    A cursor that generates results as named tuples created by
    :func:`~collections.namedtuple`.

    ``fetch*()`` methods will return named tuples instead of regular tuples, so
    their elements can be accessed both as regular numeric items as well as
    attributes.
    """

    # ascii except alnum and underscore
    _re_clean = re.compile(
        '[' + re.escape(' !"#$%&\'()*+,-./:;<=>?@[\\]^`{|}~') + ']')

    @classmethod
    @lru_cache(512)
    def _make_nt(self, key):
        fields = []
        for s in key:
            s = self._re_clean.sub('_', s)
            # Python identifier cannot start with numbers, namedtuple fields
            # cannot start with underscore.
            if s[0] == '_' or '0' <= s[0] <= '9':
                s = 'f' + s
            fields.append(s)

        return namedtuple('Record', fields)

    def fetchone(self):
        rv = super(NamedTupleCursor, self).fetchone()
        if rv is not None:
            nt = self._make_nt(self._columns)
            rv = nt(*rv)
        return rv

    def fetchmany(self, size=None):
        rv = super(NamedTupleCursor, self).fetchmany(size=size)
        nt = self._make_nt(self._columns)
        return [nt(*x) for x in rv]

    def fetchall(self):
        rv = super(NamedTupleCursor, self).fetchall()
        nt = self._make_nt(self._columns)
        return [nt(*x) for x in rv]
