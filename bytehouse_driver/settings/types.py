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

from ..util.helpers import asbool
from ..varint import write_varint
from ..writer import write_binary_str


class SettingType(object):
    @classmethod
    def write(cls, value, buf):
        raise NotImplementedError


class SettingUInt64(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_varint(int(value), buf)


class SettingBool(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_varint(asbool(value), buf)


class SettingString(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_binary_str(value, buf)


class SettingChar(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_binary_str(value[0], buf)


class SettingFloat(SettingType):
    @classmethod
    def write(cls, value, buf):
        """
        Float is written in string representation.
        """
        write_binary_str(str(value), buf)


class SettingMaxThreads(SettingUInt64):
    @classmethod
    def write(cls, value, buf):
        if value == 'auto':
            value = 0
        super(SettingMaxThreads, cls).write(value, buf)
