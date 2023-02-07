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

from enum import Enum

from .. import errors
from .intcolumn import IntColumn


class EnumColumn(IntColumn):
    py_types = (Enum, int, str)

    def __init__(self, enum_cls, **kwargs):
        self.enum_cls = enum_cls
        super(EnumColumn, self).__init__(**kwargs)

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        enum_cls = self.enum_cls

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            source_value = item.name if isinstance(item, Enum) else item

            # Check real enum value
            try:
                if isinstance(source_value, str):
                    items[i] = enum_cls[source_value].value
                else:
                    items[i] = enum_cls(source_value).value
            except (ValueError, KeyError):
                choices = ', '.join(
                    "'{}' = {}".format(x.name.replace("'", r"\'"), x.value)
                    for x in enum_cls
                )
                enum_str = '{}({})'.format(enum_cls.__name__, choices)

                raise errors.LogicalError(
                    "Unknown element '{}' for type {}"
                    .format(source_value, enum_str)
                )

    def after_read_items(self, items, nulls_map=None):
        enum_cls = self.enum_cls

        if nulls_map is None:
            return tuple(enum_cls(item).name for item in items)
        else:
            return tuple(
                (None if is_null else enum_cls(items[i]).name)
                for i, is_null in enumerate(nulls_map)
            )


class Enum8Column(EnumColumn):
    ch_type = 'Enum8'
    format = 'b'
    int_size = 1


class Enum16Column(EnumColumn):
    ch_type = 'Enum16'
    format = 'h'
    int_size = 2


def create_enum_column(spec, column_options):
    if spec.startswith('Enum8'):
        params = spec[6:-1]
        cls = Enum8Column
    else:
        params = spec[7:-1]
        cls = Enum16Column

    return cls(Enum(cls.ch_type, _parse_options(params)), **column_options)


def _parse_options(option_string):
    options = dict()
    after_name = False
    escaped = False
    quote_character = None
    name = ''
    value = ''

    for ch in option_string:
        if escaped:
            name += ch
            escaped = False  # accepting escaped character

        elif after_name:
            if ch in (' ', '='):
                pass
            elif ch == ',':
                options[name] = int(value)
                after_name = False
                name = ''
                value = ''  # reset before collecting new option
            else:
                value += ch

        elif quote_character:
            if ch == '\\':
                escaped = True
            elif ch == quote_character:
                quote_character = None
                after_name = True  # start collecting option value
            else:
                name += ch

        else:
            if ch == "'":
                quote_character = ch

    if after_name:
        options.setdefault(name, int(value))  # append word after last comma

    return options
