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

from ipaddress import IPv4Address, IPv6Address, AddressValueError

from .. import errors
from .exceptions import ColumnTypeMismatchException
from .stringcolumn import ByteFixedString
from .intcolumn import UInt32Column


class IPv4Column(UInt32Column):
    ch_type = "IPv4"
    py_types = (str, IPv4Address, int)

    def __init__(self, types_check=False, **kwargs):
        # UIntColumn overrides before_write_item and check_item
        # in its __init__ when types_check is True so we force
        # __init__ without it then add the appropriate check method for IPv4
        super(UInt32Column, self).__init__(types_check=False, **kwargs)

        self.types_check_enabled = types_check
        if types_check:

            def check_item(value):
                if isinstance(value, int) and value < 0:
                    raise ColumnTypeMismatchException(value)

                if not isinstance(value, IPv4Address):
                    try:
                        value = IPv4Address(value)
                    except AddressValueError:
                        # Cannot parse input in a valid IPv4
                        raise ColumnTypeMismatchException(value)

            self.check_item = check_item

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is None:
            return tuple(IPv4Address(item) for item in items)
        else:
            return tuple(
                (None if is_null else IPv4Address(items[i]))
                for i, is_null in enumerate(nulls_map)
            )

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            # allow Ipv4 in integer, string or IPv4Address object
            try:
                if isinstance(item, int):
                    continue

                if not isinstance(item, IPv4Address):
                    item = IPv4Address(item)

                items[i] = int(item)
            except AddressValueError:
                raise errors.CannotParseDomainError(
                    "Cannot parse IPv4 '{}'".format(item)
                )


class IPv6Column(ByteFixedString):
    ch_type = "IPv6"
    py_types = (str, IPv6Address, bytes)

    def __init__(self, types_check=False, **kwargs):
        super(IPv6Column, self).__init__(16, types_check=types_check, **kwargs)

        if types_check:

            def check_item(value):
                if isinstance(value, bytes) and len(value) != 16:
                    raise ColumnTypeMismatchException(value)

                if not isinstance(value, IPv6Address):
                    try:
                        value = IPv6Address(value)
                    except AddressValueError:
                        # Cannot parse input in a valid IPv6
                        raise ColumnTypeMismatchException(value)

            self.check_item = check_item

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is None:
            return tuple(IPv6Address(item) for item in items)
        else:
            return tuple(
                (None if is_null else IPv6Address(items[i]))
                for i, is_null in enumerate(nulls_map)
            )

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            # allow Ipv6 in bytes or python IPv6Address
            # this is raw bytes (not encoded) in order to fit FixedString(16)
            try:
                if isinstance(item, bytes):
                    continue

                if not isinstance(item, IPv6Address):
                    item = IPv6Address(item)
                items[i] = item.packed
            except AddressValueError:
                raise errors.CannotParseDomainError(
                    "Cannot parse IPv6 '{}'".format(item)
                )
