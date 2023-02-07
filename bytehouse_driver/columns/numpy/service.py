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

from ..service import aliases
from ... import errors
from .datecolumn import NumpyDateColumn
from .datetimecolumn import create_numpy_datetime_column
from .floatcolumn import NumpyFloat32Column, NumpyFloat64Column
from .intcolumn import (
    NumpyInt8Column, NumpyInt16Column, NumpyInt32Column, NumpyInt64Column,
    NumpyUInt8Column, NumpyUInt16Column, NumpyUInt32Column, NumpyUInt64Column
)
from .lowcardinalitycolumn import create_numpy_low_cardinality_column
from .stringcolumn import create_string_column
from .tuplecolumn import create_tuple_column
from ..nullablecolumn import create_nullable_column

column_by_type = {c.ch_type: c for c in [
    NumpyDateColumn,
    NumpyFloat32Column, NumpyFloat64Column,
    NumpyInt8Column, NumpyInt16Column, NumpyInt32Column, NumpyInt64Column,
    NumpyUInt8Column, NumpyUInt16Column, NumpyUInt32Column, NumpyUInt64Column
]}


def get_numpy_column_by_spec(spec, column_options):
    def create_column_with_options(x):
        return get_numpy_column_by_spec(x, column_options)

    if spec == 'String' or spec.startswith('FixedString'):
        return create_string_column(spec, column_options)

    elif spec.startswith('DateTime'):
        return create_numpy_datetime_column(spec, column_options)

    elif spec.startswith('Tuple'):
        return create_tuple_column(
            spec, create_column_with_options, column_options
        )

    elif spec.startswith('Nullable'):
        return create_nullable_column(spec, create_column_with_options)

    elif spec.startswith('LowCardinality'):
        return create_numpy_low_cardinality_column(
            spec, create_column_with_options, column_options
        )
    else:
        for alias, primitive in aliases:
            if spec.startswith(alias):
                return create_column_with_options(
                    primitive + spec[len(alias):]
                )

        if spec in column_by_type:
            cls = column_by_type[spec]
            return cls(**column_options)

        raise errors.UnknownTypeError('Unknown type {}'.format(spec))
