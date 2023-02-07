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

def get_inner_spec(column_name, spec):
    brackets = 0
    offset = len(column_name)
    i = offset
    for i, ch in enumerate(spec[offset:], offset):
        if ch == '(':
            brackets += 1

        elif ch == ')':
            brackets -= 1

        if brackets == 0:
            break

    return spec[offset + 1:i]


def get_inner_columns(spec):
    brackets = 0
    column_begin = 0

    columns = []
    for i, x in enumerate(spec + ','):
        if x == ',':
            if brackets == 0:
                columns.append(spec[column_begin:i])
                column_begin = i + 1
        elif x == '(':
            brackets += 1
        elif x == ')':
            brackets -= 1
        elif x == ' ':
            if brackets == 0:
                column_begin = i + 1
    return columns


def get_inner_columns_with_types(spec):
    brackets = 0
    prev_comma = 0
    prev_space = 0

    columns = []
    for i, x in enumerate(spec + ','):
        if x == ',':
            if brackets == 0:
                columns.append((
                    spec[prev_comma:prev_space].strip(),
                    spec[prev_space:i]
                ))
                prev_comma = i + 1
        elif x == '(':
            brackets += 1
        elif x == ')':
            brackets -= 1
        elif x == ' ':
            if brackets == 0:
                prev_space = i + 1
    return columns
