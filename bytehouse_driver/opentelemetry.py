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

class OpenTelemetryTraceContext(object):
    traceparent_tpl = 'xx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxx-xx'
    translation = str.maketrans('1234567890abcdef', 'xxxxxxxxxxxxxxxx')

    def __init__(self, traceparent, tracestate):
        # xx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxx-xx
        # ^              ^                           ^         ^
        # version     trace_id                    span_id      flags

        self.trace_id = None  # UUID
        self.span_id = None  # UInt64
        self.tracestate = tracestate  # String
        self.trace_flags = None  # UInt8

        if traceparent is not None:
            self.parse_traceparent(traceparent)

        super(OpenTelemetryTraceContext, self).__init__()

    def parse_traceparent(self, traceparent):
        traceparent = traceparent.lower()

        if len(traceparent) != len(self.traceparent_tpl):
            raise ValueError('unexpected length {}, expected {}'.format(
                len(traceparent), len(self.traceparent_tpl)
            ))

        if traceparent.translate(self.translation) != self.traceparent_tpl:
            raise ValueError(
                'Malformed traceparant header: {}'.format(traceparent)
            )

        parts = traceparent.split('-')
        version = int(parts[0], 16)
        if version != 0:
            raise ValueError(
                'unexpected version {}, expected 00'.format(parts[0])
            )

        self.trace_id = (int(parts[1][16:], 16) << 64) + int(parts[1][:16], 16)
        self.span_id = int(parts[2], 16)
        self.trace_flags = int(parts[3], 16)
