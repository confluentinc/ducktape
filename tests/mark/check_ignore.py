# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark.mark_expander import MarkedFunctionExpander
from ducktape.mark import ignore, ignored, parametrize, matrix

import pytest


class CheckIgnore(object):
    def check_simple(self):
        @ignore
        def function(x=1, y=2, z=3):
            return x, y, z

        assert ignored(function)
        context_list = MarkedFunctionExpander(function=function).expand()
        assert len(context_list) == 1
        assert context_list[0].ignore

    def check_simple_method(self):
        class C(object):
            @ignore
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert ignored(C.function)
        context_list = MarkedFunctionExpander(function=C.function, cls=C).expand()
        assert len(context_list) == 1
        assert context_list[0].ignore

    def check_ignore_all(self):
        @ignore
        @parametrize(x=100, y=200, z=300)
        @parametrize(x=100, z=300)
        @parametrize(y=200)
        @parametrize()
        def function(x=1, y=2, z=3):
            return x, y, z

        assert ignored(function)
        context_list = MarkedFunctionExpander(function=function).expand()
        assert len(context_list) == 4
        for ctx in context_list:
            assert ctx.ignore

    def check_ignore_all_method(self):
        """Check @ignore() with no arguments used with various parametrizations on a method."""
        class C(object):
            @ignore
            @parametrize(x=100, y=200, z=300)
            @parametrize(x=100, z=300)
            @parametrize(y=200)
            @matrix(x=[1, 2, 3])
            @parametrize()
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert ignored(C.function)
        context_list = MarkedFunctionExpander(function=C.function, cls=C).expand()
        assert len(context_list) == 7
        for ctx in context_list:
            assert ctx.ignore

    def check_ignore_specific(self):

        @ignore(x=100, y=200, z=300)
        @parametrize(x=100, y=200, z=300)
        @parametrize(x=100, z=300)
        @parametrize(y=200)
        @parametrize()
        def function(x=1, y=2, z=3):
            return x, y, z

        assert ignored(function)
        context_list = MarkedFunctionExpander(None, function=function).expand()
        assert len(context_list) == 4
        for ctx in context_list:
            if ctx.injected_args == {"x": 100, "y": 200, "z": 300}:
                assert ctx.ignore
            else:
                assert not ctx.ignore

    def check_ignore_specific_method(self):
        class C(object):
            @ignore(x=100, y=200, z=300)
            @parametrize(x=100, y=200, z=300)
            @parametrize(x=100, z=300)
            @parametrize(y=200)
            @parametrize()
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert ignored(C.function)
        context_list = MarkedFunctionExpander(function=C.function, cls=C).expand()
        assert len(context_list) == 4
        for ctx in context_list:
            if ctx.injected_args == {"x": 100, "y": 200, "z": 300}:
                assert ctx.ignore
            else:
                assert not ctx.ignore

    def check_invalid_specific_ignore(self):
        """If there are no test cases to which ignore applies, it should raise an error
        Keeping in mind annotations "point down": they only apply to test cases physically below.
        """
        class C(object):
            @parametrize(x=100, y=200, z=300)
            @parametrize(x=100, z=300)
            @parametrize(y=200)
            @parametrize()
            @ignore(x=100, y=200, z=300)
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert ignored(C.function)
        with pytest.raises(AssertionError):
            MarkedFunctionExpander(function=C.function, cls=C).expand()

    def check_invalid_ignore_all(self):
        """If there are no test cases to which ignore applies, it should raise an error"""
        class C(object):
            @parametrize(x=100, y=200, z=300)
            @parametrize(x=100, z=300)
            @parametrize(y=200)
            @parametrize()
            @ignore
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert ignored(C.function)
        with pytest.raises(AssertionError):
            MarkedFunctionExpander(function=C.function, cls=C).expand()
