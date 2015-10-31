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

from ducktape.mark import parametrize
from ducktape.mark._mark import parametrized, MarkedFunctionExpander
from ducktape.mark import matrix


class CheckParametrize(object):
    def check_simple(self):
        @parametrize(x=100, z=300)
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        injected_args_list = [m.injected_args for m in function.marks]
        assert len(injected_args_list) == 1
        assert injected_args_list[0] == {"x": 100, "z": 300}

        context_list = MarkedFunctionExpander(function=function).expand()

        all_f = [cxt.function for cxt in context_list]
        assert all_f[0]() == (100, 2, 300)

    def check_simple_method(self):
        class C(object):
            @parametrize(x=100, z=300)
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert parametrized(C.function)
        injected_args_list = [m.injected_args for m in C.function.marks]
        assert len(injected_args_list) == 1
        assert injected_args_list[0] == {"x": 100, "z": 300}

        context_list = MarkedFunctionExpander(None, function=C.function).expand()
        all_f = [cxt.function for cxt in context_list]
        assert all_f[0](C()) == (100, 2, 300)

    def check_stacked(self):
        @parametrize(x=100, y=200, z=300)
        @parametrize(x=100, z=300)
        @parametrize(y=200)
        @parametrize()
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        injected_args_list = [m.injected_args for m in function.marks]
        assert len(injected_args_list) == 4

        context_list = MarkedFunctionExpander(None, function=function).expand()
        all_f = [cxt.function for cxt in context_list]
        assert all_f[0]() == (100, 200, 300)
        assert all_f[1]() == (100, 2, 300)
        assert all_f[2]() == (1, 200, 3)
        assert all_f[3]() == (1, 2, 3)

    def check_stacked_method(self):
        class C(object):
            @parametrize(x=100, y=200, z=300)
            @parametrize(x=100, z=300)
            @parametrize(y=200)
            @parametrize()
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert parametrized(C.function)
        injected_args_list = [m.injected_args for m in C.function.marks]
        assert len(injected_args_list) == 4

        context_list = MarkedFunctionExpander(None, function=C.function).expand()
        all_f = [cxt.function for cxt in context_list]
        c = C()
        assert all_f[0](c) == (100, 200, 300)
        assert all_f[1](c) == (100, 2, 300)
        assert all_f[2](c) == (1, 200, 3)
        assert all_f[3](c) == (1, 2, 3)


class CheckMatrix(object):
    def check_simple(self):

        @matrix(x=[1, 2], y=[-1, -2])
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        injected_args_list = [m.injected_args for m in function.marks]
        assert len(injected_args_list) == 1

        context_list = MarkedFunctionExpander(None, function=function).expand()
        assert len(context_list) == 4

        for ctx in context_list:
            f = ctx.function
            injected_args = ctx.injected_args
            assert f() == (injected_args['x'], injected_args['y'], 3)

    def check_simple_method(self):

        class C(object):
            @matrix(x=[1, 2], y=[-1, -2])
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert parametrized(C.function)
        injected_args_list = [m.injected_args for m in C.function.marks]
        assert len(injected_args_list) == 1

        context_list = MarkedFunctionExpander(None, function=C.function).expand()
        assert len(context_list) == 4

        c = C()
        for ctx in context_list:
            f = ctx.function
            injected_args = ctx.injected_args
            assert f(c) == (injected_args['x'], injected_args['y'], 3)

    def check_stacked(self):
        @matrix(x=[1, 2], y=[0])
        @matrix(x=[-1], z=[-10])
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        injected_args_list = [m.injected_args for m in function.marks]
        assert len(injected_args_list) == 2

        context_list = MarkedFunctionExpander(None, function=function).expand()
        assert len(context_list) == 3

        expected_output = {(1, 0, 3), (2, 0, 3), (-1, 2, -10)}
        output = set()
        for c in context_list:
            output.add(c.function())

        assert output == expected_output

    def check_stacked_method(self):
        class C(object):
            @matrix(x=[1, 2], y=[0])
            @matrix(x=[-1], z=[-10])
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert parametrized(C.function)
        injected_args_list = [m.injected_args for m in C.function.marks]
        assert len(injected_args_list) == 2

        context_list = MarkedFunctionExpander(None, function=C.function).expand()
        assert len(context_list) == 3

        expected_output = {(1, 0, 3), (2, 0, 3), (-1, 2, -10)}
        output = set()
        for ctx in context_list:
            output.add(ctx.function(C()))

        assert output == expected_output