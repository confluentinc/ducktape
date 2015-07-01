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
from ducktape.mark import parametrized
from ducktape.mark import matrix


class CheckParametrize(object):
    def check_simple(self):
        @parametrize(x=100, z=300)
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        assert parametrized(function)
        assert len(function) == 1

        all_f = [f for f in function]
        assert all_f[0].__name__ == "function"
        assert all_f[0].kwargs == {"x": 100, "z": 300}
        assert all_f[0]() == (100, 2, 300)

    def check_simple_method(self):
        class C(object):
            @parametrize(x=100, z=300)
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert parametrized(C.function)
        assert len(C.function) == 1

        all_f = [f for f in C.function]
        c = C()
        assert all_f[0].kwargs == {"x": 100, "z": 300}
        assert all_f[0](c) == (100, 2, 300)

    def check_stacked(self):
        @parametrize(x=100, y=200, z=300)
        @parametrize(x=100, z=300)
        @parametrize(y=200)
        @parametrize()
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        assert len(function) == 4

        all_f = [f for f in function]
        assert all_f[0].kwargs == {"x": 100, "y": 200, "z": 300}
        assert all_f[0]() == (100, 200, 300)

        assert all_f[1].kwargs == {"x": 100, "z": 300}
        assert all_f[1]() == (100, 2, 300)

        assert all_f[2].kwargs == {"y": 200}
        assert all_f[2]() == (1, 200, 3)

        assert all_f[3].kwargs == {}
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
        assert len(C.function) == 4

        all_f = [func for func in C.function]
        c = C()
        assert all_f[0].kwargs == {"x": 100, "y": 200, "z": 300}
        assert all_f[0](c) == (100, 200, 300)

        assert all_f[1].kwargs == {"x": 100, "z": 300}
        assert all_f[1](c) == (100, 2, 300)

        assert all_f[2].kwargs == {"y": 200}
        assert all_f[2](c) == (1, 200, 3)

        assert all_f[3].kwargs == {}
        assert all_f[3](c) == (1, 2, 3)


class CheckMatrix(object):
    def check_simple(self):

        @matrix(x=[1, 2], y=[-1, -2])
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        assert len(function) == 4

        all_f = [f for f in function]
        all_kwargs = [f.kwargs for f in all_f]
        assert all_f[0].__name__ == "function"
        assert sorted(all_kwargs) == sorted([
            {'x': 1, 'y': -1},
            {'x': 1, 'y': -2},
            {'x': 2, 'y': -1},
            {'x': 2, 'y': -2}])

        for f in all_f:
            assert f() == (f.kwargs['x'], f.kwargs['y'], 3)

    def check_simple_method(self):

        class C(object):
            @matrix(x=[1, 2], y=[-1, -2])
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert parametrized(C.function)
        assert len(C.function) == 4

        all_f = [f for f in C.function]
        all_kwargs = [f.kwargs for f in all_f]
        assert all_f[0].__name__ == "function"
        assert sorted(all_kwargs) == sorted([
            {'x': 1, 'y': -1},
            {'x': 1, 'y': -2},
            {'x': 2, 'y': -1},
            {'x': 2, 'y': -2}])

        for f in all_f:
            assert f(C()) == (f.kwargs['x'], f.kwargs['y'], 3)

    def check_stacked(self):
        @matrix(x=[1, 2])
        @matrix(y=[-1, -2])
        def function(x=1, y=2, z=3):
            return x, y, z

        assert parametrized(function)
        assert len(function) == 4

        all_f = [f for f in function]
        all_kwargs = [f.kwargs for f in all_f]
        assert all_f[0].__name__ == "function"
        assert sorted(all_kwargs) == sorted([
            {'x': 1, 'y': -1},
            {'x': 1, 'y': -2},
            {'x': 2, 'y': -1},
            {'x': 2, 'y': -2}])

        for f in all_f:
            assert f() == (f.kwargs['x'], f.kwargs['y'], 3)

    def check_stacked_method(self):
        class C(object):
            @matrix(x=[1, 2])
            @matrix(y=[-1, -2])
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert parametrized(C.function)
        assert len(C.function) == 4

        all_f = [f for f in C.function]
        all_kwargs = [f.kwargs for f in all_f]
        assert all_f[0].__name__ == "function"
        assert sorted(all_kwargs) == sorted([
            {'x': 1, 'y': -1},
            {'x': 1, 'y': -2},
            {'x': 2, 'y': -1},
            {'x': 2, 'y': -2}])

        for f in all_f:
            assert f(C()) == (f.kwargs['x'], f.kwargs['y'], 3)