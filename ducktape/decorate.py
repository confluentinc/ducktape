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


import functools

PARAMETRIZED = "PARAMETRIZED"
INJECTED = "INJECTED"
MATRIX = "MATRIX"
MARKS = {PARAMETRIZED, INJECTED, MATRIX}

_marked_functions = {}


def mark_as(fun, mark):
    """Attach a tag indicating that fun has been marked with the given mark"""
    if fun in _marked_functions:
        _marked_functions[fun].add(mark)
    else:
        _marked_functions[fun] = {mark}


def marked(f, mark):
    if f is None:
        return False

    try:
        if f not in _marked_functions:
            return False
    except TypeError:
        # this check throws TypeError if f is not a hashable object
        return False

    return mark in _marked_functions[f]


def _expandable(f):
    """Return True iff f 'expands' into many test functions."""
    return marked(f, PARAMETRIZED) or marked(f, MATRIX)


def parametrized(f):
    return marked(f, PARAMETRIZED)


def injected(f):
    return marked(f, INJECTED)


class TestGenerator():
    """Helper class used with @matrix and @parametrize decorators."""

    def __init__(self, wrapped, kwargs_list):
        self.kwargs_pointer = 0
        self.wrapped = wrapped
        self.kwargs_list = kwargs_list

    @property
    def test_method(self):
        """Gives access to """
        if _expandable(self.wrapped):
            return self.wrapped.test_method
        else:
            return self.wrapped

    @property
    def __name__(self):
        return self.test_method.__name__

    def __len__(self):
        return len(self.kwargs_list)

    def __iter__(self):
        return self

    def next(self):
        if self.kwargs_pointer < len(self):
            current_kwargs = self.kwargs_list[self.kwargs_pointer]
            self.kwargs_pointer += 1
            return _inject(**current_kwargs)(self.test_method)
        else:
            raise StopIteration


def i_to_indices(i, vars, values):
    assert len(vars) == len(values)
    num_matrix_elements = reduce(lambda x, y: x * len(y), values, 1)
    assert 0 <= i < num_matrix_elements

    indices = [0] * len(vars)
    for j in range(len(vars)):
        indices[j] = i % len(values[j])
        i /= len(values[j])
        if i == 0:
            break

    return indices


def matrix(**kwargs):
    vars = []
    values = []
    for v in kwargs:
        vars.append(v)
        values.append(kwargs[v])

    num_params = 0
    if len(vars) > 0:
        num_params = reduce(lambda x, y: x * len(y), values, 1)

    def parametrizer(f):
        kwargs_list = []

        for i in range(num_params):
            indices = i_to_indices(i, vars, values)
            kwargs_list.append(
                {vars[j]: values[j][indices[j]] for j in range(len(indices))})

        if _expandable(f):
            all_kwargs_list = []
            for sub_kwargs in f.kwargs_list:
                for kwargs in kwargs_list:
                    current_kwargs = kwargs.copy()
                    current_kwargs.update(sub_kwargs)
                    all_kwargs_list.append(current_kwargs)
        else:
            all_kwargs_list = kwargs_list

        wrapped = TestGenerator(f, all_kwargs_list)
        mark_as(wrapped, MATRIX)
        return wrapped
    return parametrizer


def parametrize(**kwargs):
    """Function decorator used to parametrize its arguments.

    Example::

        # decorating g with @parametrize transforms it into an iterable object.
        # Iterating yields the original g with the specified arguments injected.
        @parametrize(x=1, y=2 z=-1)
        @parametrize(x=3, y=4, z=5)
        def g(x, y, z):
            print "x = %s, y = %s, z = %s" % (x, y, z)

        for f in g:
            f()

        # output:
        # x = 1, y = 2, z = -1
        # x = 3, y = 4, z = 5
    """
    def parametrizer(f):
        kwargs_list = [kwargs]
        if _expandable(f):
            kwargs_list.extend(f.kwargs_list)

        wrapped = TestGenerator(f, kwargs_list)
        mark_as(wrapped, PARAMETRIZED)
        return wrapped
    return parametrizer


def _inject(**kwargs):
    """Inject variables into the arguments of a function or method.
    Essentially replaces the default values of the function.
    """

    def injector(f):
        assert callable(f)

        @functools.wraps(f)
        def wrapped(*w_args, **w_kwargs):
            kwargs.update(w_kwargs)
            return f(*w_args, **kwargs)

        mark_as(wrapped, INJECTED)
        wrapped.kwargs = kwargs
        return wrapped
    return injector
