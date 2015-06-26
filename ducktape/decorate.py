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

_marked_functions = {}


def mark_as(fun, mark):
    """Attach a tag indicating that fun has been marked with the given mark_as.

    This allows us to determine
    """
    if fun in _marked_functions:
        _marked_functions[fun].add(mark)
    else:
        _marked_functions[fun] = {mark}


def marked(f, mark):
    if f is None:
        return False

    if f not in _marked_functions:
        return False

    return mark in _marked_functions[f]

def is_multi(f):
    return marked(f, PARAMETRIZED) or marked(f, MATRIX)


def parametrized(f):
    return marked(f, PARAMETRIZED)


def injected(f):
    return marked(f, INJECTED)


class TestGenerator():
    def __init__(self, wrapped, kwargs_list):
        self.kwargs_pointer = 0
        self.f_pointer = 0
        self.kwargs_list = kwargs_list
        self.wrapped = wrapped

    @property
    def test_method(self):
        if is_multi(self.wrapped):
            return self.wrapped.test_method
        else:
            return self.wrapped

    def __len__(self):
        return len(self.kwargs_list)

    def __iter__(self):
        return self

    def next(self):
        if self.kwargs_pointer < len(self):
            current_func = inject(**self.kwargs_list[self.kwargs_pointer])(self.test_method)
            self.kwargs_pointer += 1
            return current_func
        else:
            if is_multi(self.wrapped) and self.f_pointer < len(self.wrapped):
                self.f_pointer += 1
                return self.wrapped.next()
            else:
                raise StopIteration


class MatrixTestGenerator():
    def __init__(self, wrapped, kwargs_list):
        self.kwargs_pointer = 0
        self._top_level_kwargs_list = kwargs_list
        self._kwargs_list = None
        self.wrapped = wrapped

    @property
    def kwargs_list(self):
        if self._kwargs_list is None:
            if is_multi(self.wrapped):
                self._kwargs_list = []
                for sub_kwargs in self.wrapped.kwargs_list:
                    for kwargs in self._top_level_kwargs_list:
                        current_kwargs = kwargs.copy()
                        current_kwargs.update(sub_kwargs)
                        self._kwargs_list.append(current_kwargs)
            else:
                self._kwargs_list = self._top_level_kwargs_list

        return self._kwargs_list

    @property
    def test_method(self):
        if is_multi(self.wrapped):
            return self.wrapped.test_method
        else:
            return self.wrapped

    def __len__(self):
        return len(self.kwargs_list)

    def __iter__(self):
        return self

    def next(self):
        if self.kwargs_pointer < len(self):
            current_kwargs = self.kwargs_list[self.kwargs_pointer]
            self.kwargs_pointer += 1
            return inject(**current_kwargs)(self.test_method)
        else:
            raise StopIteration


def i_to_indices(i, vars, values):
    assert len(vars) == len(values)

    indices = [0] * len(vars)
    for j in range(len(vars)):
        indices[j] = i % len(values[j])
        i /= len(values[j])
        if i == 0:
            break

    return indices


def matrix(**kwargs):
    """
    Note: ordering won't be guaranteed.
    """
    vars = []
    values = []
    for v in kwargs:
        vars.append(v)
        values.append(kwargs[v])

    if len(vars) > 0:
        num_params = 1
        for v in vars:
            num_params *= len(kwargs[v])

    def parametrizer(f):
        kwargs_list = []

        for i in range(num_params):
            indices = i_to_indices(i, vars, values)
            kwargs_list.append(
                {vars[j]: values[j][indices[j]] for j in range(len(indices))})


        wrapped = MatrixTestGenerator(f, kwargs_list)
        mark_as(wrapped, MATRIX)
        return wrapped
    return parametrizer


def parametrize(**kwargs):
    """Function decorator used to parametrize its arguments.

    Example::
        @parametrize(x=[1,  2,  3],
                     y=[10, 10, 12],
                     z=[-1, -2, -3])
        def g(x, y, z):
            print "x = %s, y = %s, z = %s" % (x, y, z)

        # g is a function marked as parametrized
        # calling g will now return a generator object:
        for f in g():
            f()

        # output:
        # x = 1, y = 10, z = -1
        # x = 2, y = 10, z = -2
        # x = 3, y = 12, z = -3
    """
    vars = kwargs.keys()
    if len(vars) > 0:
        num_params = len(kwargs[vars[0]])
        for param_list in kwargs.values():
            assert len(param_list) == num_params, "params: %s, num_params: %s" % (param_list, num_params)

    def parametrizer(f):
        kwargs_list = []
        for i in range(num_params):
            kwargs_list.append({v: kwargs[v][i] for v in vars})

        wrapped = TestGenerator(f, kwargs_list)
        mark_as(wrapped, PARAMETRIZED)
        return wrapped
    return parametrizer


def inject(**kwargs):
    """Inject variables into the arguments of a function or method."""

    def injector(f):
        @functools.wraps(f)
        def wrapped(*w_args, **w_kwargs):
            kwargs.update(w_kwargs)
            return f(*w_args, **kwargs)

        mark_as(wrapped, INJECTED)
        wrapped.kwargs = kwargs
        return wrapped
    return injector


def main():

    # @inject(x="a", y=5, z=30)
    # def f(x, y, z):
    #     return "x=%s, y=%s, z=%s" % (x, y, z)
    #
    # print f.__name__
    # print f()
    #
    # class C(object):
    #     def __init__(self):
    #         self.a = 20
    #
    #     @inject(x=30)
    #     def f(self, x):
    #         return self.a + x
    #
    # c = C()
    # print c.f()
    # print c.f(x=10)  # can still override
    #


    class A(object):
        @inject(variables="x", vals=[10])
        def f(self, x):
            print "original method: self=%s, x=%s" % (str(self), str(x))
            print x * x
            return x * x

        @parametrize(x=[2, 4, 6])
        @parametrize(x=[-1, -3])
        def g(self, x):
            print "original method: self=%s, x=%s" % (str(self), str(x))
            return x * x

        @matrix(x=[1, 3, 5], y=[1, 2], z=[-1, 1])
        def m(self, x, y, z):
            return [x, y, z]

        @matrix(x=[-1, 1])
        @matrix(y=[2, 3])
        @matrix(z=[0, 10])
        def m2(self, x, y, z):
            return [x, y, z]
    gen = A.g
    for fun in gen:
        print fun(A())

    print "Trying @matrix. A.m has %d functions" % len(A.m)
    for fun in A.m:
        print fun(A())

    print "Trying @matrix. A.m2 has %d functions" % len(A.m2)
    for fun in A.m2:
        print fun(A())


if __name__ == "__main__":
    main()
