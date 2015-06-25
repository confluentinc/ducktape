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

_marked_functions = {}


def mark_function(fun, mark):
    """Attach a tag indicating that fun has been marked with the given mark.

    This allows us to determine
    """
    print "Marking %s as %s" % (str(fun), mark)

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


def parametrized(f):
    return f.__name__ == "test_generator"
    # return marked(f, PARAMETRIZED)


def injected(f):
    return marked(f, INJECTED)


def parametrize(variables="", variable_assignments=()):
    """Function decorator used to parametrize its arguments.

    Example::
        @parametrize(variables="x, y, z", variable_assignments=((1, 2, 3), ("x", "y", "z")))
        def g(x, y, z):
            print "x = %s, y = %s, z = %s" % (x, y, z)

        # g is a function marked as parametrized
        # calling g will now return a generator object:
        for f in g():
            f()

        # output:
        # x = 1, y = 2, z = 3
        # x = x, y = y, z = z
    """
    def parametrizer(f):
        def test_generator():
            for param_list in variable_assignments:
                yield inject(variables=variables, vals=param_list)(f)

        print "Things in test_generator:", dir(test_generator)
        mark_function(test_generator, PARAMETRIZED)
        return test_generator
    return parametrizer





def parametrize_method(variables="", variable_assignments=()):
    """Function decorator used to parametrize its arguments.

    Example::
        @parametrize(variables="x, y, z", variable_assignments=(
                                            (1, 2, 3),
                                            (10, 11, 12)))
        def g(x, y, z):
            print "x = %s, y = %s, z = %s" % (x, y, z)

        # g is a function marked as parametrized
        # calling g will now return a generator object:
        for f in g():
            f()

        # output:
        # x = 1, y = 2, z = 3
        # x = x, y = y, z = z
    """
    v = variables.split(",")
    v = [x.strip() for x in v]

    def parametrizer(f):

        @staticmethod
        def test_generator():
            for param_list in variable_assignments:
                assert len(v) == len(param_list), \
                    "Number of variables did not match the number of values to be assigned in this parametrized function. " \
                    "variables=%s, variable_assignments=%s, fn=%s" % (str(variables), str(param_list), str(f))

                kwargs = {}
                for i in range(len(v)):
                    kwargs[v[i]] = param_list[i]

                yield inject(**kwargs)(f)

        mark_function(test_generator, PARAMETRIZED)
        return test_generator
    return parametrizer


def inject(**kwargs):
    """Inject variables into the arguments of a function or method."""

    def injector(f):
        @functools.wraps(f)
        def wrapped(*w_args, **w_kwargs):
            kwargs.update(w_kwargs)
            return f(*w_args, **kwargs)

        mark_function(wrapped, INJECTED)
        wrapped.kwargs = kwargs
        return wrapped
    return injector


def main():

    @inject(x="a", y=5, z=30)
    def f(x, y, z):
        return "x=%s, y=%s, z=%s" % (x, y, z)

    print f.__name__
    print f()

    class C(object):
        def __init__(self):
            self.a = 20

        @inject(x=30)
        def f(self, x):
            return self.a + x

    c = C()
    print c.f()
    print c.f(x=10)  # can still override



    # @parametrize(variables="x, y, z", variable_assignments=(
    #         (1, 2, 3),
    #         ("x", "y", "z"),
    #         ([1], "a", 500)))
    # def g(x, y, z):
    #     print "x = %s, y = %s, z = %s" % (x, y, z)
    #




    #
    # for fun in g():
    #     print fun.__name__
    #     print fun.injected
    #     fun()


    class A(object):
        @inject(variables="x", vals=[10])
        def f(self, x):
            print "original method: self=%s, x=%s" % (str(self), str(x))
            print x * x
            return x * x

        @parametrize_method(variables="x", variable_assignments=((10,), (20,)))
        def g(self, x):
            print "original method: self=%s, x=%s" % (str(self), str(x))
            return x * x

    # f = A.f
    # a = A()
    # print f(a)
    #
    # print parametrized(A.g)
    # gen = A.g()
    # for fun in gen:
    #     print fun(A())


if __name__ == "__main__":
    main()
