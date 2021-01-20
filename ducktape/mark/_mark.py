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


from ducktape.errors import DucktapeError
from six import iteritems

import functools
import itertools
import os


class Mark(object):
    """Common base class for "marks" which may be applied to test functions/methods."""

    @staticmethod
    def mark(fun, mark):
        """Attach a tag indicating that fun has been marked with the given mark

        Marking fun updates it with two attributes:

        - marks:      a list of mark objects applied to the function. These may be strings or objects subclassing Mark
                      we use a list because in some cases, it is useful to preserve ordering.
        - mark_names: a set of names of marks applied to the function
        """
        # Update fun.marks
        if hasattr(fun, "marks"):
            fun.marks.append(mark)
        else:
            fun.__dict__["marks"] = [mark]

        # Update fun.mark_names
        if hasattr(fun, "mark_names"):
            fun.mark_names.add(mark.name)
        else:
            fun.__dict__["mark_names"] = {mark.name}

    @staticmethod
    def marked(f, mark):
        if f is None:
            return False

        if not hasattr(f, "mark_names"):
            return False

        return mark.name in f.mark_names

    @staticmethod
    def clear_marks(f):
        if not hasattr(f, "marks"):
            return

        del f.__dict__["marks"]
        del f.__dict__["mark_names"]

    @property
    def name(self):
        return "MARK"

    def apply(self, seed_context, context_list):
        raise NotImplementedError("Subclasses should implement apply")

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        return self.name == other.name


class Ignore(Mark):
    """Ignore a specific parametrization of test."""

    def __init__(self, **kwargs):
        # Ignore tests with injected_args matching self.injected_args
        self.injected_args = kwargs

    @property
    def name(self):
        return "IGNORE"

    def apply(self, seed_context, context_list):
        assert len(context_list) > 0, "ignore annotation is not being applied to any test cases"
        for ctx in context_list:
            ctx.ignore = ctx.ignore or self.injected_args is None or self.injected_args == ctx.injected_args
        return context_list

    def __eq__(self, other):
        return super(Ignore, self).__eq__(other) and self.injected_args == other.injected_args


class IgnoreAll(Ignore):
    """This mark signals to ignore all parametrizations of a test."""

    def __init__(self):
        super(IgnoreAll, self).__init__()
        self.injected_args = None


class Matrix(Mark):
    """Parametrize with a matrix of arguments.
    Assume each values in self.injected_args is iterable
    """

    def __init__(self, **kwargs):
        self.injected_args = kwargs
        for k in self.injected_args:
            try:
                iter(self.injected_args[k])
            except TypeError as te:
                raise DucktapeError("Expected all values in @matrix decorator to be iterable: " + str(te))

    @property
    def name(self):
        return "MATRIX"

    def apply(self, seed_context, context_list):
        for injected_args in cartesian_product_dict(self.injected_args):
            injected_fun = _inject(**injected_args)(seed_context.function)
            context_list.insert(0, seed_context.copy(function=injected_fun, injected_args=injected_args))

        return context_list

    def __eq__(self, other):
        return super(Matrix, self).__eq__(other) and self.injected_args == other.injected_args


class Defaults(Mark):
    """Parametrize with a default matrix of arguments on existing parametrizations.
    Assume each values in self.injected_args is iterable
    """

    def __init__(self, **kwargs):
        self.injected_args = kwargs
        for k in self.injected_args:
            try:
                iter(self.injected_args[k])
            except TypeError as te:
                raise DucktapeError("Expected all values in @defaults decorator to be iterable: " + str(te))

    @property
    def name(self):
        return "DEFAULTS"

    def apply(self, seed_context, context_list):
        new_context_list = []
        if context_list:
            for ctx in context_list:
                for injected_args in cartesian_product_dict(
                        {arg: self.injected_args[arg] for arg in self.injected_args if arg not in ctx.injected_args}):
                    injected_args.update(ctx.injected_args)
                    injected_fun = _inject(**injected_args)(seed_context.function)
                    new_context_list.insert(0, seed_context.copy(function=injected_fun, injected_args=injected_args))
        else:
            for injected_args in cartesian_product_dict(self.injected_args):
                injected_fun = _inject(**injected_args)(seed_context.function)
                new_context_list.insert(0, seed_context.copy(function=injected_fun, injected_args=injected_args))

        return new_context_list

    def __eq__(self, other):
        return super(Defaults, self).__eq__(other) and self.injected_args == other.injected_args


class Parametrize(Mark):
    """Parametrize a test function"""

    def __init__(self, **kwargs):
        self.injected_args = kwargs

    @property
    def name(self):
        return "PARAMETRIZE"

    def apply(self, seed_context, context_list):
        injected_fun = _inject(**self.injected_args)(seed_context.function)
        context_list.insert(0, seed_context.copy(function=injected_fun, injected_args=self.injected_args))
        return context_list

    def __eq__(self, other):
        return super(Parametrize, self).__eq__(other) and self.injected_args == other.injected_args


class Env(Mark):
    def __init__(self, **kwargs):
        self.injected_args = kwargs
        self.should_ignore = any(os.environ.get(key) != value for key, value in iteritems(kwargs))

    @property
    def name(self):
        return "ENV"

    def apply(self, seed_context, context_list):
        for ctx in context_list:
            ctx.ignore = ctx.ignore or self.should_ignore

        return context_list

    def __eq__(self, other):
        return super(Env, self).__eq__(other) and self.injected_args == other.injected_args


PARAMETRIZED = Parametrize()
MATRIX = Matrix()
DEFAULTS = Defaults()
IGNORE = Ignore()
ENV = Env()


def _is_parametrize_mark(m):
    return m.name == PARAMETRIZED.name or m.name == MATRIX.name or m.name == DEFAULTS.name


def parametrized(f):
    """Is this function or object decorated with @parametrize or @matrix?"""
    return Mark.marked(f, PARAMETRIZED) or Mark.marked(f, MATRIX) or Mark.marked(f, DEFAULTS)


def ignored(f):
    """Is this function or object decorated with @ignore?"""
    return Mark.marked(f, IGNORE)


def is_env(f):
    return Mark.marked(f, ENV)


def cartesian_product_dict(d):
    """Return the "cartesian product" of this dictionary's values.
    d is assumed to be a dictionary, where each value in the dict is a list of values

    Example::

        {
            "x": [1, 2],
            "y": ["a", "b"]
        }

        expand this into a list of dictionaries like so:

        [
            {
                "x": 1,
                "y": "a"
            },
            {
                "x": 1,
                "y": "b"
            },
            {
                "x": 2,
                "y": "a"
            },
            {
                "x": 2,
                "y", "b"
            }
        ]
    """
    # Establish an ordering of the keys
    key_list = [k for k in d.keys()]

    expanded = []
    values_list = [d[k] for k in key_list]  # list of lists
    for v in itertools.product(*values_list):
        # Iterate through the cartesian product of the lists of values
        # One dictionary per element in this cartesian product
        new_dict = {}
        for i in range(len(key_list)):
            new_dict[key_list[i]] = v[i]
        expanded.append(new_dict)
    return expanded


def matrix(**kwargs):
    """Function decorator used to parametrize with a matrix of values.
    Decorating a function or method with ``@matrix`` marks it with the Matrix mark. When expanded using the
    ``MarkedFunctionExpander``, it yields a list of TestContext objects, one for every possible combination
    of arguments.

    Example::

        @matrix(x=[1, 2], y=[-1, -2])
        def g(x, y):
            print "x = %s, y = %s" % (x, y)

        for ctx in MarkedFunctionExpander(..., function=g, ...).expand():
            ctx.function()

        # output:
        # x = 1, y = -1
        # x = 1, y = -2
        # x = 2, y = -1
        # x = 2, y = -2
    """
    def parametrizer(f):
        Mark.mark(f, Matrix(**kwargs))
        return f
    return parametrizer


def defaults(**kwargs):
    """Function decorator used to parametrize with a default matrix of values.
    Decorating a function or method with ``@defaults`` marks it with the Defaults mark. When expanded using the
    ``MarkedFunctionExpander``, it yields a list of TestContext objects, one for every possible combination
    of defaults combined with ``@matrix`` and ``@parametrize``. If there are overlap between defaults
    and parametrization, defaults will not be applied.

    Example::

        @defaults(z=[1, 2])
        @matrix(x=[1], y=[1, 2])
        @parametrize(x=3, y=4)
        @parametrize(x=3, y=4, z=999)
        def g(x, y, z):
            print "x = %s, y = %s" % (x, y)

        for ctx in MarkedFunctionExpander(..., function=g, ...).expand():
            ctx.function()

        # output:
        # x = 1, y = 1, z = 1
        # x = 1, y = 1, z = 2
        # x = 1, y = 2, z = 1
        # x = 1, y = 2, z = 2
        # x = 3, y = 4, z = 1
        # x = 3, y = 4, z = 2
        # x = 3, y = 4, z = 999
    """
    def parametrizer(f):
        Mark.mark(f, Defaults(**kwargs))
        return f
    return parametrizer


def parametrize(**kwargs):
    """Function decorator used to parametrize its arguments.
    Decorating a function or method with ``@parametrize`` marks it with the Parametrize mark.

    Example::

        @parametrize(x=1, y=2 z=-1)
        @parametrize(x=3, y=4, z=5)
        def g(x, y, z):
            print "x = %s, y = %s, z = %s" % (x, y, z)

        for ctx in MarkedFunctionExpander(..., function=g, ...).expand():
            ctx.function()

        # output:
        # x = 1, y = 2, z = -1
        # x = 3, y = 4, z = 5
    """
    def parametrizer(f):
        Mark.mark(f, Parametrize(**kwargs))
        return f
    return parametrizer


def ignore(*args, **kwargs):
    """
    Test method decorator which signals to the test runner to ignore a given test.

    Example::

        When no parameters are provided to the @ignore decorator, ignore all parametrizations of the test function

        @ignore  # Ignore all parametrizations
        @parametrize(x=1, y=0)
        @parametrize(x=2, y=3)
        def the_test(...):
            ...

    Example::

        If parameters are supplied to the @ignore decorator, only ignore the parametrization with matching parameter(s)

        @ignore(x=2, y=3)
        @parametrize(x=1, y=0)  # This test will run as usual
        @parametrize(x=2, y=3)  # This test will be ignored
        def the_test(...):
            ...
    """
    if len(args) == 1 and len(kwargs) == 0:
        # this corresponds to the usage of the decorator with no arguments
        # @ignore
        # def test_function:
        #   ...
        Mark.mark(args[0], IgnoreAll())
        return args[0]

    # this corresponds to usage of @ignore with arguments
    def ignorer(f):
        Mark.mark(f, Ignore(**kwargs))
        return f

    return ignorer


def env(**kwargs):
    def environment(f):
        Mark.mark(f, Env(**kwargs))
        return f

    return environment


def _inject(*args, **kwargs):
    """Inject variables into the arguments of a function or method.
    This is almost identical to decorating with functools.partial, except we also propagate the wrapped
    function's __name__.
    """

    def injector(f):
        assert callable(f)

        @functools.wraps(f)
        def wrapper(*w_args, **w_kwargs):
            return functools.partial(f, *args, **kwargs)(*w_args, **w_kwargs)

        wrapper.args = args
        wrapper.kwargs = kwargs
        wrapper.function = f

        return wrapper
    return injector
