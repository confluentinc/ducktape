from ducktape.mark.mark_expander import MarkedFunctionExpander
from ducktape.mark import ok_to_fail, oked_to_fail, parametrize, matrix

import pytest

class CheckOkToFail(object):
    def check_simple(self):
        @ok_to_fail
        def function(x=1, y=2, z=3):
            return x, y, z

        assert oked_to_fail(function)
        context_list = MarkedFunctionExpander(function=function).expand()
        assert len(context_list) == 1
        assert context_list[0].ok_to_fail

    def check_simple_method(self):
        class C(object):
            @ok_to_fail
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert oked_to_fail(C.function)
        context_list = MarkedFunctionExpander(function=C.function, cls=C).expand()
        assert len(context_list) == 1
        assert context_list[0].ok_to_fail

    def check_ok_to_fail_method(self):
        """Check @ok_to_fail() with no arguments used with various parametrizations on a method."""
        class C(object):
            @ok_to_fail
            @parametrize(x=100, y=200, z=300)
            @parametrize(x=100, z=300)
            @parametrize(y=200)
            @matrix(x=[1, 2, 3])
            @parametrize()
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert oked_to_fail(C.function)
        context_list = MarkedFunctionExpander(function=C.function, cls=C).expand()
        assert len(context_list) == 7
        for ctx in context_list:
            assert ctx.ok_to_fail

    def check_invalid_ok_to_fail(self):
        """If there are no test cases to which ok_to_fail applies, it should raise an error"""
        class C(object):
            @parametrize(x=100, y=200, z=300)
            @parametrize(x=100, z=300)
            @parametrize(y=200)
            @parametrize()
            @ok_to_fail
            def function(self, x=1, y=2, z=3):
                return x, y, z

        assert oked_to_fail(C.function)
        with pytest.raises(AssertionError):
            MarkedFunctionExpander(function=C.function, cls=C).expand()
