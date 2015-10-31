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

from tests import ducktape_mock
from ducktape.tests.test import Test, TestContext, _escape_pathname


class CheckEscapePathname(object):

    def check_illegal_path(self):
        path = "\\/.a=2,   b=x/y/z"
        assert _escape_pathname(path) == "a=2.b=x.y.z"

    def check_negative(self):
        # it's better if negative numbers are preserved
        path = "x= -2, y=-50"
        assert _escape_pathname(path) == "x=-2.y=-50"

    def check_many_dots(self):
        path = "..a.....b.c...d."
        assert _escape_pathname(path) == "a.b.c.d"


class CheckDescription(object):
    """Check that pulling a description from a test works as expected."""
    def check_from_function(self):
        """If the function has a docstring, the description should come from the function"""
        context = TestContext(session_context=ducktape_mock.session_context(), cls=DummyTest, function=DummyTest.test_function_description)
        assert context.description == "function description"

    def check_from_class(self):
        """If the test method has no docstring, description should come from the class docstring"""
        context = TestContext(session_context=ducktape_mock.session_context(), cls=DummyTest, function=DummyTest.test_class_description)
        assert context.description == "class description"

    def check_no_description(self):
        """If nobody has a docstring, there shouldn't be an error, and description should be empty string"""
        context = TestContext(session_context=ducktape_mock.session_context(), cls=DummyTestNoDescription, function=DummyTestNoDescription.test_this)
        assert context.description == ""


class DummyTest(Test):
    """class description"""
    def test_class_description(self):
        pass

    def test_function_description(self):
        """function description"""
        pass


class DummyTestNoDescription(Test):
    def test_this(self):
        pass