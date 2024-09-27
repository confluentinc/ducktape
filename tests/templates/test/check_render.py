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

from ducktape.tests import Test
from ducktape.tests import TestContext
from ducktape.template import TemplateRenderer

from tests.ducktape_mock import session_context

import os
import tempfile


class CheckTemplateRenderingTest(object):
    """
    Minimal test to verify template rendering functionality
    """

    def setup(self):
        dir = tempfile.gettempdir()
        session_ctx = session_context(results_dir=dir)
        test_ctx = TestContext(session_context=session_ctx)
        return TemplateRenderingTest(test_ctx)

    def check_string_template(self):
        test = self.setup()
        test.other = "goodbye"
        result = test.render_template("Hello {{name}} and {{other}}", name="World")
        assert "Hello World and goodbye" == result

    def check_file_template(self):
        test = self.setup()
        test.name = "world"
        assert "Sample world" == test.render("sample")


class CheckPackageSearchPath(object):
    """
    Simple check on extracting package and search path based on module name.
    """

    def check_package_search_path(self):
        package, path = TemplateRenderer._package_search_path("a.b.c")
        # search path should be b/templates since templates is by convention a sibling of c
        assert package == "a" and path == os.path.join("b", "templates")

        package, path = TemplateRenderer._package_search_path("hi")
        assert package == "hi" and path == "templates"

        package, path = TemplateRenderer._package_search_path("")
        assert package == "" and path == "templates"

    def check_get_ctx(self):
        class A(TemplateRenderer):
            x = "xxx"

        class B(A):
            y = "yyy"

        b = B()
        b.instance = "b instance"

        ctx_a = A()._get_ctx()
        assert ctx_a["x"] == "xxx"
        assert "yyy" not in ctx_a
        assert "instance" not in ctx_a

        ctx_b = b._get_ctx()
        assert ctx_b["x"] == "xxx"
        assert ctx_b["y"] == "yyy"
        assert ctx_b["instance"] == "b instance"


class TemplateRenderingTest(Test):
    pass
