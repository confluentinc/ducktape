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

from ducktape.tests.test import Test, TestContext
from ducktape.tests.session import SessionContext

from tests.test_utils.mock import MockArgs

import tempfile

class CheckTemplateRenderingTest(object):
    """
    Minimal test to verify template rendering functionality
    """

    def setup(self):
        dir = tempfile.gettempdir()
        session_ctx = SessionContext("session_id", dir, None, MockArgs())
        test_ctx = TestContext(session_ctx)
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

class TemplateRenderingTest(Test):
    pass
