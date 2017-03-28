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

from ducktape.services.service import Service
from tests.ducktape_mock import test_context


class CheckTemplateRenderingService(object):
    """
    Tests rendering of templates, using input from a Service
    """

    def new_instance(self):
        return TemplateRenderingService()

    def check_simple(self):
        self.new_instance().render_simple()

    def check_single_variable(self):
        self.new_instance().render_single_variable()

    def check_overload(self):
        self.new_instance().render_overload()

    def check_class_template(self):
        self.new_instance().render_class_template()

    def check_file_template(self):
        self.new_instance().render_file_template()


class TemplateRenderingService(Service):
    NO_VARIABLE = "fixed content"
    SIMPLE_VARIABLE = "Hello {{a_field}}!"
    OVERLOAD_VARIABLES = "{{normal}} {{overload}}"

    CLASS_CONSTANT_TEMPLATE = "{{ CLASS_CONSTANT }}"
    CLASS_CONSTANT = "constant"

    def __init__(self):
        super(TemplateRenderingService, self).__init__(test_context(), 1)

    def render_simple(self):
        """Test that a trivial template works"""
        assert self.render_template(self.NO_VARIABLE) == self.NO_VARIABLE

    def render_single_variable(self):
        """Test that fields on the object are available to templates"""
        self.a_field = "world"
        assert self.render_template(self.SIMPLE_VARIABLE) == "Hello world!"

    def render_overload(self):
        self.normal = "normal"
        assert self.render_template(self.OVERLOAD_VARIABLES, overload='overloaded') == "normal overloaded"

    def render_class_template(self):
        assert self.render_template(self.CLASS_CONSTANT_TEMPLATE) == self.CLASS_CONSTANT
        self.CLASS_CONSTANT = "instance override"
        assert self.render_template(self.CLASS_CONSTANT_TEMPLATE) == "instance override"

    def render_file_template(self):
        self.a_field = "world"
        assert "Sample world" == self.render("sample")
