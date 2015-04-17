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

from jinja2 import Template, FileSystemLoader, Environment
import os.path
import inspect

class TemplateRenderer(object):

    def render_template(self, template, **kwargs):
        """
        Render a template using the context of the current object, optionally with overrides.

        :param template: the template to render, a Template or a str
        :param kwargs: optional override parameters
        :return: the rendered template
        """
        if not hasattr(template, 'render'): template = Template(template)
        return template.render(self.__dict__, **kwargs)

    def render(self, path, **kwargs):
        """
        Render a template loaded from a file, searching .

        :param path: path, relative to the search paths, to the template file
        :param kwargs:
        :return:
        """
        if not hasattr(self, 'template_loader'):
            class_dir = os.path.dirname(inspect.getfile(self.__class__))
            self.template_loader = FileSystemLoader(os.path.join(class_dir, 'templates'))
            self.template_env = Environment(loader=self.template_loader, trim_blocks=True, lstrip_blocks=True)
        template = self.template_env.get_template(path)
        return self.render_template(template, **kwargs)