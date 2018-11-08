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

from ducktape.utils.util import package_is_installed

from jinja2 import Template, FileSystemLoader, PackageLoader, ChoiceLoader, Environment
import os.path
import inspect


class TemplateRenderer(object):

    def _get_ctx(self):
        ctx = {k: getattr(self.__class__, k) for k in dir(self.__class__)}
        ctx.update(self.__dict__)
        return ctx

    def render_template(self, template, **kwargs):
        """
        Render a template using the context of the current object, optionally with overrides.

        :param template: the template to render, a Template or a str
        :param kwargs: optional override parameters
        :return: the rendered template
        """
        if not hasattr(template, 'render'):
            template = Template(template)
        ctx = self._get_ctx()
        return template.render(ctx, **kwargs)

    @staticmethod
    def _package_search_path(module_name):
        """
        :param module_name: Name of a module
        :return: (package, package_search_path) where package is the package containing the module,
            and package_search_path is a path relative to the package in which to search for templates.
        """
        module_parts = module_name.split(".")
        package = module_parts[0]

        # Construct path relative to package under which "templates" would be found
        directory = ""
        for d in module_parts[1: -1]:
            directory = os.path.join(directory, d)
        return package, os.path.join(directory, "templates")

    def render(self, path, **kwargs):
        """
        Render a template loaded from a file.
        template files referenced in file f should be in a sibling directory of f called "templates".

        :param path: path, relative to the search paths, to the template file
        :param kwargs: optional override parameters
        :return: the rendered template
        """
        if not hasattr(self, 'template_loader'):
            class_dir = os.path.dirname(inspect.getfile(self.__class__))

            module_name = self.__class__.__module__
            package, package_search_path = self._package_search_path(module_name)

            loaders = []
            msg = ""
            if os.path.isdir(class_dir):
                # FileSystemLoader overrides PackageLoader if the path containing this directory
                # is a valid directory. FileSystemLoader throws an error from which ChoiceLoader
                # doesn't recover if the directory is invalid
                loaders.append(FileSystemLoader(os.path.join(class_dir, 'templates')))
            else:
                msg += "Will not search in %s for template files since it is not a valid directory. " % class_dir

            if package_is_installed(package):
                loaders.append(PackageLoader(package, package_search_path))
            else:
                msg += "Will not search in package %s for template files because it cannot be imported."

            if len(loaders) == 0:
                # Expect at least one of FileSystemLoader and PackageLoader to be present
                raise EnvironmentError(msg)

            self.template_loader = ChoiceLoader(loaders)
            self.template_env = Environment(loader=self.template_loader, trim_blocks=True, lstrip_blocks=True)

        template = self.template_env.get_template(path)
        return self.render_template(template, **kwargs)
