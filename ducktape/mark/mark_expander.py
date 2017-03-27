# Copyright 2016 Confluent Inc.
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


from ._mark import parametrized, Parametrize, _is_parametrize_mark
from ducktape.tests.test import TestContext


class MarkedFunctionExpander(object):
    """This class helps expand decorated/marked functions into a list of test context objects. """

    def __init__(self, session_context=None, module=None, cls=None, function=None, file=None, cluster=None):
        self.seed_context = TestContext(
            session_context=session_context, module=module, cls=cls, function=function, file=file, cluster=cluster)

        if parametrized(function):
            self.context_list = []
        else:
            self.context_list = [self.seed_context]

    def expand(self, test_parameters=None):
        """Inspect self.function for marks, and expand into a list of test context objects useable by the test runner.
        """
        f = self.seed_context.function

        # If the user has specified that they want to run tests with specific parameters, apply the parameters first,
        # then subsequently strip any parametrization decorators. Otherwise, everything gets applied normally.
        if test_parameters is not None:
            self.context_list = Parametrize(**test_parameters).apply(self.seed_context, self.context_list)

        for m in getattr(f, "marks", []):
            if test_parameters is None or not _is_parametrize_mark(m):
                self.context_list = m.apply(self.seed_context, self.context_list)

        return self.context_list
