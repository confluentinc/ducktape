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

from ducktape.mark.mark_expander import MarkedFunctionExpander
from ducktape.mark import env, is_env

import os


class CheckEnv(object):

    def check_does_not_raise_exception_when_key_not_exists(self):
        class C(object):
            @env(BLAH='8')
            def function(self):
                return 1

    def check_has_env_annotation(self):
        class C(object):
            @env(JAVA_HOME="blah")
            def function(self):
                return 1

        assert is_env(C.function)

    def check_is_ignored_if_env_not_correct(self):
        class C(object):
            @env(JAVA_HOME="blah")
            def function(self):
                return 1

        context_list = MarkedFunctionExpander(function=C.function, cls=C).expand()
        assert context_list[0].ignore

    def check_is_not_ignore_if_correct_env(self):
        os.environ['test_key'] = 'test'

        class C(object):
            @env(test_key='test')
            def function(self):
                return 1

        context_list = MarkedFunctionExpander(function=C.function, cls=C).expand()
        assert not context_list[0].ignore
