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

from ducktape.mark._parametrize import mark_as


def has_regression(fun):
    if hasattr(fun, "marks"):
        for mark in fun.marks:
            if isinstance(mark, RegressionMark):
                return True
    return False


class Mark(object):
    pass


class RegressionMark(Mark):
    def __init__(self, variable_selector):
        self.variable_selector = variable_selector


def regression(variable_selector):
    """regression decorator"""
    def regressionizer(f):
        mark_as(f, RegressionMark(variable_selector))
        return f

    return regressionizer
