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

import json
import os


class TestReporter(object):
    def __init__(self, results):
        """
        :type results: ducktape.tests.result.TestResults
        """
        self.results = results

    def pass_fail(self, success):
        """Convenient helper. Converts boolean to PASS/FAIL."""
        return "PASS" if success else "FAIL"

    def report(self):
        raise NotImplementedError("method report must be implemented by subclasses of TestReporter")


class SimpleReporter(TestReporter):
    def header_string(self):
        """Header lines of the report"""
        header_lines = [
            "Test run with session_id " + self.results.session_context.session_id,
            self.pass_fail(self.results.get_aggregate_success()),
            "---------------------------------------------------------------------"
            ]

        return "\n".join(header_lines)

    def result_string(self, result):
        """Stringify a single result."""
        result_lines = [
            self.pass_fail(result.success) + ": " + result.test_name]

        if not result.success:
            # Add summary if the test failed
            result_lines.append("    " + result.summary)

        if result.data is not None:
            result_lines.append(json.dumps(result.data))

        return "\n".join(result_lines)

    def report_string(self):
        """Get the whole report string."""
        report_lines = [
            self.header_string()]

        report_lines.extend(
            [self.result_string(result) for result in self.results])

        return "\n".join(report_lines)


class SimpleFileReporter(SimpleReporter):
    def report(self):
        report_file = os.path.join(self.results.session_context.results_dir, "summary")
        with open(report_file, "w") as fp:
            fp.write(self.report_string())


class SimpleStdoutReporter(SimpleReporter):
    def report(self):
        print self.report_string()