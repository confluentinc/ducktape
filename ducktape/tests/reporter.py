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
import shutil
import pkg_resources


class TestReporter(object):
    def __init__(self, results):
        """
        :type results: ducktape.tests.result.TestResults
        """
        self.results = results

    def pass_fail(self, success):
        """Convenient helper. Converts boolean to PASS/FAIL."""
        return "PASS" if success else "FAIL"

    def format_time(self, t):
        """Return human-readable interval of time.
        Assumes t is in units of seconds.
        """
        minutes = int(t / 60)
        seconds = t % 60

        r = ""
        if minutes > 0:
            r += "%d minute%s " % (minutes, "" if minutes == 1 else "s")
        r += "%.3f seconds" % seconds
        return r

    def report(self):
        raise NotImplementedError("method report must be implemented by subclasses of TestReporter")


class SimpleReporter(TestReporter):
    def header_string(self):
        """Header lines of the report"""
        header_lines = [
            "=========================================================================================",
            "session_id: %s" % self.results.session_context.session_id,
            "run time:   %s" % self.format_time(self.results.run_time),
            "tests run:  %d" % len(self.results),
            "passed:     %d" % self.results.num_passed(),
            "failed:     %d" % self.results.num_failed(),
            "========================================================================================="
        ]

        return "\n".join(header_lines)

    def result_string(self, result):
        """Stringify a single result."""

        result_lines = [
            self.pass_fail(result.success) + ":     " + result.test_name,
            "run time: %s" % self.format_time(result.run_time)
            ]

        if not result.success:
            # Add summary if the test failed
            result_lines.append("\n")
            result_lines.append("    " + result.summary)

        if result.data is not None:
            result_lines.append(json.dumps(result.data))

        result_lines.append("-----------------------------------------------------------------------------------------")
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


class HTMLReporter(TestReporter):

    def format_result(self, result):
        if result.success:
            test_result = 'pass'
        else:
            test_result = 'fail'

        result_json = {
            "test_name": result.test_name,
            "test_result": test_result,
            "summary": result.summary,
            "test_log": result.test_context.results_dir
        }
        return result_json

    def format_report(self):
        template = pkg_resources.resource_string(__name__, '../templates/report/report.html')

        num_tests = len(self.results)
        num_passes = 0
        result_string = ""
        for result in self.results:
            if result.success:
                num_passes += 1
            result_string += json.dumps(self.format_result(result))
            result_string += ","

        args = {
            'num_tests': num_tests,
            'num_passes': self.results.num_passed(),
            'num_failures': self.results.num_failed(),
            'run_time': self.results.run_time,
            'session': self.results.session_context.session_id,
            'tests': result_string
        }

        html = template % args
        report_html = os.path.join(self.results.session_context.results_dir, "report.html")
        with open(report_html, "w") as fp:
            fp.write(html)
            fp.close()

        report_css = os.path.join(self.results.session_context.results_dir, "report.css")
        report_css_origin = pkg_resources.resource_filename(__name__, '../templates/report/report.css')
        shutil.copy2(report_css_origin, report_css)

    def report(self):
        self.format_report()
