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

from ducktape.utils.terminal_size import get_terminal_size
from ducktape.tests.result import PASS, FAIL, IGNORE


DEFAULT_SEPARATOR_WIDTH = 100


def format_time(t):
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


class SingleResultReporter(object):
    """Helper class for creating a view of results from a single test."""

    def __init__(self, result):
        self.result = result
        self.width = get_terminal_size()[0]

    def result_string(self):
        """Stringify single result"""
        result_lines = [
            "test_id:    %s" % self.result.test_context.test_id,
            "status:     %s" % str(self.result.test_status).upper(),
            "run time:   %s" % format_time(self.result.run_time),
        ]

        if self.result.test_status == FAIL:
           # Add summary if the test failed
            result_lines.append("\n")
            result_lines.append("    " + self.result.summary)

        if self.result.data is not None:
            result_lines.append(json.dumps(self.result.data))

        return "\n".join(result_lines)

    def report_string(self):
        """Get the whole report string."""
        return "\n".join(["=" * self.width,
                          self.result_string()])


class SingleResultFileReporter(SingleResultReporter):
    def report(self):
        self.width = DEFAULT_SEPARATOR_WIDTH
        report_file = os.path.join(self.result.test_context.results_dir, "report.txt")
        with open(report_file, "w") as fp:
            fp.write(self.report_string())

        # write collected data
        if self.result.data is not None and len(self.result.data) > 0:
            data_file = os.path.join(self.result.test_context.results_dir, "data.json")
            with open(data_file, "w") as fp:
                fp.write(json.dumps(self.result.data))


class SummaryReporter(object):
    def __init__(self, results):
        self.results = results
        self.width = get_terminal_size()[0]

    def report(self):
        raise NotImplementedError("method report must be implemented by subclasses of SummaryReporter")


class SimpleSummaryReporter(SummaryReporter):
    def header_string(self):
        """Header lines of the report"""
        header_lines = [
            "=" * self.width,
            "SESSION REPORT (ALL TESTS)",
            "session_id: %s" % self.results.session_context.session_id,
            "run time:   %s" % format_time(self.results.run_time),
            "tests run:  %d" % len(self.results),
            "passed:     %d" % self.results.num_passed,
            "failed:     %d" % self.results.num_failed,
            "ignored:    %d" % self.results.num_ignored,
            "=" * self.width
        ]

        return "\n".join(header_lines)

    def report_string(self):
        """Get the whole report string."""
        report_lines = [
            self.header_string()]

        report_lines.extend(
            [SingleResultReporter(result).result_string() + "\n" + "-" * self.width for result in self.results])

        return "\n".join(report_lines)


class SimpleFileSummaryReporter(SimpleSummaryReporter):
    def report(self):
        self.width = DEFAULT_SEPARATOR_WIDTH
        report_file = os.path.join(self.results.session_context.results_dir, "report.txt")
        with open(report_file, "w") as fp:
            fp.write(self.report_string())


class SimpleStdoutSummaryReporter(SimpleSummaryReporter):
    def report(self):
        print self.report_string()


class HTMLSummaryReporter(SummaryReporter):

    def format_test_name(self, result):
        lines = ["Module: " + result.test_context.module_name,
                 "Class:  " + result.test_context.cls_name,
                 "Method: " + result.test_context.function_name]

        if result.test_context.injected_args is not None:
            lines.append("Arguments:")
            lines.append(
                json.dumps(result.test_context.injected_args, sort_keys=True, indent=2, separators=(',', ': ')))

        return "\n".join(lines)

    def format_result(self, result):
        test_result = str(result.test_status).lower()

        result_json = {
            "test_name": self.format_test_name(result),
            "test_result": test_result,
            "description": result.description,
            "run_time": format_time(result.run_time),
            "data": "" if result.data is None else json.dumps(result.data, sort_keys=True, indent=2, separators=(',', ': ')),
            "test_log": self.test_results_dir(result)
        }
        return result_json

    def test_results_dir(self, result):
        """Return *relative path* to test results directory.

        Path is relative to the base results_dir. Relative path behaves better if the results directory is copied,
        moved etc.
        """
        base_dir = os.path.abspath(result.session_context.results_dir)
        base_dir = os.path.join(base_dir, "")  # Ensure trailing directory indicator

        test_results_dir = os.path.abspath(result.test_context.results_dir)
        return test_results_dir[len(base_dir):]  # truncate the "absolute" portion

    def format_report(self):
        template = pkg_resources.resource_string(__name__, '../templates/report/report.html')

        num_tests = len(self.results)
        num_passes = 0
        result_string = ""
        for result in self.results:
            if result.test_status == PASS:
                num_passes += 1
            result_string += json.dumps(self.format_result(result))
            result_string += ","

        args = {
            'num_tests': num_tests,
            'num_passes': self.results.num_passed,
            'num_failures': self.results.num_failed,
            'num_ignored': self.results.num_ignored,
            'run_time': format_time(self.results.run_time),
            'session': self.results.session_context.session_id,
            'tests': result_string,
            'test_status_names': ",".join(["\'%s\'" % str(status) for status in [PASS, FAIL, IGNORE]])
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
