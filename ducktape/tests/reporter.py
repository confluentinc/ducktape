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

from ducktape.utils.terminal_size import get_terminal_size
from ducktape.tests.result_store import TestKey, FileSystemResultStore

import json
import os
import shutil
import pkg_resources


DEFAULT_SEPARATOR_WIDTH = 100


def pass_fail(success):
    """Convenient helper. Converts boolean to PASS/FAIL."""
    return "PASS" if success else "FAIL"


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

    def __init__(self, datum):
        self.datum = datum
        self.width = get_terminal_size()[0]

    def result_string(self):
        """Stringify single result"""
        result_lines = [
            "test_id:    %s" % self.datum["test_id"],
            "status:     %s" % self.datum["status"].upper(),
            "run time:   %s" % format_time(self.datum["run_time_sec"]),
        ]

        if self.datum["status"].lower() != "pass" and self.datum["error_msg"] is not None:
            # Add summary if the test failed
            result_lines.append("\n")
            result_lines.append("    " + self.datum["error_msg"])

        if self.datum["data"] is not None:
            result_lines.append(json.dumps(self.datum["data"]))

        return "\n".join(result_lines)

    def report_string(self):
        """Get the whole report string."""
        return "\n".join(["=" * self.width,
                          self.result_string()])


class SingleResultStdoutReporter(SingleResultReporter):
    def report(self):
        print self.report_string()


class SingleResultFileReporter(SingleResultReporter):
    def __init__(self, datum, results_dir):
        super(SingleResultFileReporter, self).__init__(datum)
        self.results_dir = results_dir

    def report(self):
        self.width = DEFAULT_SEPARATOR_WIDTH
        report_file = os.path.join(self.results_dir, "report.txt")
        with open(report_file, "w") as fp:
            fp.write(self.report_string())


class SummaryReporter(object):
    def __init__(self, session_id, result_store):
        self.session_id = session_id
        self.result_store = result_store
        self.data = self.result_store.session_test_data(session_id)
        self.total_run_time = sum([d["run_time_sec"] for d in self.data])  # TODO - get by fetching session data
        self.num_tests = len(self.data)
        self.num_passed = len([d for d in self.data if d["status"].lower() == "pass"])
        self.num_failed = len([d for d in self.data if d["status"].lower() == "fail"])
        self.width = get_terminal_size()[0]

    def report(self):
        raise NotImplementedError("method report must be implemented by subclasses of SummaryReporter")


class SimpleSummaryReporter(SummaryReporter):
    def header_string(self):
        """Header lines of the report"""

        header_lines = [
            "=" * self.width,
            "SESSION REPORT (ALL TESTS)",
            "session_id: %s" % self.session_id,
            "run time:   %s" % self.total_run_time,
            "tests run:  %d" % self.num_tests,
            "passed:     %d" % self.num_passed,
            "failed:     %d" % self.num_failed,
            "=" * self.width
        ]

        return "\n".join(header_lines)

    def report_string(self):
        """Get the whole report string."""
        report_lines = [
            self.header_string()]

        report_lines.extend(
            [SingleResultReporter(datum).result_string() + "\n" + "-" * self.width for datum in self.data])

        return "\n".join(report_lines)


class SimpleFileSummaryReporter(SimpleSummaryReporter):
    def __init__(self, session_id, result_store, result_dir):
        super(SimpleFileSummaryReporter, self).__init__(session_id, result_store)
        self.result_dir = result_dir

    def report(self):
        self.width = DEFAULT_SEPARATOR_WIDTH
        report_file = os.path.join(self.result_dir, "report.txt")
        with open(report_file, "w") as fp:
            fp.write(self.report_string())


class SimpleStdoutSummaryReporter(SimpleSummaryReporter):
    def report(self):
        print self.report_string()


class HTMLSummaryReporter(SummaryReporter):
    def __init__(self, session_id, result_store, results_dir):
        super(HTMLSummaryReporter, self).__init__(session_id, result_store)
        self.results_dir = results_dir

    def format_test_name(self, datum):
        lines = ["Module: " + datum["module_name"],
                 "Class:  " + datum["cls_name"],
                 "Method: " + datum["function_name"]]

        if datum["injected_args"] is not None:
            lines.append("Arguments:")
            lines.append(
                json.dumps(datum["injected_args"], sort_keys=True, indent=2, separators=(',', ': ')))

        return "\n".join(lines)

    def format_result(self, datum):
        # k = TestKey.from_test_context(result.test_context)
        # all_results = self.result_store.get(result.test_context.test_id)

        result_json = {
            "test_id": self.format_test_name(datum),
            "test_result": datum["status"],
            "description": datum["description"],
            "run_time": format_time(datum["run_time_sec"]),
            "data": self.format_result_data(datum["data"]),
            "test_log": self.test_results_dir(datum)
        }
        return result_json

    def format_result_data(self, data):

        return "" if data is None else json.dumps(data, sort_keys=True, indent=2, separators=(',', ': '))

    def test_results_dir(self, test_results_dir):
        """Return *relative path* to test results directory.

        Path is relative to the base results_dir. Relative path behaves better if the results directory is copied,
        moved etc.
        """
        # base_dir = os.path.abspath(self.session_results_dir)
        # base_dir = os.path.join(base_dir, "")  # Ensure trailing directory indicator
        #
        # test_results_dir = os.path.abspath(test_results_dir)
        # return test_results_dir[len(base_dir):]  # truncate the "absolute" portion
        return "."

    def format_report(self):
        template = pkg_resources.resource_string(__name__, '../templates/report/report.html')

        result_string = ""
        for datum in self.data:
            result_string += json.dumps(self.format_result(datum))
            result_string += ","

        args = {
            'num_tests': self.num_tests,
            'num_passes': self.num_passed,
            'num_failures': self.num_failed,
            'run_time': format_time(self.total_run_time),
            'session': self.session_id,
            'tests': result_string
        }

        html = template % args
        report_html = os.path.join(self.results_dir, "report.html")
        with open(report_html, "w") as fp:
            fp.write(html)
            fp.close()

        report_css = os.path.join(self.results_dir, "report.css")
        report_css_origin = pkg_resources.resource_filename(__name__, '../templates/report/report.css')
        shutil.copy2(report_css_origin, report_css)

    def report(self):
        self.format_report()
