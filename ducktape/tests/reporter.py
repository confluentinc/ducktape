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

from __future__ import print_function

import json
import sys
from pathlib import Path

import yaml
import os
import shutil
import xml.etree.ElementTree as ET

from ducktape.utils.terminal_size import get_terminal_size
from ducktape.utils.util import ducktape_version
from ducktape.tests.status import PASS, FAIL, IGNORE, FLAKY
from ducktape.json_serializable import DucktapeJSONEncoder

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
            "test_id:    %s" % self.result.test_id,
            "status:     %s" % str(self.result.test_status).upper(),
            "run time:   %s" % format_time(self.result.run_time_seconds),
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
        report_file = os.path.join(self.result.results_dir, "report.txt")
        with open(report_file, "w") as fp:
            fp.write(self.report_string())

        # write collected data
        if self.result.data is not None:
            data_file = os.path.join(self.result.results_dir, "data.json")
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
            "ducktape version: %s" % ducktape_version(),
            "session_id:       %s" % self.results.session_context.session_id,
            "run time:         %s" % format_time(self.results.run_time_seconds),
            "tests run:        %d" % len(self.results),
            "passed:           %d" % self.results.num_passed,
            "flaky:            %d" % self.results.num_flaky,
            "failed:           %d" % self.results.num_failed,
            "ignored:          %d" % self.results.num_ignored,
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
        print(self.report_string())


class JSONReporter(object):
    def __init__(self, results):
        self.results = results

    def report(self):
        report_file = os.path.abspath(os.path.join(self.results.session_context.results_dir, "report.json"))
        with open(report_file, "w") as f:
            f.write(json.dumps(self.results, cls=DucktapeJSONEncoder, sort_keys=True, indent=2, separators=(',', ': ')))


class JUnitReporter(object):
    def __init__(self, results):
        self.results = results

    def report(self):
        report_file = os.path.abspath(os.path.join(self.results.session_context.results_dir, "report.xml"))
        testsuites = {}

        # First bucket by module_name and argregate counts
        for result in self.results:
            module_name = result.module_name
            testsuites.setdefault(module_name, {})
            # Set default values
            testsuite = testsuites[module_name]
            testsuite.setdefault('tests', 0)
            testsuite.setdefault('skipped', 0)
            testsuite.setdefault('failures', 0)
            testsuite.setdefault('errors', 0)
            testsuite.setdefault('testcases', []).append(result)

            # Always increment total number of tests
            testsuite['tests'] += 1
            if result.test_status == FAIL:
                testsuite['failures'] += 1
            elif result.test_status == IGNORE:
                testsuite['skipped'] += 1

        total = self.results.num_failed + self.results.num_ignored + self.results.num_passed + self.results.num_flaky
        # Now start building XML document
        root = ET.Element('testsuites', attrib=dict(
            name="ducktape", time=str(self.results.run_time_seconds),
            tests=str(total), disabled="0", errors="0",
            failures=str(self.results.num_failed)
        ))
        for module_name, testsuite in testsuites.items():
            xml_testsuite = ET.SubElement(root, 'testsuite', attrib=dict(
                name=module_name, tests=str(testsuite['tests']), disabled="0",
                errors="0", failures=str(testsuite['failures']), skipped=str(testsuite['skipped'])
            ))
            for test in testsuite['testcases']:
                # Since we're already aware of module_name and cls_name, strip that prefix off
                full_name = "{module_name}.{cls_name}.".format(module_name=module_name, cls_name=test.cls_name)
                if test.test_id.startswith(full_name):
                    name = test.test_id[len(full_name):]
                else:
                    name = test.test_id
                xml_testcase = ET.SubElement(xml_testsuite, 'testcase', attrib=dict(
                    name=name, classname=test.cls_name, time=str(test.run_time_seconds),
                    status=str(test.test_status), assertions=""
                ))
                if test.test_status == FAIL:
                    xml_failure = ET.SubElement(xml_testcase, 'failure', attrib=dict(
                        message=test.summary.splitlines()[0]
                    ))
                    xml_failure.text = test.summary
                elif test.test_status == IGNORE:
                    ET.SubElement(xml_testcase, 'skipped')

        with open(report_file, "w") as f:
            content = ET.tostring(root)
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            f.write(content)


class HTMLSummaryReporter(SummaryReporter):

    def format_test_name(self, result):
        lines = ["Module:      " + result.module_name,
                 "Class:       " + result.cls_name,
                 "Method:      " + result.function_name,
                 f"Nodes (used/allocated): {result.nodes_used}/{result.nodes_allocated}"]

        if result.injected_args is not None:
            lines.append("Arguments:")
            lines.append(
                json.dumps(result.injected_args, sort_keys=True, indent=2, separators=(',', ': ')))

        return "\n".join(lines)

    def format_result(self, result):
        test_result = str(result.test_status).lower()

        result_json = {
            "test_name": self.format_test_name(result),
            "test_result": test_result,
            "description": result.description,
            "run_time": format_time(result.run_time_seconds),
            "data": "" if result.data is None else json.dumps(result.data, sort_keys=True,
                                                              indent=2, separators=(',', ': ')),
            "summary": result.summary,
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

        test_results_dir = os.path.abspath(result.results_dir)
        return test_results_dir[len(base_dir):]  # truncate the "absolute" portion

    def format_report(self):
        if sys.version_info >= (3, 9):
            import importlib.resources as importlib_resources
            template = importlib_resources.files('ducktape').joinpath('templates/report/report.html').read_text('utf-8')
        else:
            import pkg_resources
            template = pkg_resources.resource_string(__name__, '../templates/report/report.html').decode('utf-8')

        num_tests = len(self.results)
        num_passes = 0
        failed_result_string = []
        passed_result_string = []
        ignored_result_string = []
        flaky_result_string = []

        for result in self.results:
            json_string = json.dumps(self.format_result(result))
            if result.test_status == PASS:
                num_passes += 1
                passed_result_string.append(json_string)
                passed_result_string.append(",")
            elif result.test_status == FAIL:
                failed_result_string.append(json_string)
                failed_result_string.append(",")
            elif result.test_status == IGNORE:
                ignored_result_string.append(json_string)
                ignored_result_string.append(",")
            elif result.test_status == FLAKY:
                flaky_result_string.append(json_string)
                flaky_result_string.append(",")
            else:
                raise Exception("Unknown test status in report: {}".format(result.test_status.to_json()))

        args = {
            'ducktape_version': ducktape_version(),
            'num_tests': num_tests,
            'num_passes': self.results.num_passed,
            'num_flaky': self.results.num_flaky,
            'num_failures': self.results.num_failed,
            'num_ignored': self.results.num_ignored,
            'run_time': format_time(self.results.run_time_seconds),
            'session': self.results.session_context.session_id,
            'passed_tests': "".join(passed_result_string),
            'flaky_tests': "".join(flaky_result_string),
            'failed_tests': "".join(failed_result_string),
            'ignored_tests': "".join(ignored_result_string),
            'test_status_names': ",".join(["\'%s\'" % str(status) for status in [PASS, FAIL, IGNORE, FLAKY]])
        }

        html = template % args
        report_html = os.path.join(self.results.session_context.results_dir, "report.html")
        with open(report_html, "w") as fp:
            fp.write(html)
            fp.close()

        report_css = os.path.join(self.results.session_context.results_dir, "report.css")

        if sys.version_info >= (3, 9):
            import importlib.resources as importlib_resources
            with importlib_resources.as_file(importlib_resources.files('ducktape')
                                             / 'templates/report/report.css') as report_css_origin:
                shutil.copy2(report_css_origin, report_css)
        else:
            import pkg_resources
            report_css_origin = pkg_resources.resource_filename(__name__, '../templates/report/report.css')
            shutil.copy2(report_css_origin, report_css)

    def report(self):
        self.format_report()


class FailedTestSymbolReporter(SummaryReporter):

    def __init__(self, results):
        super().__init__(results)
        self.working_dir = Path().absolute()
        self.separator = "=" * self.width

    def to_symbol(self, result):
        p = Path(result.file_name).relative_to(self.working_dir)
        line = f'{p}::{result.cls_name}.{result.function_name}'
        if result.injected_args:
            injected_args_str = json.dumps(result.injected_args, separators=(',', ':'))
            line += f'@{injected_args_str}'
        return line

    def dump_test_suite(self, lines):
        print(self.separator)
        print('FAILED TEST SUITE')
        suite = {self.results.session_context.session_id: lines}
        file_path = Path(self.results.session_context.results_dir) / "rerun-failed.yml"
        with file_path.open('w') as fp:
            print(f'Test suite to rerun failed tests: {file_path}')
            yaml.dump(suite, stream=fp, indent=4)

    def print_test_symbols_string(self, lines):
        print(self.separator)
        print('FAILED TEST SYMBOLS')
        print('Pass the test symbols below to your ducktape run')
        # quote the symbol because json parameters will be processed by shell otherwise, making it not copy-pasteable
        print(' '.join([f"'{line}'" for line in lines]))

    def report(self):
        symbols = [self.to_symbol(result) for result in self.results if result.test_status == FAIL]
        if not symbols:
            return

        self.dump_test_suite(symbols)
        self.print_test_symbols_string(symbols)
