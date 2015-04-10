from ducktape.tests.test import TestLoader
from ducktape.tests.test import SerialTestRunner
from ducktape.tests.test import SimpleStdoutReporter
from ducktape.tests.test import SimpleFileReporter
from ducktape.tests.test import TestSessionContext
from ducktape.cluster.vagrant import VagrantCluster

import os
import sys
import random
import time


def mock_test_method():
    def test_method(self_obj):
        success = random.randint(0, 100) > 20
        if not success:
            raise AssertionError("Something bad happened!")
    return test_method


def mock_setup():
    def setup(self_obj):
        print "Setting up!"
    return setup


def mock_teardown():
    def teardown(self_obj):
        print "Tearing down!"
    return teardown


def swap_in_mock_run(test_classes):
    for tc in test_classes:
        tc.run = mock_test_method()


def swap_in_mock_fixtures(test_classes):
    for tc in test_classes:
        if hasattr(tc, "setUp"):
            tc.setUp = mock_setup()
        if hasattr(tc, "tearDown"):
            tc.tearDown = mock_teardown()


def get_test_session_id():
    TEST_ID_FILE = ".ducktape/test_id"

    def get_id(day, num):
        return day + "--%03d" % num

    def split_id(an_id):
        day = an_id[:10]
        num = int(an_id[12:])
        return day, num

    def today():
        return time.strftime("%Y-%m-%d")

    def next_id(prev_id):
        if prev_id is None:
            prev_day = today()
            prev_num = 0
        else:
            prev_day, prev_num = split_id(prev_id)

        if prev_day == today():
            next_day = prev_day
            next_num = prev_num + 1
        else:
            next_day = today()
            next_num = 1

        return get_id(next_day, next_num)

    # Generate current test_session_id
    if os.path.isfile(TEST_ID_FILE):
        with open(TEST_ID_FILE, "r") as fp:
            test_id = next_id(fp.read())
    else:
        test_id = next_id(None)

    # Record current test_session_id
    with open(TEST_ID_FILE, "w") as fp:
        fp.write(test_id)

    return test_id


def main():
    if len(sys.argv) != 2:
        print "Usage: ducktape <directory_name>"
        sys.exit(1)

    if not os.path.isdir(".ducktape"):
        os.makedirs(".ducktape")

    test_session_id = get_test_session_id()
    print test_session_id
    test_session_results_dir = test_session_id + "-test-results"
    os.mkdir(test_session_results_dir)
    test_session_context = TestSessionContext(test_session_results_dir, test_session_id)

    loader = TestLoader()
    test_classes = loader.discover(sys.argv[1])
    swap_in_mock_run(test_classes)
    swap_in_mock_fixtures(test_classes)

    # TODO command-line hooks specify type of cluster and type of test runner
    cluster = VagrantCluster()
    runner = SerialTestRunner(test_session_context, test_classes, cluster)
    test_results = runner.run_all_tests()

    # TODO command-line hook for type of reporter
    reporter = SimpleStdoutReporter(test_results)
    reporter.report()
    reporter = SimpleFileReporter(test_results)
    reporter.report()

    if not test_results.get_aggregate_success():
        sys.exit(1)

if __name__ == "__main__":
    main()






