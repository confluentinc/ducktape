from pathlib import Path
from unittest.mock import Mock

from ducktape.tests.reporter import FailedTestSymbolReporter


def check_to_symbol_no_args(tmp_path):
    result = Mock(file_name='/test_folder/test_file', cls_name='TestClass', function_name='test_func',
                  injected_args=None)
    reporter = FailedTestSymbolReporter(Mock())
    reporter.working_dir = Path('/')
    assert reporter.to_symbol(result) == 'test_folder/test_file::TestClass.test_func'


def check_to_symbol_relative_path(tmp_path):
    result = Mock(file_name='/test_folder/test_file', cls_name='TestClass', function_name='test_func',
                  injected_args=None)
    reporter = FailedTestSymbolReporter(Mock())
    reporter.working_dir = Path('/test_folder')
    assert reporter.to_symbol(result) == 'test_file::TestClass.test_func'


def check_to_symbol_with_args():
    result = Mock(file_name='/test_folder/test_file', cls_name='TestClass', function_name='test_func',
                  injected_args={'arg': 'val'})

    reporter = FailedTestSymbolReporter(Mock())
    reporter.working_dir = Path('/')
    assert reporter.to_symbol(result) == 'test_folder/test_file::TestClass.test_func@{"arg":"val"}'
