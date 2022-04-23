from unittest.mock import Mock

import pytest

from ducktape.tests.reporter import FailedTestSymbolReporter


def check_to_symbol_no_args():
    result = Mock(file_name='test_folder/test_file', cls_name='TestClass', function_name='test_func',
                  injected_args=None)

    assert FailedTestSymbolReporter.to_symbol(result) == 'test_folder/test_file::TestClass.test_func'


def check_to_symbol_with_args():
    result = Mock(file_name='test_folder/test_file', cls_name='TestClass', function_name='test_func',
                  injected_args={'arg': 'val'})

    assert FailedTestSymbolReporter.to_symbol(result) == 'test_folder/test_file::TestClass.test_func@{"arg":"val"}'
