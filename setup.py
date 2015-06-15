from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand
import sys

from ducktape.utils.util import ducktape_version

class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(name="ducktape",
      version=ducktape_version(),
      description="Distributed system test tools",
      author="Confluent",
      platforms=["any"], 
      entry_points={
        'console_scripts': ['ducktape=ducktape.command_line.main:main'],
      },
      license="apache2.0",
      url="http://github.com/confluentinc/ducktape",
      packages=find_packages(),
      package_data={'ducktape': ['templates/report/*']},
      tests_require=['pytest'],
      cmdclass={'test': PyTest},
      )
