from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand
import re
import sys

version = ''
with open('ducktape/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


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
        self.run_command('flake8')
        sys.exit(errno)


test_req = open('requirements-test.txt').read()


setup(name="ducktape",
      version=version,
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
      python_requires='>= 3.6',
      install_requires=open('requirements.txt').read(),
      tests_require=test_req,
      extras_require={'test': test_req},
      setup_requires=['flake8==3.8.3'],
      cmdclass={'test': PyTest},
      )
