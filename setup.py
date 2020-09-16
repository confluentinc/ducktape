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
      install_requires=['jinja2==2.10.1',
                        'boto3==1.9.217',
                        # jinja2 pulls in MarkupSafe with a > constraint, but we need to constrain it for compatibility
                        'MarkupSafe<2.0.0',
                        'pyparsing<3.0.0',
                        'zipp<2.0.0',
                        'pywinrm==0.2.2',
                        'requests==2.22.0',
                        'bcrypt==3.1.7',
                        'paramiko~=2.3.2',
                        'pysistence==0.4.1',
                        'pyzmq==18.1.0',
                        'pycryptodome==3.8.2',
                        'more-itertools==5.0.0',
                        'tox==3.13.2',
                        'six==1.12.0'],
      tests_require=['pytest==4.6.5',
                     'mock==3.0.5',
                     'psutil==5.6.3',
                     'memory_profiler==0.55',
                     'statistics==1.0.3.5',
                     'requests-testadapter==0.3.0'],
      setup_requires=['flake8==3.7.8'],
      cmdclass={'test': PyTest},
      )
