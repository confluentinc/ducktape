.. _topics-changelog:

====
Changelog
====

0.11.0
======
- Option to fail tests without `@cluster` annotation. Deprecate ``min_cluster_spec()`` method in the ``Test`` class - `#336 <https://github.com/confluentinc/ducktape/pull/336>`_

0.10.1
======
- Disable health checks for nodes, effectively disabling `#325 <https://github.com/confluentinc/ducktape/pull/325>`_. See github issue for details - `#339 <https://github.com/confluentinc/ducktape/issues/339>`_

0.10.0
======
- **DO NOT USE**, this release has a nasty bug - `#339 <https://github.com/confluentinc/ducktape/issues/339>`_
- Do not schedule tests on unresponsive nodes - `#325 <https://github.com/confluentinc/ducktape/pull/325>`_

0.9.2
=====
- Service release, no ducktape changes, simply fixed readthedocs configs.

0.9.1
=====
- use a generic network device based on the devices found on the remote machine rather than a hardcoded one - `#314 <https://github.com/confluentinc/ducktape/pull/314>`_ and `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- clean up process properly after an exception during test runner execution - `#323 <https://github.com/confluentinc/ducktape/pull/323>`_
- log ssh errors - `#319 <https://github.com/confluentinc/ducktape/pull/319>`_
- update vagrant tests to use ubuntu20 - `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- added command to print the total number of nodes the tests run will require - `#320 <https://github.com/confluentinc/ducktape/pull/320>`_
- drop support for python 3.6 and add support for python 3.9 - `#317 <https://github.com/confluentinc/ducktape/pull/317>`_

0.9.0
=====
- Upgrade paramiko version to 2.10.0 - `#312 <https://github.com/confluentinc/ducktape/pull/312>`_
- Support SSH timeout - `#311 <https://github.com/confluentinc/ducktape/pull/311>`_

0.8.x
=====
- Support test suites
- Easier way to rerun failed tests - generate test suite with all the failed tests and also print them in the log so that user can copy them and paste as ducktape command line arguments
- Python 2 is no longer supported, minimum supported version is 3.6
- [backport, also in 0.9.1] - use a generic network device based on the devices found on the remote machine rather than a hardcoded one - `#314 <https://github.com/confluentinc/ducktape/pull/314>`_ and `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- [backport, also in 0.9.1] - clean up process properly after an exception during test runner execution - `#323 <https://github.com/confluentinc/ducktape/pull/323>`_
- [backport, also in 0.9.1] - log ssh errors - `#319 <https://github.com/confluentinc/ducktape/pull/319>`_
- [backport, also in 0.9.1] - update vagrant tests to use ubuntu20 - `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- [backport, also in 0.9.1] - added command to print the total number of nodes the tests run will require - `#320 <https://github.com/confluentinc/ducktape/pull/320>`_