.. _topics-changelog:

====
Changelog
====

0.9.3
=====
- Removed `tox` from requirements. It was not used, but was breaking our builds due to recent pushes to `virtualenv`.

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

0.8.17
======
- Removed `tox` from requirements. It was not used, but was breaking our builds due to recent pushes to `virtualenv`.

0.8.x
=====
- Support test suites
- Easier way to rerun failed tests - generate test suite with all the failed tests and also print them in the log so that user can copy them and paste as ducktape command line arguments
- Python 2 is no longer supported, minimum supported version is 3.6
- Added `--deflake N` flag - if provided, it will attempt to rerun each failed test  up to N times, and if it eventually passes, it will be marked as Flaky - `#299 <https://github.com/confluentinc/ducktape/pull/299>`_
- [backport, also in 0.9.1] - use a generic network device based on the devices found on the remote machine rather than a hardcoded one - `#314 <https://github.com/confluentinc/ducktape/pull/314>`_ and `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- [backport, also in 0.9.1] - clean up process properly after an exception during test runner execution - `#323 <https://github.com/confluentinc/ducktape/pull/323>`_
- [backport, also in 0.9.1] - log ssh errors - `#319 <https://github.com/confluentinc/ducktape/pull/319>`_
- [backport, also in 0.9.1] - update vagrant tests to use ubuntu20 - `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- [backport, also in 0.9.1] - added command to print the total number of nodes the tests run will require - `#320 <https://github.com/confluentinc/ducktape/pull/320>`_