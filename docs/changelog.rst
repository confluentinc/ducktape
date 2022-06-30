.. _topics-changelog:

====
Changelog
====

0.10.0
======
- Do not schedule tests on unresponsive nodes - `#325 <https://github.com/confluentinc/ducktape/pull/325>`_

0.9.1
=====
- use a generic network device based on the devices found on the remote machine rather than a hardcoded one - `#314 <https://github.com/confluentinc/ducktape/pull/314>`_ and `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- clean up process properly after an exception during test runner execution - `#323 <https://github.com/confluentinc/ducktape/pull/323>`_
- log ssh errors - `#319 <https://github.com/confluentinc/ducktape/pull/319>`_
- update vagrant tests to use ubuntu20 - `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- add flag that can print the total number of nodes a test run will require - `#320 <https://github.com/confluentinc/ducktape/pull/320>`_
- drop support for python 3.6 and add support for python 3.9 - `#317 <https://github.com/confluentinc/ducktape/pull/317>`_

0.9.0
=====
- Upgrade paramiko version to 2.10.0 - `#312 <https://github.com/confluentinc/ducktape/pull/312>`_
- Support SSH timeout - `#311 <https://github.com/confluentinc/ducktape/pull/311>`_
