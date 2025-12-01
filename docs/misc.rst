.. _topics-misc:

====
Misc
====

Developer Install
=================

If you are are a ducktape developer, consider using the develop command instead of install. This allows you to make code changes without constantly reinstalling ducktape (see http://stackoverflow.com/questions/19048732/python-setup-py-develop-vs-install for more information)::

    cd ducktape
    python setup.py develop

To uninstall::

    cd ducktape
    python setup.py develop --uninstall


Unit Tests
==========

You can run the tests with code coverage and style check using `tox <https://tox.readthedocs.io/en/latest/>`_::

    tox

Alternatively, you can activate the virtualenv and run pytest and ruff directly::

    source ~/.virtualenvs/ducktape/bin/activate
    pytest tests
    ruff check
    ruff format --check


System Tests
============

System tests are included under the `systests/` directory. These tests are end to end tests that run across multiple VMs, testing ducktape in an environment similar to how it would be used in practice to test other projects.

The system tests run against virtual machines managed by `Vagrant <https://www.vagrantup.com/>`_. With Vagrant installed, start the VMs (3 by default)::

  vagrant up

From a developer install, running the system tests now looks the same as using ducktape on your own project::

  ducktape systests/

You should see the tests running, and then results and logs will be in the default directory, `results/`. By using a developer install, you can make modifications to the ducktape code and iterate on system tests without having to re-install after each modification.

When you're done running tests, you can destroy the VMs::

  vagrant destroy


Windows
=======

Ducktape support Services that run on Windows, but only in EC2.

When a ``Service`` requires a Windows machine, AWS credentials must be configured on the machine running ducktape.

Ducktape uses the `boto3`_ Python module to connect to AWS. And ``boto3`` support many different `configuration options`_

.. _boto3: https://aws.amazon.com/sdk-for-python/
.. _configuration options: https://boto3.readthedocs.io/en/latest/guide/configuration.html#guide-configuration

Here's an example bare minimum configuration using environment variables::

    export AWS_ACCESS_KEY_ID="ABC123"
    export AWS_SECRET_ACCESS_KEY="secret"
    export AWS_DEFAULT_REGION="us-east-1"

The region can be any AWS region, not just ``us-east-1``.
