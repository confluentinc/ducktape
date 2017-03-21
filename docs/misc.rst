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

You can run the tests via the setup.py script::

    python setup.py test

Alternatively, if you've installed pytest ``sudo pip install pytest`` you can run
it directly on the ``tests`` directory::

    py.test tests


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
