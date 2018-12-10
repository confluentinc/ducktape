.. _topics-install:

=======
Install
=======

1. Install `cryptography`_ (used by `paramiko` which Ducktape depends on), this may have non-python external requirements

.. _cryptography: https://cryptography.io/en/latest/installation

    * OSX (if needed)::

        brew install openssl

    * Ubuntu::

        sudo apt-get install build-essential libssl-dev libffi-dev python-dev

    * Fedora and RHEL-derivatives::

        sudo yum install gcc libffi-devel python-devel openssl-devel


2. As a general rule, it's recommended to use an isolation tool such as ``virtualenv``

3. Install Ducktape::

    pip install ducktape

.. note::

    On OSX you may need to::

        C_INCLUDE_PATH=/usr/local/opt/openssl/include LIBRARY_PATH=/usr/local/opt/openssl/lib pip install ducktape

    If you got error message `failed with error code 1`, you may need to install ducktape by::

        pip install --user ducktape

