.. _topics-install:

=======
Install
=======

.. note:: As a general rule, it's recommended to use an isolation tool such as ``virtualenv``.

Prerequisites:

* ducktape uses paramiko, which depends upon cryptography (https://cryptography.io/en/latest/installation/), which has non-python external requirements

    * OSX should just work

    * Ubuntu::

        $ sudo apt-get install build-essential libssl-dev libffi-dev python-dev

    * Fedora and RHEL-derivatives::

        $ sudo yum install gcc libffi-devel python-devel openssl-devel

* Install ducktape::

    pip install ducktape



* On macOS you may need to ``brew install openssl`` and then install using::

    C_INCLUDE_PATH=/usr/local/opt/openssl/include LIBRARY_PATH=/usr/local/opt/openssl/lib pip install ducktape
