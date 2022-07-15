Ducktape documentation quick start guide
========================================


Build the documentation
-----------------------

To render the pages run::
```shell
tox -e docs
```
    
The rendered pages will be in ``docs/_build/html``


Specify documentation format
----------------------------

Documentation is built using [sphinx-build](https://www.sphinx-doc.org/en/master/man/sphinx-build.html) command.
You can select which builder to use using SPHINX_BUILDER command:
```shell
SPHINX_BUILDER=man tox -e docs
```
All available values: https://www.sphinx-doc.org/en/master/man/sphinx-build.html#cmdoption-sphinx-build-M


Pass options to sphinx-build
----------------------------
Any argument after `--` will be passed to the 
[sphinx-build](https://www.sphinx-doc.org/en/master/man/sphinx-build.html) command directly:
```shell
tox -e docs -- -E
```


