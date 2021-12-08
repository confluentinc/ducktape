from ducktape.tests.test import Test


class TestNotLoaded(Test):
    """Loader should not discover this - module name does not match default pattern."""

    def test_a(self):
        pass
