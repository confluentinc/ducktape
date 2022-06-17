from ducktape.cluster.cluster_spec import LINUX, WINDOWS
from ducktape.cluster.remoteaccount import RemoteAccount


class FakeRemoteAccount(RemoteAccount):

    def __init__(self, *args, is_available=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.os = LINUX
        self.is_available = is_available

    def available(self):
        return self.is_available

    def fetch_externally_routable_ip(self, *args, **kwargs):
        return 'fake ip'


class FakeWindowsRemoteAccount(FakeRemoteAccount):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.os = WINDOWS


def create_fake_remote_account(*args, **kwargs):
    return FakeRemoteAccount(*args, **kwargs)
