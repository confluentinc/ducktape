from ducktape.cluster.cluster_spec import LINUX
from ducktape.cluster.remoteaccount import RemoteAccount


class FakeRemoteAccount(RemoteAccount):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.os = LINUX

    def available(self):
        return True

    def fetch_externally_routable_ip(self, *args, **kwargs):
        return 'fake ip'


def create_fake_remote_account(*args, **kwargs):
    return FakeRemoteAccount(*args, **kwargs)
