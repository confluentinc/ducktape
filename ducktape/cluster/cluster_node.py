from typing import Optional

from ducktape.cluster.remoteaccount import RemoteAccount


class ClusterNode(object):
    def __init__(self, account: RemoteAccount, **kwargs):
        self.account = account
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def name(self) -> Optional[str]:
        return self.account.hostname

    @property
    def operating_system(self) -> Optional[str]:
        return self.account.operating_system
