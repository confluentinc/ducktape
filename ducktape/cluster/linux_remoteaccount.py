# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.cluster.cluster_spec import LINUX
from ducktape.cluster.remoteaccount import RemoteAccount


class LinuxRemoteAccount(RemoteAccount):

    def __init__(self, ssh_config, externally_routable_ip=None, logger=None):
        super(LinuxRemoteAccount, self).__init__(ssh_config, externally_routable_ip=externally_routable_ip,
                                                 logger=logger)
        self._ssh_client = None
        self._sftp_client = None
        self.os = LINUX

    @property
    def local(self):
        """Returns True if this 'remote' account is probably local.
        This is an imperfect heuristic, but should work for simple local testing."""
        return self.hostname == "localhost" and self.user is None and self.ssh_config is None

    def fetch_externally_routable_ip(self, is_aws):
        if is_aws:
            cmd = "/sbin/ifconfig eth0 "
        else:
            cmd = "/sbin/ifconfig eth1 "
        cmd += r"| grep 'inet ' | tail -n 1 | egrep -o '[0-9\.]+' | head -n 1 2>&1"
        output = "".join(self.ssh_capture(cmd))
        return output.strip()
