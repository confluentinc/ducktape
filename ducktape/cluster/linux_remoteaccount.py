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
from ducktape.cluster.remoteaccount import RemoteAccount, RemoteAccountError


class LinuxRemoteAccount(RemoteAccount):

    def __init__(self, *args, **kwargs):
        super(LinuxRemoteAccount, self).__init__(*args, **kwargs)
        self._ssh_client = None
        self._sftp_client = None
        self.os = LINUX

    @property
    def local(self):
        """Returns True if this 'remote' account is probably local.
        This is an imperfect heuristic, but should work for simple local testing."""
        return self.hostname == "localhost" and self.user is None and self.ssh_config is None

    def get_network_devices(self):
        """
        Utility to get all network devices on a linux account
        """
        return [
            device
            for device in self.sftp_client.listdir('/sys/class/net')
        ]

    def get_external_accessible_network_devices(self):
        """
        gets the subset of devices accessible through an external conenction
        """
        return [
            device
            for device in self.get_network_devices()
            if device != 'lo'  # do not include local device
            and (device.startswith("en") or device.startswith('eth'))  # filter out other devices; "en" means ethernet
            # eth0 can also sometimes happen, see https://unix.stackexchange.com/q/134483
        ]

    # deprecated, please use the self.externally_routable_ip that is set in your cluster,
    # not explicitly deprecating it as it's used by vagrant cluster
    def fetch_externally_routable_ip(self, is_aws=None):
        if is_aws is not None:
            self.logger.warning("fetch_externally_routable_ip: is_aws is a deprecated flag, and does nothing")

        devices = self.get_external_accessible_network_devices()

        self.logger.debug("found devices: {}".format(devices))

        if not devices:
            raise RemoteAccountError(self, "Couldn't find any network devices")

        fmt_cmd = (
            "/sbin/ifconfig {device} | "
            "grep 'inet ' | "
            "tail -n 1 | "
            r"egrep -o '[0-9\.]+' | "
            "head -n 1 2>&1"
        )

        ips = [
            "".join(
                self.ssh_capture(fmt_cmd.format(device=device))
            ).strip()
            for device in devices
        ]
        self.logger.debug("found ips: {}".format(ips))
        self.logger.debug("returning the first ip found")
        return next(iter(ips))
