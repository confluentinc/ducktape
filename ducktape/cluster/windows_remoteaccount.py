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

import logging

from ducktape.cluster.remoteaccount import RemoteAccount


class WindowsRemoteAccount(RemoteAccount):

    def __init__(self, winrm_config, externally_routable_ip=None, logger=None):
        super(WindowsRemoteAccount, self).__init__(winrm_config, externally_routable_ip=externally_routable_ip,
                                                   logger=logger)

        self.os = RemoteAccount.WINDOWS

    def fetch_externally_routable_ip(self, is_aws):
        return "52.90.218.68" # TODO: implement this

    def close(self):
        return # TODO: implement this

    def run_command(self, cmd):
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)
