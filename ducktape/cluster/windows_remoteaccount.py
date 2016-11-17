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

from ducktape.cluster.remoteaccount import RemoteAccount


class WindowsRemoteAccount(RemoteAccount):
    def __init__(self, ssh_config, externally_routable_ip=None, logger=None):
        super(WindowsRemoteAccount, self).__init__(ssh_config, externally_routable_ip=externally_routable_ip,
                                                   logger=logger)

    def fetch_externally_routable_ip(self, is_aws):
        return "52.90.218.68" # TODO: implement this

    def close(self):
        return # TODO: implement this

    # NOTE TO SELF: as I implement parent methods, only implement the methods needed for my work. Don't implement all of them for the sake of doing so
