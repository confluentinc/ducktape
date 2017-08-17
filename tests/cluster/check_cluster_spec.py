# Copyright 2015 Confluent Inc.
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

from ducktape.cluster.cluster_spec import ClusterSpec


class CheckClusterSpec(object):
    def check_cluster_spec_sizes(self):
        simple_linux_2 = ClusterSpec.simple_linux(2)
        assert 2 == len(simple_linux_2)
        assert 0 == len(ClusterSpec.empty())

    def check_to_string(self):
        empty = ClusterSpec.empty()
        assert "[]" == str(empty)
        simple_linux_5 = ClusterSpec.simple_linux(5)
        assert '[{"num_nodes": 5, "os": "linux"}]' == str(simple_linux_5)
