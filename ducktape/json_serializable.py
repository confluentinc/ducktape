# Copyright 2016 Confluent Inc.
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


from json import JSONEncoder


class DucktapeJSONEncoder(JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_json"):
            # to_json may return a dict or array or other naturally json serializable object
            # this allows serialization to work correctly on nested items
            return obj.to_json()
        else:
            # Let the base class default method raise the TypeError
            return JSONEncoder.default(self, obj)
