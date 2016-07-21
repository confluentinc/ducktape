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

import pickle


class SerDe(object):
    def serialize(self, obj):
        if hasattr(obj, 'serialize'):
            obj.serialize()
        else:
            return pickle.dumps(obj)

    def deserialize(self, bytes_obj, obj_cls=None):
        if obj_cls and hasattr(obj_cls, 'deserialize'):
            return obj_cls.deserialize(bytes_obj)
        else:
            return pickle.loads(bytes_obj)
