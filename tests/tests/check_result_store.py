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


from ducktape.tests.result_store import FileSystemResultStore

import os
import random
import tempfile


class CheckFileSystemResultStore(object):

    def setup(self):
        self.result_store_dir = tempfile.mkdtemp()
        self.store = FileSystemResultStore(self.result_store_dir)

    def teardown(self):
        try:
            os.system("rm -rf %s" % self.result_store_dir)
        except:
            pass

    def check_put_get(self):
        datum = {"x":2}
        session_id = "abc"
        test_id = "123"

        self.store.put(session_id="abc", test_id="123", datum=datum)
        assert self.store.get(session_id, test_id) == datum

    def check_session_test_data(self):
        session_id = "abc"
        data = []
        test_ids = []
        for i in range(10):
            test_id = str(random.randint(0, 2**64 - 1))
            datum = {"x": random.randint(0, 2**64 - 1), "test_id": test_id}
            data.append(datum)
            test_ids.append(test_id)

            self.store.put(session_id, test_id, datum)

        from_store = self.store.session_test_data(session_id)
        assert from_store == data
        assert [d["test_id"] for d in from_store] == test_ids

    def check_session_data(self):
        session_datum = {"x":2}
        session_id = "abc"

        self.store.put_session_data(session_id=session_id, datum=session_datum)
        stored_datum = self.store.session_data(session_id=session_id)

        assert stored_datum == session_datum
