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

from ducktape.utils.util import ducktape_version

import os
import json
import sqlite3
import time


def create_test_datum(result):

    return {
        "timestamp": time.time(),
        "ducktape_version": ducktape_version(),
        "session_id": result.session_context.session_id,
        "description": result.description,
        "test_id": result.test_context.test_id,
        "module_name": result.test_context.module_name,
        "cls_name": result.test_context.cls_name,
        "function_name": result.test_context.function_name,
        "injected_args": result.test_context.injected_args,
        "run_time_sec": result.run_time,
        "services": result.test_context.services,
        "status": "pass" if result.success else "fail",
        "error_msg": result.summary,
        "data": result.data
    }


def create_session_datum(session_datum):
    return session_datum


class TestKey(object):
    """
    A TestKey identifies a particular test with a particular set of arguments
    """

    @staticmethod
    def from_test_context(test_context):
        return TestKey(test_context.module_name,
                                test_context.cls_name,
                                test_context.function_name,
                                test_context.injected_args)


    @staticmethod
    def from_datum(datum):
        return TestKey(datum["module_name"],
                                datum["cls_name"],
                                datum["function_name"],
                                datum["injected_args"])

    def __init__(self, module_name, cls_name, function_name, injected_args={}):
        self.module_name = module_name
        self.cls_name = cls_name
        self.function_name = function_name
        self.injected_args = injected_args

    def __repr__(self):
        return "~~".join(
            [self.module_name,
             self.cls_name,
             self.function_name,
             json.dumps(self.injected_args, separators=(',', ':'))])


class FileSystemResultStore(object):
    DB_NAME = "test_data.db"

    def __init__(self, root_results_dir):
        """Assume key is a string representing a path relative to root result directory"""
        self.root_results_dir = root_results_dir
        self.db_file = os.path.join(self.root_results_dir, "test_data.db")
        self._bootstrap_store()

    def _bootstrap_store(self):
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            cur.execute("""create table if not exists test_data (
              id integer primary key autoincrement,
              timestamp integer not null,
              test_id text not null,
              session_id text not null,
              data text not null
            );""")

            cur.execute("""create table if not exists session_data (
              id integer primary key autoincrement,
              timestamp integer not null,
              session_id text not null,
              data text not null
            );""")


    def _serialize(self, datum):
        return json.dumps(datum, separators=(',', ':'))

    def _deserialize(self, datum_bytes):
        return json.loads(datum_bytes)

    def put(self, session_id, test_id, datum):
        """"""
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            row = (int(time.time()), str(test_id), session_id, self._serialize(datum))
            cur.execute("insert into test_data(timestamp, test_id, session_id, data) values (?, ?, ?, ?)", row)

    def get(self, session_id, test_id):
        """Single datum is indexed by test_id and session_id"""

        test_id = str(test_id)
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            cur.execute("select data from test_data where test_id=? and session_id=?", (str(test_id), session_id))
            return self._deserialize(cur.fetchone()[0])

    def session_ids(self):
        """Return a list of all session_ids in the store"""
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            cur.execute("select distinct session_id from test_data")
            return [s[0] for s in cur.fetchall()]

    def test_ids(self):
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            cur.execute("select distinct test_id from test_data")
            return [s[0] for s in cur.fetchall()]

    def session_test_data(self, session_id):
        """Return a list of all session_ids in the store"""
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            cur.execute("select data from test_data where session_id=?", (session_id,))
            return [self._deserialize(d[0]) for d in cur.fetchall()]

    def test_data(self, test_id):
        """Return a list of all session_ids in the store"""
        test_id = str(test_id)
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            cur.execute("select data from test_data where test_id=?", (test_id,))
            return [self._deserialize(d[0]) for d in cur.fetchall()]

    def put_session_data(self, session_id, datum):
        """Return metadata pertaining to the particular test run with the given session_id."""
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            row = (int(time.time()), session_id, self._serialize(datum))
            cur.execute("insert into session_data(timestamp, session_id, data) values (?, ?, ?)", row)

    def session_data(self, session_id):
        """Return a list of all session_ids in the store"""
        with sqlite3.connect(self.db_file) as connection:
            cur = connection.cursor()
            cur.execute("select data from session_data where session_id=?", (session_id,))
            result = [self._deserialize(d[0]) for d in cur.fetchall()]
            assert len(result) == 1
            return result[0]


