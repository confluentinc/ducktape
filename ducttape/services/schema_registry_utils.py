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

import json
import urllib2

SCHEMA_REGISTRY_V1_JSON = "application/vnd.schemaregistry.v1+json"
SCHEMA_REGISTRY_V1_JSON_WEIGHTED = SCHEMA_REGISTRY_V1_JSON

# These are defaults that track the most recent API version. These should always be specified
# anywhere the latest version is produced/consumed.
SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT = SCHEMA_REGISTRY_V1_JSON
SCHEMA_REGISTRY_DEFAULT_JSON = "application/vnd.schemaregistry+json"
SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED = SCHEMA_REGISTRY_DEFAULT_JSON + "; qs=0.9"
JSON = "application/json"
JSON_WEIGHTED = JSON + "; qs=0.5"

PREFERRED_RESPONSE_TYPES = [SCHEMA_REGISTRY_V1_JSON, SCHEMA_REGISTRY_DEFAULT_JSON, JSON]

# This type is completely generic and carries no actual information about the type of data, but
# it is the default for request entities if no content type is specified. Well behaving users
# of the API will always specify the content type, but ad hoc use may omit it. We treat this as
# JSON since that's all we currently support.
GENERIC_REQUEST = "application/octet-stream"

# Minimum header data necessary for using the Schema Registry REST api.
SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES = {"Content-Type": SCHEMA_REGISTRY_V1_JSON_WEIGHTED, "Accept": "*/*"}


class RequestData(object):
    """
    Interface for classes wrapping data that goes in the body of an http request.
    """
    def to_json(self):
        raise NotImplementedError("Subclasses should implement to_json")


class RegisterSchemaRequest(RequestData):
    def __init__(self, schema_string):
        self.schema = schema_string

    def to_json(self):
        return json.dumps({"schema": self.schema})


class Compatibility(object):
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"


class ConfigUpdateRequest(RequestData):
    def __init__(self, compatibility):
        # "NONE", "BACKWARD", "FORWARD", or "FULL"
        self.compatibility = compatibility.upper()

    def to_json(self):
        return json.dumps({"compatibility": self.compatibility.upper()})


def http_request(url, method, data="", headers=None):
    if url[0:7].lower() != "http://":
        url = "http://%s" % url

    req = urllib2.Request(url, data, headers)
    req.get_method = lambda: method
    return urllib2.urlopen(req)


def make_schema_string(num=None):
    if num is not None and num >= 0:
        field_name = "f%d" % num
    else:
        field_name = "f"

    schema_str = json.dumps({
        'type': 'record',
        'name': 'myrecord',
        'fields': [
            {
                'type': 'string',
                'name': field_name
            }
        ]
    })

    return schema_str


def ping_registry(base_url):
    resp = http_request(base_url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)
    return resp.getcode()


def register_schema(base_url, schema_string, subject):
    """
    return id of registered schema, or return -1 to indicate that the request was not successful.
    """

    request_data = RegisterSchemaRequest(schema_string).to_json()
    url = base_url + "/subjects/%s/versions" % subject
    resp = http_request(url, "POST", request_data, SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    data = json.loads(resp.read())
    return int(data["id"])


def update_config(base_url, compatibility, subject=None):
    if subject is None:
        url = "%s/config" % base_url
    else:
        url = "%s/config/%s" % (base_url, subject)

    request_data = ConfigUpdateRequest(compatibility).to_json()
    http_request(url, "PUT", request_data, SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)


def get_config(base_url, subject=None):
    if subject is None:
        url = "%s/config" % base_url
    else:
        url = "%s/config/%s" % (base_url, subject)
    resp = http_request(url, "GET", "", SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    # Return the config json directly without extracting data
    return resp.read()


def get_all_subjects(base_url):
    url = "%s/subjects" % base_url
    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)
    data = resp.read()

    return json.loads(data)


def get_by_schema(base_url, schema, subject):
    url = "%s/subjects/%s" % (base_url, subject)

    request_data = RegisterSchemaRequest(schema).to_json()
    resp = http_request(url, "POST", data=request_data, headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    return json.loads(resp.read())


def get_all_versions(base_url, subject):
    url = "%s/subjects/%s/versions" % (base_url, subject)
    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    return json.loads(resp.read())


def get_schema_by_version(base_url, subject, version):
    url = "%s/subjects/%s/versions/%d" % (base_url, subject, version)
    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    return json.loads(resp.read())


def get_schema_by_id(base_url, id):
    url = "%s/schemas/ids/%d" % (base_url, id)

    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)
    return json.loads(resp.read())
