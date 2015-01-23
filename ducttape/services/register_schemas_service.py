# from .service import Service
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
DEFAULT_REQUEST_PROPERTIES = {"Content-Type": SCHEMA_REGISTRY_V1_JSON_WEIGHTED, "Accept": "*/*"}


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


class ConfigUpdateRequest(RequestData):
    def __init__(self, compatibility):
        # "none", "backward", "forward", or "full"
        self.compatibility = compatibility.upper()

    def to_json(self):
        return json.dumps({"compatibility": self.compatibility.upper()})


def http_request(url, method, data, headers):
    if url[0:7].lower() != "http://":
        url = "http://%s" % url

    req = urllib2.Request(url, data, headers)
    req.get_method = lambda: method
    resp = urllib2.urlopen(req)

    if resp.getcode() >= 400:
        raise Exception("There was an error with the http request. HTTP code " + resp.getcode())

    return resp


def make_schema_string(num=None):
    if num is not None and num >=0:
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


def register_schema(base_url, register_schema_request, subject):
    """
    return id of registered schema, or return -1 to indicate that the request was not successful.
    """

    url = base_url + "/subjects/%s/versions" % subject
    resp = http_request(url, "POST", register_schema_request.to_json(), DEFAULT_REQUEST_PROPERTIES)

    return int(resp.read()["id"])


def update_config(base_url, config_update_request, subject=None):
    if subject is None:
        url = "%s/config" % base_url
    else:
        url = "%s/config/%s" % (base_url, subject)

    http_request(url, "PUT", config_update_request.to_json(), DEFAULT_REQUEST_PROPERTIES)


def get_config(base_url, subject=None):
    if subject is None:
        url = "%s/config" % base_url
    else:
        url = "%s/config/%s" % (base_url, subject)

    resp = http_request(url, "GET", None, DEFAULT_REQUEST_PROPERTIES)
    return resp.read()


# public static Schema getId(String baseUrl, Map<String, String> requestProperties,
#                            int id) throws IOException {
#   String url = String.format("%s/subjects/%d", baseUrl, id);
#
#   Schema response = RestUtils.httpRequest(url, "GET", null, requestProperties,
#                                           GET_SCHEMA_RESPONSE_TYPE);
#   return response;
# }
#
# public static Schema getVersion(String baseUrl, Map<String, String> requestProperties,
#                                 String subject, int version) throws IOException {
#   String url = String.format("%s/subjects/%s/versions/%d", baseUrl, subject, version);
#
#   Schema response = RestUtils.httpRequest(url, "GET", null, requestProperties,
#                                           GET_SCHEMA_RESPONSE_TYPE);
#   return response;
# }
#
# public static List<Integer> getAllVersions(String baseUrl, Map<String, String> requestProperties,
#                                            String subject) throws IOException {
#   String url = String.format("%s/subjects/%s/versions", baseUrl, subject);
#
#   List<Integer> response = RestUtils.httpRequest(url, "GET", null, requestProperties,
#                                                  ALL_VERSIONS_RESPONSE_TYPE);
#   return response;
# }
#
# public static List<String> getAllSubjects(String baseUrl, Map<String, String> requestProperties)
#     throws IOException {
#   String url = String.format("%s/subjects", baseUrl);
#
#   List<String> response = RestUtils.httpRequest(url, "GET", null, requestProperties,
#                                                 ALL_TOPICS_RESPONSE_TYPE);
#   return response;
# }








#
#
#
# class RegisterSchemasService(Service):
#     """ This service issues a bunch of register requests to a schema registry service.
#     """
#     def __init__(self, cluster, num_nodes, zk, kafka, schema_registry):
#         super(RegisterSchemasService, self).__init__(cluster, num_nodes)
#         self.zk = zk
#         self.kafka = kafka
#         self.schema_registry = schema_registry
#
#     def start(self):
#         super(RegisterSchemasService, self).start()
#         template = open('templates/schema-registry.properties').read()
#
#         for idx, node in enumerate(self.nodes, 1):
#             pass
#
#     def stop(self):
#         # for idx, node in enumerate(self.nodes, 1):
#         #     self.logger.info("Stopping Schema Registry node %d on %s", idx, node.account.hostname)
#         #     self._stop_and_clean(node, True)
#         #     node.free()
#         pass
#
#     def _stop_and_clean(self, node, allow_fail=False):
#         # node.account.ssh("/opt/schema-registry/bin/schema-registry-stop", allow_fail=allow_fail)
#         # node.account.ssh("rm -rf /mnt/schema-registry.properties /mnt/schema-registry.log")
#         pass









