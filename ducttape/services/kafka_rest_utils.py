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

KAFKA_V1_JSON = "application/vnd.kafka.v1+json"
KAFKA_V1_JSON_WEIGHTED = KAFKA_V1_JSON

# These are defaults that track the most recent API version. These should always be specified
# anywhere the latest version is produced/consumed.
KAFKA_MOST_SPECIFIC_DEFAULT = KAFKA_V1_JSON
KAFKA_DEFAULT_JSON = "application/vnd.kafka+json"
KAFKA_DEFAULT_JSON_WEIGHTED = KAFKA_DEFAULT_JSON + "; qs=0.9"
JSON = "application/json"
JSON_WEIGHTED = JSON + "; qs=0.5"

PREFERRED_RESPONSE_TYPES = [KAFKA_V1_JSON, KAFKA_DEFAULT_JSON, JSON]

# Minimal header for using the kafka rest api
KAFKA_REST_DEFAULT_REQUEST_PROPERTIES = {"Content-Type": KAFKA_V1_JSON_WEIGHTED, "Accept": "*/*"}