# Copyright 2021 Piotr Tutak

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import Blueprint, current_app
from flask.json import jsonify
from flask.wrappers import Response

from support_orchestrator.utils.find import get_active_hbase_client

api = Blueprint("api.v1.jobs", __name__, url_prefix="/api/v1/base")


@api.route("/", methods=("GET",))
def root() -> Response:
    return jsonify({})


@api.route("/spark", methods=("GET",))
def spark_flows() -> Response:
    hbase_client = get_active_hbase_client(current_app.config)
    return jsonify(hbase_client.select("SELECT * FROM SPARK_DATAFLOWS"))


@api.route("/flink", methods=("GET",))
def flink_flows() -> Response:
    hbase_client = get_active_hbase_client(current_app.config)
    return jsonify(hbase_client.select("SELECT * FROM FLINK_DATAFLOWS"))


@api.route("/tables", methods=("GET",))
def tables() -> Response:
    hbase_client = get_active_hbase_client(current_app.config)
    return jsonify(hbase_client.select("SELECT * FROM DATA_TABLES"))
