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

import logging
import os.path
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List

from flask import Blueprint, current_app, request
from flask.json import jsonify
from flask.wrappers import Response

from support_orchestrator.api.response import ApiResponse, ResponseState
from support_orchestrator.exceptions.response import DatasetException
from support_orchestrator.orchestrator.dataset_orchestrator import DatasetOrchestrator
from support_orchestrator.utils.validate import validate

api = Blueprint("api.v1.datasets", __name__, url_prefix="/api/v1/datasets")


LOGGER = logging.getLogger(__name__)


SCHEMA_TYPES_MAPPING = {
    "string": {"spark_type": "StringType", "hbase_type": "VARCHAR"},
    "integer": {"spark_type": "IntegerType", "hbase_type": "INTEGER"},
    "biginteger": {"spark_type": "LongType", "hbase_type": "BIGINT"},
    "boolean": {"spark_type": "BooleanType", "hbase_type": "BOOLEAN"},
    "double": {"spark_type": "DoubleType", "hbase_type": "DOUBLE"},
    "timestamp": {"spark_type": "TimestampType", "hbase_type": "TIMESTAMP"},
}


ROOT = Path(os.path.dirname(__file__))

DATASET_POST_SCHEMA = ROOT / "schema" / "dataset_post_schema.yml"
DATASET_POST_TABLE_SCHEMA = ROOT / "schema" / "dataset_post_table_schema.yaml"
DATASET_POST_OUTPUT_SCHEMA = ROOT / "schema" / "dataset_post_output_schema.yaml"


@api.route("/", methods=("GET",))
def datasets() -> Response:
    def datasets_list(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.datasets()

    return prepare_response(datasets_list)


@api.route("/", methods=("POST",))
def dataset_add() -> Response:
    post_content = request.json
    errors = validate(post_content, DATASET_POST_SCHEMA)
    if errors is not None:
        return jsonify(ApiResponse(ResponseState.FAILURE, {"errors": errors}).to_jsonable())

    def add_dataset(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.add_dataset(post_content["name"], create_schema(post_content["schema"]))

    return prepare_response(add_dataset)


@api.route("/<dataset_id>", methods=("GET",))
def dataset_get(dataset_id: str) -> Response:
    def get_dataset(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.get_dataset(dataset_id)

    return prepare_response(get_dataset)


@api.route("/<dataset_id>", methods=("DELETE",))
def dataset_delete(dataset_id: str) -> Response:
    return jsonify({})


@api.route("/<dataset_id>/output", methods=("GET",))
def dataset_outputs(dataset_id: str) -> Response:
    def dataset_outputs_list(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.dataset_outputs(dataset_id)

    return prepare_response(dataset_outputs_list)


@api.route("/<dataset_id>/output", methods=("POST",))
def dataset_output_add(dataset_id: str) -> Response:
    post_content = request.json
    errors = validate(post_content, DATASET_POST_OUTPUT_SCHEMA)
    if errors is not None:
        return jsonify(ApiResponse(ResponseState.FAILURE, {"errors": errors}).to_jsonable())
    select_expr: List[str] = post_content["select_expression"]

    def add_dataset_output(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.add_dataset_output(dataset_id, select_expr)

    return prepare_response(add_dataset_output)


@api.route("/<dataset_id>/output/<output_id>", methods=("GET",))
def dataset_output_get(dataset_id: str, output_id: str) -> Response:
    def get_dataset_output(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.get_dataset_output(dataset_id, output_id)

    return prepare_response(get_dataset_output)


@api.route("/<dataset_id>/output/<output_id>", methods=("DELETE",))
def dataset_output_delete(dataset_id: str, output_id: str) -> Response:
    return jsonify({})


@api.route("/<dataset_id>/tables", methods=("GET",))
def dataset_tables(dataset_id: str) -> Response:
    def dataset_tables_list(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.dataset_tables(dataset_id)

    return prepare_response(dataset_tables_list)


@api.route("/<dataset_id>/tables", methods=("POST",))
def dataset_table_add(dataset_id: str) -> Response:
    post_content = request.json
    errors = validate(post_content, DATASET_POST_TABLE_SCHEMA)
    if errors is not None:
        return jsonify(ApiResponse(ResponseState.FAILURE, {"errors": errors}).to_jsonable())
    LOGGER.info(f"Created schema: {create_schema(post_content['schema'])}")

    def add_dataset_table(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.add_dataset_table(
            dataset_id, post_content["name"], create_schema(post_content["schema"]), post_content["select_expression"]
        )

    return prepare_response(add_dataset_table)


@api.route("/<dataset_id>/tables/<table_id_or_name>", methods=("GET",))
def dataset_table_get(dataset_id: str, table_id_or_name: str) -> Response:
    def get_dataset_table(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.get_dataset_table(dataset_id, table_id_or_name)

    return prepare_response(get_dataset_table)


@api.route("/<dataset_id>/tables/<table_id_or_name>", methods=("DELETE",))
def dataset_table_delete(dataset_id: str, table_id_or_name: str) -> Response:
    return jsonify({})


@api.route("/<dataset_id>/tables/<table_id_or_name>/output", methods=("GET",))
def dataset_table_outputs(dataset_id: str, table_id_or_name: str) -> Response:
    def dataset_table_outputs_list(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.dataset_table_outputs(dataset_id, table_id_or_name)

    return prepare_response(dataset_table_outputs_list)


@api.route("/<dataset_id>/tables/<table_id_or_name>/output", methods=("POST",))
def dataset_table_output_add(dataset_id: str, table_id_or_name: str) -> Response:
    def add_dataset_table_output(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.add_dataset_table_output(dataset_id, table_id_or_name)

    return prepare_response(add_dataset_table_output)


@api.route("/<dataset_id>/tables/<table_id_or_name>/output/<output_id>", methods=("GET",))
def dataset_table_output_get(dataset_id: str, table_id_or_name: str, output_id: str) -> Response:
    def get_dataset_table_output(dataset_orchestrator: DatasetOrchestrator) -> List[Dict[str, Any]]:
        return dataset_orchestrator.get_dataset_table_output(dataset_id, table_id_or_name, output_id)

    return prepare_response(get_dataset_table_output)


@api.route("/<dataset_id>/tables/<table_id_or_name>/output/<output_id>", methods=("DELETE",))
def dataset_table_output_delete(dataset_id: str, table_id_or_name: str, output_id: str) -> Response:
    "?"
    return jsonify({})


def prepare_response(orchestrator_func: Callable[[DatasetOrchestrator], List[Dict[str, Any]]]) -> Response:
    dataset_orchestrator = DatasetOrchestrator(current_app.config)
    try:
        response = orchestrator_func(dataset_orchestrator)
        return jsonify(ApiResponse(ResponseState.SUCCESS, response).to_jsonable())
    except DatasetException as e:
        LOGGER.info("".join(traceback.TracebackException.from_exception(e).format()))
        return jsonify(ApiResponse(ResponseState.FAILURE, e.content).to_jsonable())
    except Exception as e:
        LOGGER.info("".join(traceback.TracebackException.from_exception(e).format()))
        return jsonify(
            ApiResponse(
                ResponseState.FAILURE, {"errors": "".join(traceback.TracebackException.from_exception(e).format())}
            ).to_jsonable()
        )


def create_schema(content_schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    changed_schema = []
    for item in content_schema:
        changed_schema.append(
            {
                "column_name": item["column_name"],
                "spark_type": SCHEMA_TYPES_MAPPING[item["column_type"]]["spark_type"],
                "hbase_type": SCHEMA_TYPES_MAPPING[item["column_type"]]["hbase_type"],
                "nullable": item["nullable"],
                "primary_key": item.get("primary_key", False),
            }
        )
    return changed_schema
