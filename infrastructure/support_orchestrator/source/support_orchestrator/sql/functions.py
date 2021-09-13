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

from typing import Any, Dict, List

from .schema import Column, Table


def map_rows(select_response: List[List[Any]], columns: List[str]) -> List[Dict[str, Any]]:
    mapped_response = []
    enumerated_columns = list(enumerate(columns))
    for row in select_response:
        mapped_response.append({column: row[i] for i, column in enumerated_columns})
    return mapped_response


def create_table(table_name: str, table_schema: List[Dict[str, Any]]) -> Table:
    columns = [
        Column(
            column["column_name"],
            column["hbase_type"],
            primary_key=column["primary_key"],
            not_null=not column["nullable"],
        )
        for column in table_schema
    ]
    return Table(table_name, *columns)
