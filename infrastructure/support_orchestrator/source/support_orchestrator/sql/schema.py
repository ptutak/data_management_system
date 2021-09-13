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

from typing import Dict, Iterable, List, Optional

from support_orchestrator.exceptions.internal import NoSuchColumnException


class Column:
    def __init__(self, name: str, type: str, primary_key: bool = False, not_null: bool = False) -> None:
        self._name = name
        self._type = type
        self._primary_key = primary_key
        self._not_null = not_null

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> str:
        return self._type

    @property
    def primary_key(self) -> bool:
        return self._primary_key

    @property
    def not_null(self) -> bool:
        return self._not_null

    def create(self) -> str:
        return f"{self._name} {self._type}"

    def primary_key_option(self) -> str:
        return " PRIMARY KEY" if self.primary_key else ""

    def not_null_option(self) -> str:
        return " NOT NULL" if self.not_null else ""


class ColumnManager:
    def __init__(self, columns: Dict[str, Column]) -> None:
        self._columns = columns

    def __getattr__(self, attribute: str) -> Column:
        if attribute not in self._columns:
            raise NoSuchColumnException(f"No such column {attribute}")
        return self._columns[attribute]


class Table:
    def __init__(self, name: str, *columns: Column) -> None:
        self._name = name
        self._columns = {column.name: column for column in columns}
        self._column_manager = ColumnManager(self._columns)

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> Dict[str, Column]:
        return self._columns

    @property
    def c(self) -> ColumnManager:
        return self._column_manager

    def _row_create_str(self) -> str:
        if len(list(column for column in self._columns.values() if column.primary_key)) > 1:
            return ",\n\t".join(
                [f"{column.create()}{column.not_null_option()}" for column in self._columns.values()]
                + [self._get_pk_constraint()]
            )
        else:
            return ",\n\t".join(
                (
                    f"{column.create()}{column.not_null_option()}{column.primary_key_option()}"
                    for column in self._columns.values()
                )
            )

    def _get_pk_constraint(self) -> str:
        col_str = str(tuple(column.name for column in self._columns.values() if column.primary_key))
        col_str = col_str.replace("'", "")
        return f"CONSTRAINT pk PRIMARY KEY {col_str}"

    def create(self) -> str:
        return f"CREATE TABLE {self.name} (\n\t{self._row_create_str()}\n)"

    def create_if_not_exists(self) -> str:
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n\t{self._row_create_str()}\n)"

    def upsert(self, *values, on_duplicate_key: Optional[str] = None, **kw_values) -> str:
        if values and kw_values:
            raise RuntimeError("Cannot upsert values with and without keyword names.")
        if on_duplicate_key is not None:
            on_duplicate_key_str = f" ON DUPLICATE KEY {on_duplicate_key}"
        else:
            on_duplicate_key_str = ""
        if values:
            query = f"UPSERT INTO {self.name} VALUES {tuple(values)}{on_duplicate_key_str}"
        else:
            kw_values_str = str(tuple(kw_values.keys())).replace("'", "")
            query = (
                f"UPSERT INTO {self.name} {kw_values_str} VALUES"
                f" {self._prepare_values(kw_values.values())}{on_duplicate_key_str}"
            )
        return query

    def _get_mandatory_column_names(self) -> List[str]:
        return [column.name for column in self._columns.values() if column.not_null or column.primary_key]

    def _prepare_values(self, values: Iterable[Optional[str]]) -> str:
        def value_str(v: Optional[str]):
            if v is None:
                return "NULL"
            if isinstance(v, int):
                return f"{v}"
            return f"'{v}'"

        return "".join(["(", ",".join(value_str(value) for value in values), ")"])

    def drop(self) -> str:
        return f"DROP TABLE {self.name}"

    def drop_if_exists(self) -> str:
        return f"DROP TABLE IF EXISTS {self.name}"

    def select(self, *columns: str) -> str:
        if columns:
            self_columns_set = set(self._columns.keys())
            if not set(columns).issubset(self_columns_set):
                raise NoSuchColumnException(f"There is no such column {self_columns_set - set(columns)}")
            columns_str = ",".join(columns)
        else:
            columns_str = ",".join(self._columns.keys())
        return f"SELECT {columns_str} FROM {self.name}"
