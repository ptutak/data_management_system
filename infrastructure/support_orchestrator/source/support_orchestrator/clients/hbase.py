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
from contextlib import contextmanager
from typing import List, cast

import phoenixdb
from phoenixdb.connection import Connection
from phoenixdb.cursor import Cursor

LOGGER = logging.getLogger(__name__)


class HBaseClient:
    def __init__(self, queryserver_url: str) -> None:
        self._queryserver_url = queryserver_url

    @contextmanager
    def session(self, autocommit: bool = True) -> Cursor:
        connection: Connection = phoenixdb.connect(self._queryserver_url, autocommit=autocommit)
        cursor: Cursor = connection.cursor()
        try:
            with cursor:
                yield cursor
            if not autocommit:
                connection.commit()
        finally:
            connection.close()

    def use(self, schema: str) -> None:
        with cast(Cursor, self.session()) as session:
            session.execute(f"USE {schema}")

    def use_default(self) -> None:
        with cast(Cursor, self.session()) as session:
            session.execute("USE DEFAULT")

    def show_tables(self) -> List[List[str]]:
        with cast(Cursor, self.session()) as session:
            session.execute("SHOW TABLES")
            return session.fetchall()

    def show_table(self, table: str) -> List[List[str]]:
        with cast(Cursor, self.session()) as session:
            session.execute(f"SHOW TABLE {table}")
            return session.fetchall()

    def execute(self, sql: str) -> None:
        LOGGER.info(sql)
        with cast(Cursor, self.session()) as session:
            session.execute(sql)

    def select(self, select_str: str) -> List[List[str]]:
        LOGGER.info(select_str)
        with cast(Cursor, self.session()) as session:
            session.execute(select_str)
            return session.fetchall()
