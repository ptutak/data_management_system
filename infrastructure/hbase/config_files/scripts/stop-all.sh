#!/bin/bash
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

"$HBASE_HOME/bin/stop-hbase.sh"

if [[ -f "$HBASE_HOME/pid/hbase-hbase-master.pid" ]]
then
    MASTER_PID="$(cat "$HBASE_HOME/pid/hbase-hbase-master.pid")"
    kill -9 "$MASTER_PID"
fi

while read -r regionserver
do
    ssh -n "$regionserver" "$HBASE_HOME/bin/hbase-daemon.sh stop regionserver"
done <"$HBASE_HOME/conf/regionservers"
