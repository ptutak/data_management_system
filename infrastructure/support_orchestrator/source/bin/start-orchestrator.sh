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

set -e
shopt -s extglob
function resolve_dir () {
    SOURCE="$1"
    while [ -h "$SOURCE" ]
    do
        # resolve $SOURCE until the file is no longer a symlink
        # if $SOURCE was a relative symlink, we need to resolve it
        # relative to the path where the symlink file was located
        DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
    echo "$DIR"
}

if [[ -z "$SUPPORT_HOME" ]]
then
    SUPPORT_CONF_DIR="$( readlink -f "$( resolve_dir "${BASH_SOURCE[0]}")/../conf")"
else
    SUPPORT_CONF_DIR="$( readlink -f "$SUPPORT_HOME/conf" )"
fi
export SUPPORT_CONF_DIR

if [[ -f "$SUPPORT_CONF_DIR/orchestrator-env.sh" ]]
then
    source "$SUPPORT_CONF_DIR/orchestrator-env.sh"
fi

if [[ -z "$SUPPORT_HOME" ]]
then
    SUPPORT_HOME="$( readlink -f "$SUPPORT_CONF_DIR/.." )"
fi
export SUPPORT_HOME

source "$SUPPORT_HOME/bin/orchestrator-defaults.sh"

echo "Orchestrator starting... at $SUPPORT_HOME"
if [[ -z "$SUPPORT_DEBUG_MODE" ]]
then
    cd "$SUPPORT_HOME" && waitress-serve --listen="$SUPPORT_LISTEN_ADDRESS" --call 'support_orchestrator.app:create_app'
else
    IFS=":" read -ra host_port <<<"$SUPPORT_LISTEN_ADDRESS"
    cd "$SUPPORT_HOME" && FLASK_APP="support_orchestrator.app" flask run --host "${host_port[0]}" --port "${host_port[1]}"
fi
