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

INTERVAL=2
FAIL_OK=false

while [[ $# -gt 0 ]]
do
  arg="$1"
  case $arg in
    -i|--interval)
      INTERVAL="$2"
      shift 2
      ;;
    --fail-ok)
      FAIL_OK=true
      shift
      ;;
    *)
      break
      ;;
  esac
done

if $FAIL_OK
then
    while "$@" || true
    do
        sleep "$INTERVAL"
    done
else
    while "$@"
    do
        sleep "$INTERVAL"
    done
fi
