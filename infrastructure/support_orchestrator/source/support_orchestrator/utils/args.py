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

import itertools
from typing import List


def format_args(*args, **kwargs) -> List[str]:
    multiple_formatted = [
        (f"--{key.replace('_', '-')}", l_value)
        for key, value in kwargs.items()
        if isinstance(value, List)
        for l_value in value
    ]
    multiple_formatted = itertools.chain(*multiple_formatted)
    flags_formatted = [
        f"--{key.replace('_', '-')}" for key, value in kwargs.items() if isinstance(value, bool) and value is True
    ]
    kwargs_formatted = {
        f"--{key.replace('_', '-')}": value
        for key, value in kwargs.items()
        if not (isinstance(value, List) or isinstance(value, bool))
    }
    kwargs_flattened = itertools.chain(*kwargs_formatted.items())
    return list(args) + list(kwargs_flattened) + list(multiple_formatted) + flags_formatted
