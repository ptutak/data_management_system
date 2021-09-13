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

import os.path

from flask import Flask

from .orchestrator.init_sequence import init

ROOT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.py")


def create_app(config_filename: str = ROOT_CONFIG_PATH) -> Flask:
    app = Flask(__name__)

    # App config
    app.config.from_pyfile(config_filename)

    init(app.config)

    # REST routes import
    from .api.base_routes import api as base_routes_api
    from .api.v1.base import api as base_api
    from .api.v1.datasets import api as datasets_api

    app.register_blueprint(base_routes_api)
    app.register_blueprint(datasets_api)
    app.register_blueprint(base_api)

    return app
