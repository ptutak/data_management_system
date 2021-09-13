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
import os
import traceback
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from subprocess import STDOUT
from typing import Any, Dict, List, Optional, Type

from flask import json
from yaml import safe_load

from support_orchestrator.const import AppConfig, OrchestratorConfYaml
from support_orchestrator.exceptions.response import FlinkHTTPException
from support_orchestrator.orchestrator.job_orchestrator import JobOrchestrator
from support_orchestrator.orchestrator.jobs import JobManager
from support_orchestrator.utils.find import (
    get_active_spark_client,
    get_flink_job_manager,
    get_log_dir,
    get_spark_job_manager,
)

LOGGER = logging.getLogger(__name__)


class DeploymentStatus(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class DeploymentResponse:
    def __init__(self, status: DeploymentStatus, content: Any) -> None:
        self._status = status
        self._content = content

    @property
    def status(self) -> DeploymentStatus:
        return self._status

    @property
    def content(self) -> Any:
        return self._content

    def __str__(self) -> str:
        return f"{self.status}: {self.content}"


class Deployment(ABC):
    def __init__(self, name: str, job_name: str, params: Dict[str, Any]) -> None:
        self._name = name
        self._job_name = job_name
        self._params = params

    @property
    def name(self) -> str:
        return self._name

    @property
    def job_name(self) -> str:
        return self._job_name

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    @abstractmethod
    def deploy(self, app_config: Dict[str, Any]) -> DeploymentResponse:
        """Deploy."""

    @abstractmethod
    def job_manager(self, app_config: Dict[str, Any]) -> JobManager:
        """Returns dedicated JobManager"""

    def get_params(self, app_config: Dict[str, Any]) -> Dict[str, Any]:
        job = self.job_manager(app_config).jobs[self.job_name]
        params = {}
        for param_name, param in job.params.items():
            LOGGER.info(f"Analyzing param: {param_name}, {param}")
            if param["default"] is not None:
                params[param_name] = param["default"]
            elif param["config_key"] is not None:
                param_value = app_config[AppConfig.JOBS_CONF].get(param["config_key"], None)
                params[param_name] = param_value
        params.update(self.params)
        return params

    def to_json(self) -> str:
        return json.dumps({"name": self.name, "job_name": self.job_name, "params": self.params})

    def get_log_dir(self, app_config: Dict[str, Any]) -> Path:
        log_dir = get_log_dir(app_config) / "jobs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir


class FlinkDeployment(Deployment):
    def job_manager(self, app_config: Dict[str, Any]) -> JobManager:
        return get_flink_job_manager(app_config)

    def deploy(self, app_config: Dict[str, Any]) -> DeploymentResponse:
        orchestrator = JobOrchestrator(app_config)
        params = self.get_params(app_config)
        jar_id = orchestrator.get_flink_job_id(self.job_name)
        try:
            LOGGER.info(f"Running Flink Jar: {jar_id}, {params}")
            response: Dict[str, Any] = orchestrator.flink_client.run_jar(jar_id, **params)
            LOGGER.info(f"Job Completed{response}")
            return DeploymentResponse(DeploymentStatus.SUCCESS, response)
        except FlinkHTTPException as e:
            LOGGER.info("".join(traceback.TracebackException.from_exception(e).format()))
            return DeploymentResponse(DeploymentStatus.ERROR, {"errors": str(e)})


class SparkDeployment(Deployment):
    def __init__(self, name: str, job_name: str, params: Dict[str, Any]) -> None:
        super().__init__(name, job_name, params)
        self._environ = {"HADOOP_USER_NAME": "spark"}

    def with_environ(self, **kwargs: Any) -> "SparkDeployment":
        self._environ.update(kwargs)
        return self

    def job_manager(self, app_config: Dict[str, Any]) -> JobManager:
        return get_spark_job_manager(app_config)

    def deploy(self, app_config: Dict[str, Any]) -> DeploymentResponse:
        job = self.job_manager(app_config).jobs[self.job_name]
        params = self.get_params(app_config)
        spark_client = get_active_spark_client(app_config)
        LOGGER.info(f"Running Spark Job:{self.job_name}")
        LOGGER.info(f"{params}")
        LOGGER.info(f"{job.path}")
        response = spark_client.spark_submit(
            job.path,
            stdout=open(self.get_log_dir(app_config) / f"{self.name}.out", "a", buffering=1),
            stderr=STDOUT,
            environ=self._environ,
            **params,
        )
        return DeploymentResponse(DeploymentStatus.SUCCESS, response)


class SparkContinuousDeployment(SparkDeployment):
    def deploy(self, app_config: Dict[str, Any]) -> DeploymentResponse:
        job = self.job_manager(app_config).jobs[self.job_name]
        params = self.get_params(app_config)
        spark_client = get_active_spark_client(app_config)
        LOGGER.info(f"Running Spark Job in Background: {self.job_name}")
        LOGGER.info(f"{params}")
        LOGGER.info(f"{job.path}")
        response = spark_client.spark_submit_continuous(
            job.path,
            stdout=open(self.get_log_dir(app_config) / f"{self.name}.out", "a", buffering=1),
            stderr=STDOUT,
            **params,
        )
        return DeploymentResponse(DeploymentStatus.SUCCESS, response)


class DeploymentManager:
    JOBS_MAPPING: Dict[str, Type[Deployment]] = {
        "flink": FlinkDeployment,
        "spark": SparkDeployment,
        "spark_continuous": SparkContinuousDeployment,
    }

    def __init__(self, app_config: Optional[Dict[str, Any]] = None) -> None:
        self._deployments: Dict[str, Deployment] = {}
        self._deployment_dirs: List[Path] = []
        if app_config is not None:
            deployments_dir = Path(app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.DEPLOYMENTS_JOBS_DIR])
            self.add_deployment_dir(deployments_dir)

    def get_deployments(self, deployment_path: Path) -> Dict[str, Deployment]:
        deployments = {}
        for path in os.listdir(deployment_path):
            if not path.endswith(".yml") and not path.endswith(".yaml"):
                continue
            with open(deployment_path / path) as file:
                content = safe_load(file.read())
            for job in content["deployments"]:
                new_deployment = self.prepare_deployment(job["name"], job["type"], job["job_name"], job["params"])
                deployments[new_deployment.name] = new_deployment
        return deployments

    @property
    def deployments(self) -> Dict[str, Deployment]:
        return self._deployments

    def add_deployment_dir(self, path: Path) -> None:
        self._deployments.update(self.get_deployments(path))
        self._deployment_dirs.append(path)

    @classmethod
    def prepare_deployment(cls, name: str, type: str, job_name: str, params: Dict[str, Any]) -> Deployment:
        return cls.JOBS_MAPPING[type](name, job_name, params)
