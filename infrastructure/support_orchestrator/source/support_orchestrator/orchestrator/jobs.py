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

import os
from pathlib import Path
from typing import Any, Dict, List

from yaml import safe_load


class Job:
    def __init__(self, name: str, path: Path, params: List[Dict[str, Any]]) -> None:
        self._name = name
        self._path = path
        self._params: Dict[str, Dict[str, Any]] = {
            param["name"]: {
                "type": param["type"],
                "default": param.get("default", None),
                "config_key": param.get("config_key", None),
            }
            for param in params
        }

    @property
    def params(self) -> Dict[str, Dict[str, Any]]:
        return self._params

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> Path:
        return self._path


class JobManager:
    def __init__(self, jobs_path: Path, extension: str) -> None:
        self._jobs_dir = jobs_path
        self._jobs: Dict[str, Job] = self.get_jobs(jobs_path, extension)

    @property
    def jobs_dir(self) -> Path:
        return self._jobs_dir

    @classmethod
    def get_jobs(cls, dirname: Path, extension: str) -> Dict[str, Job]:
        job_storage: Dict[str, Job] = {}
        jobs_dir_path = dirname
        flink_files = set(os.listdir(jobs_dir_path))

        for file in flink_files:
            if file.endswith(extension) and (
                file.replace(extension, ".yml") in flink_files or file.replace(extension, ".yaml") in flink_files
            ):
                job_path = jobs_dir_path / file
                if (jobs_dir_path / file.replace(extension, ".yml")).exists():
                    job_config_path = jobs_dir_path / file.replace(extension, ".yml")
                else:
                    job_config_path = jobs_dir_path / file.replace(extension, ".yaml")

                with open(job_config_path) as job_content:
                    job_content_object = safe_load(job_content)
                new_job = Job(file.replace(extension, ""), job_path, job_content_object["params"])
                job_storage[new_job.name] = new_job
        return job_storage

    @property
    def jobs(self) -> Dict[str, Job]:
        return self._jobs
