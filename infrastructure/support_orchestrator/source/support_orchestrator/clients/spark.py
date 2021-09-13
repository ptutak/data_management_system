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
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO, Union

from support_orchestrator.utils.args import format_args

LOGGER = logging.getLogger(__name__)


class SparkClient:
    def __init__(self, spark_submit_bin_path: Path, spark_run_continuous_path: Path) -> None:
        self._spark_submit_bin_path = spark_submit_bin_path
        self._spark_run_continuous_path = spark_run_continuous_path

    def _prepare_args(self, job_path: Path, *args: str, **kwargs: Any) -> List[str]:
        kwargs_formatted = format_args(**kwargs)
        args_formatted = list(args) + kwargs_formatted
        full_args = [str(self._spark_submit_bin_path), str(job_path.absolute())] + args_formatted
        full_args_str = [str(arg) for arg in full_args]
        return full_args_str

    def spark_submit(
        self,
        job_path: Path,
        *args: str,
        stdout: Optional[Union[TextIO, int]] = None,
        stderr: Optional[Union[TextIO, int]] = None,
        environ: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        full_args_list = self._prepare_args(job_path, *args, **kwargs)
        nohup_args_list = ["nohup"] + full_args_list
        LOGGER.info(f"Spark Job:{nohup_args_list}")
        return self._submit(nohup_args_list, stdout=stdout, stderr=stderr, environ=environ)

    def spark_submit_continuous(
        self,
        job_path: Path,
        *args: str,
        stdout: Optional[Union[TextIO, int]] = None,
        stderr: Optional[Union[TextIO, int]] = None,
        environ: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        full_args_list = self._prepare_args(job_path, *args, **kwargs)
        nohup_watch_args = ["nohup", str(self._spark_run_continuous_path)] + full_args_list
        LOGGER.info(f"Spark Job Continuous: {nohup_watch_args}")
        return self._submit(nohup_watch_args, stdout=stdout, stderr=stderr, environ=environ)

    def _submit(
        self,
        args: List[str],
        stdout: Optional[Union[TextIO, int]] = None,
        stderr: Optional[Union[TextIO, int]] = None,
        environ: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        LOGGER.info(f"Spark submit path: {self._spark_submit_bin_path}")
        LOGGER.info(f"Stdout: {stdout}")
        LOGGER.info(f"Stderr: {stderr}")
        process = subprocess.Popen(
            args,
            stdout=stdout,
            stderr=stderr,
            bufsize=1,
            env=environ,
        )
        LOGGER.info(f"New process: {process}")
        return {"PID": process.pid}
