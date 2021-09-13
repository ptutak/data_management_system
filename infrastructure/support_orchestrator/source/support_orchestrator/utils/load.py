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

from typing import Any, Dict, List, Tuple

from support_orchestrator.clients.flink import FlinkClient
from support_orchestrator.clients.hbase import HBaseClient
from support_orchestrator.const import HBaseTables
from support_orchestrator.orchestrator.jobs import Job, JobManager


def get_stored_jobs(hbase_client: HBaseClient) -> Dict[Tuple[str, str], str]:
    column_list = ",".join(HBaseTables.FLINK_JOBS.columns.keys())
    all_jobs = hbase_client.select(f"SELECT {column_list} FROM {HBaseTables.FLINK_JOBS.name}")
    return {(job_name, jobmanager_name): job_id for job_name, jobmanager_name, job_id in all_jobs}


def update_job(job: Job, jobmanager_address: str, flink_client: FlinkClient, hbase_client: HBaseClient) -> None:
    response = flink_client.upload_jar(job.path)
    if response["status"] == "success":
        hbase_client.execute(
            HBaseTables.FLINK_JOBS.upsert(
                job_name=job.name,
                jobmanager_address=jobmanager_address,
                jar_id=response["filename"].split("/")[-1],
            )
        )


def check_if_job_is_uploaded(loaded_jobs: List[Dict[str, Any]], job_id: str) -> bool:
    for job in loaded_jobs:
        if job["id"] == job_id:
            return True
    return False


def load_flink_jobs(flink_client: FlinkClient, hbase_client: HBaseClient, job_manager: JobManager) -> None:
    stored_jobs = get_stored_jobs(hbase_client)
    uploaded_jars = flink_client.get_jars()
    loaded_jobs: List[Dict[str, Any]] = uploaded_jars["files"]
    jobmanager_address = uploaded_jars["address"]
    for job in job_manager.jobs.values():
        if (job.name, jobmanager_address) not in stored_jobs:
            update_job(job, jobmanager_address, flink_client, hbase_client)
        else:
            job_id = stored_jobs[(job.name, jobmanager_address)]
            if not check_if_job_is_uploaded(loaded_jobs, job_id):
                update_job(job, jobmanager_address, flink_client, hbase_client)
