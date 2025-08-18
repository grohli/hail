import base64
import json
import logging
import os
from typing import List

import aiohttp

from gear import Database
from hailtop import httpx
from hailtop.utils import check_shell, retry_transient_errors

from ....driver.instance import Instance
from ....driver.resource_manager import (
    CloudResourceManager,
    UnknownVMState,
    VMDoesNotExist,
    VMState,
    VMStateCreating,
    VMStateRunning,
    VMStateTerminated,
)
from ....file_store import FileStore
from ....instance_config import QuantifiedResource
from ..instance_config import LambdaSlimInstanceConfig
from ..resource_utils import (
    GCP_MACHINE_FAMILY,
    family_worker_type_cores_to_gcp_machine_type,
    gcp_machine_type_to_cores_and_memory_bytes,
)
from .billing_manager import GCPBillingManager

log = logging.getLogger('resource_manager')


class LambdaResourceManager(CloudResourceManager):
    def __init__(
        self,
        db: Database,
        billing_manager: GCPBillingManager,
        client_session: httpx.ClientSession,
    ):
        self.db = db
        self.billing_manager = billing_manager
        self.client_session = client_session

    async def delete_vm(self, instance: Instance):
        API_KEY = os.environ['LAMBDA_API_KEY']
        BASE_URL = 'https://cloud.lambdalabs.com/api/v1/'
        HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}
        instance_id = instance.instance_config.instance_id

        if instance_id:
            url = f'{BASE_URL}instance-operations/terminate'
            payload = {"instance_ids": [instance_id]}
            try:
                await self.client_session.post(url, headers=HEADERS, json=payload)
                log.info(f'Terminated Lambda Labs machine {instance_id}')
            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    raise VMDoesNotExist() from e
                raise

    async def get_vm_state(self, instance: Instance) -> VMState:
        API_KEY = os.environ['LAMBDA_API_KEY']
        BASE_URL = 'https://cloud.lambdalabs.com/api/v1/'

        spec = 'lambda'

        instance_id = instance.instance_config.instance_id
        if not instance_id:
            return VMStateCreating(spec, instance.time_created)

        log.info(f'lambda instance id: {instance_id}')

        url = f'{BASE_URL}instances/{instance_id}'
        payload = {
            "id": instance_id,
        }

        try:
            instance_info = await retry_transient_errors(
                self.client_session.get_read_json, url, headers={'Authorization': f'Bearer {API_KEY}'}, json=payload
            )
            state = instance_info['data']['status']
            if state == 'booting':
                return VMStateCreating(spec, instance.time_created)
            if state == 'active':
                # last_start_timestamp_msecs = parse_timestamp_msecs(spec.get('lastStartTimestamp'))
                # assert last_start_timestamp_msecs is not None
                last_start_timestamp_msecs = instance.time_created
                await check_shell(
                    f"""scp -i /lambda-ssh-key/lambda-ssh-key.pem -o StrictHostKeyChecking=no /lambda-gsa-key/key.json ubuntu@{instance_info['data']['ip']}:key.json"""
                )
                command = f"""
cat > run.sh <<'EOF'
set -ex
sudo gcloud auth activate-service-account --key-file=key.json
sudo gcloud auth configure-docker us-docker.pkg.dev --quiet
sudo docker pull us-docker.pkg.dev/hail-vdc/hail/batch-worker:cache
sudo mkdir /host
INSTANCE_CONFIG='{base64.b64encode(json.dumps(instance.instance_config.to_dict()).encode()).decode()}'
sudo docker run \
--name worker \
-e CLOUD=lambda \
-e CORES=10 \
-e NAME=lambda-worker-blah \
-e NAMESPACE=parsa \
-e ACTIVATION_TOKEN=abcdef \
-e IP_ADDRESS={instance_info['data']['ip']} \
-e BATCH_LOGS_STORAGE_URI=gs://nnf-parsa \
-e INSTANCE_ID=lambda-machine \
-e PROJECT=hail-vdc \
-e ZONE=us-central1a \
-e REGION=us-central \
-e DOCKER_PREFIX=us-docker.pkg.dev/hail-vdc/hail \
-e DOCKER_ROOT_IMAGE=ubuntu:22.04 \
-e INSTANCE_CONFIG=$INSTANCE_CONFIG \
-e MAX_IDLE_TIME_MSECS=30000 \
-e BATCH_WORKER_IMAGE=us-docker.pkg.dev/hail-vdc/hail/batch-worker:cache \
-e BATCH_WORKER_IMAGE_ID=jldksfja \
-e INTERNET_INTERFACE=eth0 \
-e UNRESERVED_WORKER_DATA_DISK_SIZE_GB=5 \
-e ACCEPTABLE_QUERY_JAR_URL_PREFIX=gs://nnf-parsa \
-e INTERNAL_GATEWAY_IP=kjdfjklsdajflksadjf \
-v /var/run/docker.sock:/var/run/docker.sock \
-v /var/run/netns:/var/run/netns:shared \
-v /usr/bin/docker:/usr/bin/docker \
-v /batch:/batch:shared \
-v /logs:/logs \
-v /global-config:/global-config \
-v /deploy-config:/deploy-config \
-v /cloudfuse:/cloudfuse:shared \
-v /etc/netns:/etc/netns \
-v /sys/fs/cgroup:/sys/fs/cgroup \
--mount type=bind,source=/host,target=/host \
--mount type=bind,source=/dev,target=/dev,bind-propagation=rshared \
--device /dev/fuse \
--device /dev \
--privileged \
--cap-add SYS_ADMIN \
--security-opt apparmor:unconfined \
--network host \
--cgroupns host \
--gpus all \
us-docker.pkg.dev/hail-vdc/hail/batch-worker:cache \
python3 -u -m batch.worker.worker
'EOF'
bash run.sh
"""

                await check_shell(
                    f"""ssh -i /lambda-ssh-key/lambda-ssh-key.pem -o StrictHostKeyChecking=no ubuntu@{instance_info['data']['ip']} 'bash -s > log.txt' <<EOF
{command}
EOF""")
                return VMStateRunning(spec, last_start_timestamp_msecs)
            if state in ('terminating', 'terminated'):
                return VMStateTerminated(spec)
            log.exception(f'Unknown gce state {state} for {instance}')
            return UnknownVMState(spec)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                raise VMDoesNotExist() from e
            raise

    def machine_type(self, cores: int, worker_type: str, local_ssd: bool) -> str:  # pylint: disable=unused-argument
        return family_worker_type_cores_to_gcp_machine_type(GCP_MACHINE_FAMILY, worker_type, cores)

    def instance_config(
        self,
        machine_type: str,
        preemptible: bool,
        local_ssd_data_disk: bool,
        data_disk_size_gb: int,
        boot_disk_size_gb: int,
        job_private: bool,
        location: str,
    ):
        return LambdaSlimInstanceConfig.create(
            self.billing_manager.product_versions, machine_type, preemptible, job_private, location, None
        )

    async def update_lambda_vm_instance_id(self, machine_name, instance_config):
        await self.db.execute_update(
            """
UPDATE instances
SET instance_config = %s WHERE name = %s;
""",
            (
                instance_config,
                machine_name,
            ),
        )

    async def _available_regions(self, response_json, machine_type):
        try:
            # Field as specified in Lambda Labs API docs
            # https://cloud.lambda.ai/api/v1/docs#get-/api/v1/instance-types
            regional_availability = response_json["data"][machine_type]["regions_with_capacity_available"]
            log.info(f'Retrieved regional availability for {machine_type}: {regional_availability}')
            return regional_availability
        except Exception:
            log.exception(f'Error retrieving available regions for {machine_type}, nothing available in any region.')
            return []  # Return empty list instead of None

    async def available_regions_from_machine_type(self, machine_type):
        API_KEY = os.environ.get('LAMBDA_API_KEY')
        if not API_KEY:
            log.error('No API key given, LAMBDA_API_KEY environment variable not set')
            raise RuntimeError('No API key given, LAMBDA_API_KEY environment variable not set')

        BASE_URL = 'https://cloud.lambdalabs.com/api/v1/'
        HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}

        try:
            url = f'{BASE_URL}instance-types'
            log.info(f'Making GET request to: {url}')
            response = await self.client_session.get(url, headers=HEADERS)
            log.info(f'Response status: {response.status}')

            response_data = await response.json()
            log.info(f'Response data keys: {list(response_data.keys()) if response_data else "No data"}')

            available_regions = await self._available_regions(response_data, machine_type)
            return available_regions
        except Exception as e:
            log.error(f'Error retrieving available regions for {machine_type}: {type(e).__name__}: {e!s}')
            raise e

    async def create_vm(
        self,
        file_store: FileStore,
        machine_name: str,
        activation_token: str,
        max_idle_time_msecs: int,
        local_ssd_data_disk: bool,
        data_disk_size_gb: int,
        boot_disk_size_gb: int,
        preemptible: bool,
        job_private: bool,
        location: str,
        machine_type: str,
        instance_config: LambdaSlimInstanceConfig,
    ) -> List[QuantifiedResource]:
        API_KEY = os.environ['LAMBDA_API_KEY']
        BASE_URL = 'https://cloud.lambdalabs.com/api/v1/'
        HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}

        default_region = 'us-east-1'
        cores, memory_in_bytes = gcp_machine_type_to_cores_and_memory_bytes(machine_type)
        cores_mcpu = cores * 1000
        total_resources_on_instance = instance_config.quantified_resources(
            cpu_in_mcpu=cores_mcpu, memory_in_bytes=memory_in_bytes, extra_storage_in_gib=0
        )

        # Get available regions for this machine type
        try:
            available_regions = await self.available_regions_from_machine_type(machine_type)
            if not available_regions:
                raise RuntimeError(f'No available regions found for machine type {machine_type}')

            # Use the first available region
            avail_region = available_regions[0]['name']
            log.info(f'Selected region {avail_region} for machine type {machine_type}')

        except Exception as e:
            log.error(f'Failed to get available regions for {machine_type}: {e}')
            # Fallback to hardcoded region if available regions check fails for some reason
            avail_region = default_region
            log.error(f'Falling back to default region: {avail_region}')

        try:
            url = f'{BASE_URL}instance-operations/launch'
            payload = {
                "region_name": avail_region,
                "instance_type_name": machine_type,
                "ssh_key_names": ['batch-worker'],
                "quantity": 1,
            }
            log.info(f'LambdaLabs API request payload: {payload}')
            response = await self.client_session.post(url, headers=HEADERS, json=payload)
            log.info(f'LambdaLabs API response status: {response.status}')

            response_data = await response.json()
            log.info(f'LambdaLabs API response data: {response_data}')

            if response.status != 200:
                raise RuntimeError(f'LambdaLabs API returned status {response.status}: {response_data}')

            instance_id = response_data['data']['instance_ids'][0]
            instance_config.instance_id = instance_id
            new_instance_config = base64.b64encode(json.dumps(instance_config.to_dict()).encode()).decode()
            log.info(f'created Lambda Labs machine {machine_name} with instance id {instance_id}')
            await self.update_lambda_vm_instance_id(machine_name, new_instance_config)

        except Exception as e:
            log.error(f'Full exception details: {type(e).__name__}: {e!s}')
            log.exception(f'error while creating Lambda Labs machine {machine_name}')
            raise e

        # TODO: create a function that checks the state of the VM
        # if the VM is running, then call create_vm_config_lambda() after this try-except block.
        # This will entail a step where we call _transfer_credentials and _setup_batch_worker.
        # As such, we need to pass (among other things) the IP address of the VM.
        # This can be accessed via instance_info['data']['ip'] as per above.

        return total_resources_on_instance
