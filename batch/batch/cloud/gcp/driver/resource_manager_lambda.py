import asyncio
import base64
import json
import logging
import os
from typing import List

import aiohttp

from gear import Database
from hailtop import httpx
from hailtop.utils import retry_transient_errors

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
                log.info(f'lambda instance {instance_id} is now active')
                log.info(f'lambda instance_info: {instance_info}')
                # last_start_timestamp_msecs = parse_timestamp_msecs(spec.get('lastStartTimestamp'))
                # assert last_start_timestamp_msecs is not None
                last_start_timestamp_msecs = instance.time_created
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

    async def _wait_for_vm_active(self, instance_id: str, timeout_seconds: int = 1200) -> dict:
        API_KEY = os.environ['LAMBDA_API_KEY']
        start_time = asyncio.get_event_loop().time()
        poll_interval = 30

        log.info(f'Waiting for Lambda VM {instance_id} to become active...')

        while True:
            current_time = asyncio.get_event_loop().time()
            elapsed = current_time - start_time

            if elapsed > timeout_seconds:
                raise RuntimeError(f'Lambda VM {instance_id} did not become active within {timeout_seconds} seconds')

            try:
                # Poll Lambda Labs API directly for VM state
                url = f'https://cloud.lambdalabs.com/api/v1/instances/{instance_id}'
                instance_info = await retry_transient_errors(
                    self.client_session.get_read_json, url, headers={'Authorization': f'Bearer {API_KEY}'}
                )

                state = instance_info['data']['status']

                if state == 'active':
                    log.info(f'Lambda VM {instance_id} is now active after {elapsed:.1f} seconds')
                    return instance_info

                elif state in ('terminating', 'terminated'):
                    raise RuntimeError(f'Lambda VM {instance_id} was terminated during startup')

                elif state == 'booting':
                    log.info(f'Lambda VM {instance_id} still booting... (elapsed: {elapsed:.1f}s)')

                else:
                    log.warning(f'Lambda VM {instance_id} in unexpected state: {state}')

            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    raise RuntimeError(f'Lambda VM {instance_id} does not exist') from e
                log.warning(f'HTTP error checking VM state (will retry): {e}')
            except Exception as e:
                log.warning(f'Error checking VM state (will retry): {e}')

            await asyncio.sleep(poll_interval)

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
                "ssh_key_names": ['batch-worker-dev-temp'],
                "file_system_names": [f'lambda-fs-{avail_region}'],
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

        return total_resources_on_instance
