import asyncio
import collections
import logging
import os
import re
import secrets
from typing import Any, Counter, Dict, List, Optional, Tuple

import sortedcontainers

from gear import Database
from gear.time_limited_max_size_cache import TimeLimitedMaxSizeCache
from hailtop import aiotools
from hailtop.utils import periodically_call, retry_transient_errors, secret_alnum_string, time_msecs

from ...globals import INSTANCE_VERSION, live_instance_states
from ...instance_config import QuantifiedResource
from ..instance import Instance
from ..location import CloudLocationMonitor
from ..resource_manager import (
    CloudResourceManager,
    UnknownVMState,
    VMDoesNotExist,
    VMStateCreating,
    VMStateRunning,
    VMStateTerminated,
)

SIXTY_SECONDS_NS = 60 * 1000 * 1000 * 1000
CACHE_CAPACITY = 1000

log = logging.getLogger('inst_coll_manager')


class InstanceCollectionManager:
    def __init__(
        self,
        db: Database,  # BORROWED
        machine_name_prefix: str,
        location_monitor: CloudLocationMonitor,
        default_region: str,
        regions: List[str],
    ):
        self.db: Database = db
        self.machine_name_prefix = machine_name_prefix
        self.location_monitor = location_monitor

        assert default_region in regions, (default_region, regions)
        self._default_region = default_region
        self.regions = regions

        self.inst_coll_regex = re.compile(f'{self.machine_name_prefix}(?P<inst_coll>.*)-.*')
        self.name_inst_coll: Dict[str, InstanceCollection] = {}
        self.name_token_cache: TimeLimitedMaxSizeCache[str, str] = TimeLimitedMaxSizeCache(
            self.get_token_from_instance_name,
            SIXTY_SECONDS_NS,
            CACHE_CAPACITY,
            'batch-driver instance name-token cache',
        )

    def register_instance_collection(self, inst_coll: 'InstanceCollection'):
        assert inst_coll.name not in self.name_inst_coll
        self.name_inst_coll[inst_coll.name] = inst_coll

    def choose_location(
        self,
        cores: int,
        local_ssd_data_disk: bool,
        data_disk_size_gb: int,
        preemptible: bool,
        regions: List[str],
        machine_type: str,
    ) -> str:
        if machine_type.startswith('gpu_'):
            return 'lambda'

        if self._default_region in regions and self.global_live_cores_mcpu // 1000 < 1_000:
            regions = [self._default_region]
        return self.location_monitor.choose_location(
            cores, local_ssd_data_disk, data_disk_size_gb, preemptible, regions, machine_type
        )

    @property
    def pools(self) -> Dict[str, 'InstanceCollection']:
        return {k: v for k, v in self.name_inst_coll.items() if v.is_pool}

    @property
    def name_instance(self):
        result = {}
        for inst_coll in self.name_inst_coll.values():
            result.update(inst_coll.name_instance)
        return result

    @property
    def global_total_n_instances(self):
        return sum(inst_coll.all_versions_total_n_instances for inst_coll in self.name_inst_coll.values())

    @property
    def global_total_cores_mcpu(self):
        return sum(inst_coll.all_versions_total_cores_mcpu for inst_coll in self.name_inst_coll.values())

    @property
    def global_live_n_instances(self):
        return sum(inst_coll.all_versions_live_n_instances for inst_coll in self.name_inst_coll.values())

    @property
    def global_live_cores_mcpu(self):
        return sum(inst_coll.all_versions_live_cores_mcpu for inst_coll in self.name_inst_coll.values())

    @property
    def global_current_version_active_schedulable_free_cores_mcpu(self):
        return sum(
            inst_coll.current_worker_version_stats.active_schedulable_free_cores_mcpu
            for inst_coll in self.name_inst_coll.values()
        )

    @property
    def global_n_instances_by_state(self) -> Counter[str]:
        return sum(
            (inst_coll.all_versions_instances_by_state for inst_coll in self.name_inst_coll.values()),
            collections.Counter(),
        )

    @property
    def global_cores_mcpu_by_state(self) -> Counter[str]:
        return sum(
            (inst_coll.all_versions_cores_mcpu_by_state for inst_coll in self.name_inst_coll.values()),
            collections.Counter(),
        )

    @property
    def global_schedulable_n_instances(self) -> int:
        return sum(pool.current_worker_version_stats.n_instances_by_state['active'] for pool in self.pools.values())

    @property
    def global_schedulable_cores_mcpu(self) -> int:
        return sum(pool.current_worker_version_stats.cores_mcpu_by_state['active'] for pool in self.pools.values())

    @property
    def global_schedulable_free_cores_mcpu(self) -> int:
        return sum(pool.current_worker_version_stats.active_schedulable_free_cores_mcpu for pool in self.pools.values())

    def get_inst_coll(self, inst_coll_name):
        return self.name_inst_coll.get(inst_coll_name)

    def get_instance(self, inst_name) -> Optional[Instance]:
        match = re.search(self.inst_coll_regex, inst_name)
        if match:
            inst_coll_name = match.groupdict()['inst_coll']
        elif inst_name.startswith(self.machine_name_prefix):
            inst_coll_name = 'standard'
        else:
            return None

        inst_coll = self.name_inst_coll.get(inst_coll_name)
        if inst_coll:
            return inst_coll.name_instance.get(inst_name)
        return None

    async def get_token_from_instance_name(self, name):
        record: Dict[str, Any] = await self.db.select_and_fetchone(
            'SELECT token FROM instances WHERE name = %s', (name), 'active_instances_only'
        )

        assert record
        return record['token']


class InstanceCollectionStats:
    def __init__(self):
        self.n_instances_by_state = {'pending': 0, 'active': 0, 'inactive': 0, 'deleted': 0}
        self.cores_mcpu_by_state = {'pending': 0, 'active': 0, 'inactive': 0, 'deleted': 0}

        self.live_free_cores_mcpu_by_region: Dict[str, int] = collections.defaultdict(int)
        # pending and active
        self.active_schedulable_free_cores_mcpu = 0

    def remove_instance(self, instance: Instance):
        self.n_instances_by_state[instance.state] -= 1
        self.cores_mcpu_by_state[instance.state] -= instance.cores_mcpu

        if instance.state in live_instance_states:
            self.live_free_cores_mcpu_by_region[instance.region] -= instance.free_cores_mcpu_nonnegative

        if instance.state == 'active':
            self.active_schedulable_free_cores_mcpu -= instance.free_cores_mcpu_nonnegative

    def add_instance(self, instance: Instance):
        self.n_instances_by_state[instance.state] += 1
        self.cores_mcpu_by_state[instance.state] += instance.cores_mcpu

        if instance.state in live_instance_states:
            self.live_free_cores_mcpu_by_region[instance.region] += instance.free_cores_mcpu_nonnegative

        if instance.state == 'active':
            self.active_schedulable_free_cores_mcpu += instance.free_cores_mcpu_nonnegative


class InstanceCollection:
    def __init__(
        self,
        db: Database,  # BORROWED
        inst_coll_manager: InstanceCollectionManager,
        resource_manager: CloudResourceManager,
        cloud: str,
        name: str,
        machine_name_prefix: str,
        is_pool: bool,
        max_instances: int,
        max_live_instances: int,
        task_manager: aiotools.BackgroundTaskManager,  # BORROWED
    ):
        self.db = db
        self.inst_coll_manager = inst_coll_manager
        self.resource_manager = resource_manager
        self.cloud = cloud
        self.name = name
        self.machine_name_prefix = f'{machine_name_prefix}{self.name}-'
        self.is_pool = is_pool
        self.max_instances = max_instances
        self.max_live_instances = max_live_instances

        self.stats_by_instance_version: Dict[int, InstanceCollectionStats] = collections.defaultdict(
            lambda: InstanceCollectionStats()
        )

        self.name_instance: Dict[str, Instance] = {}

        self.instances_by_last_updated = sortedcontainers.SortedSet(key=lambda instance: instance.last_updated)

        task_manager.ensure_future(self.monitor_instances_loop())
        self.inst_coll_manager.register_instance_collection(self)

    @property
    def current_worker_version_stats(self) -> InstanceCollectionStats:
        return self.stats_by_instance_version[INSTANCE_VERSION]

    @property
    def all_versions_instances_by_state(self):
        return sum(
            (
                collections.Counter(version_stats.n_instances_by_state)
                for version_stats in self.stats_by_instance_version.values()
            ),
            collections.Counter(),
        )

    @property
    def all_versions_cores_mcpu_by_state(self):
        return sum(
            (
                collections.Counter(version_stats.cores_mcpu_by_state)
                for version_stats in self.stats_by_instance_version.values()
            ),
            collections.Counter(),
        )

    @property
    def all_versions_total_n_instances(self):
        return sum(
            sum(version_stats.n_instances_by_state.values())
            for version_stats in self.stats_by_instance_version.values()
        )

    @property
    def all_versions_live_n_instances(self):
        return sum(
            version_stats.n_instances_by_state[state]
            for version_stats in self.stats_by_instance_version.values()
            for state in live_instance_states
        )

    @property
    def all_versions_total_cores_mcpu(self):
        return sum(
            sum(version_stats.cores_mcpu_by_state.values()) for version_stats in self.stats_by_instance_version.values()
        )

    @property
    def all_versions_live_cores_mcpu(self):
        return sum(
            version_stats.cores_mcpu_by_state[state]
            for version_stats in self.stats_by_instance_version.values()
            for state in live_instance_states
        )

    @property
    def n_instances(self) -> int:
        return len(self.name_instance)

    def choose_location(
        self,
        cores: int,
        local_ssd_data_disk: bool,
        data_disk_size_gb: int,
        preemptible: bool,
        regions: List[str],
        machine_type: str,
    ) -> str:
        return self.inst_coll_manager.choose_location(
            cores, local_ssd_data_disk, data_disk_size_gb, preemptible, regions, machine_type
        )

    def generate_machine_name(self) -> str:
        while True:
            # 36 ** 5 = ~60M
            suffix = secret_alnum_string(5, case='lower')
            machine_name = f'{self.machine_name_prefix}{suffix}'
            if machine_name not in self.name_instance:
                break
        return machine_name

    def adjust_for_remove_instance(self, instance: Instance):
        assert instance in self.instances_by_last_updated

        self.instances_by_last_updated.remove(instance)
        self.stats_by_instance_version[instance.version].remove_instance(instance)

    async def remove_instance(self, instance: Instance, reason: str, timestamp: Optional[int] = None):
        await instance.deactivate(reason, timestamp)

        await self.db.just_execute('UPDATE instances SET removed = 1 WHERE name = %s;', (instance.name,))

        self.adjust_for_remove_instance(instance)
        del self.name_instance[instance.name]

    def adjust_for_add_instance(self, instance: Instance):
        assert instance not in self.instances_by_last_updated

        self.instances_by_last_updated.add(instance)
        self.stats_by_instance_version[instance.version].add_instance(instance)

    def add_instance(self, instance: Instance):
        assert instance.name not in self.name_instance, instance.name

        self.name_instance[instance.name] = instance
        self.adjust_for_add_instance(instance)

    async def _get_lambda_ip_address(self, instance: Instance):
        # spec = 'lambda'
        API_KEY = os.environ['LAMBDA_API_KEY']
        BASE_URL = 'https://cloud.lambdalabs.com/api/v1/'

        instance_id = instance.instance_config.instance_id
        url = f'{BASE_URL}instances/{instance_id}'
        payload = {
            "id": instance_id,
        }
        try:
            instance_info = await retry_transient_errors(
                self.resource_manager.client_session.get_read_json,
                url,
                headers={'Authorization': f'Bearer {API_KEY}'},
                json=payload,
            )
            ip = instance_info['data']['ip']
            log.info(f'LambdaVM instance {instance_id} IP address from instance info: {ip}')
            instance.ip_address = ip
            log.info(f'LambdaVM instance {instance_id} IP address from ip_address field in instance: {ip}')
            return ip
        except Exception as e:
            log.error(f'Error getting lambda IP address: {e}')
            raise e

    async def _set_up_lambda_vm(self, instance: Instance):
        log.info(f'LambdaVM {instance.name}: starting docker-run bootstrap')
        try:
            lambda_ip_addr = await self._get_lambda_ip_address(instance)
            await instance.activate(lambda_ip_addr, time_msecs())
            await instance.transfer_credentials_and_configure_docker()
            # Use cached values set at create time; fall back to safe defaults for driver restarts.
            max_idle_time_msecs = getattr(instance, '_lambda_max_idle_time_msecs', 300_000)
            unreserved_disk_size_gb = getattr(instance, '_lambda_unreserved_disk_size_gb', 10)
            await instance.run_worker_container(
                file_store=self.app['file_store'],
                max_idle_time_msecs=max_idle_time_msecs,
                unreserved_disk_size_gb=unreserved_disk_size_gb,
            )
            instance._lambda_setup_completed = True
            log.info(f'LambdaVM {instance.name} docker-run bootstrap completed')
        except Exception as e:
            log.error(f'LambdaVM {instance.name} docker-run bootstrap failed: {e}')
            raise e

    async def _create_instance(
        self,
        app,
        cores: int,
        machine_type: str,
        job_private: bool,
        regions: List[str],
        preemptible: bool,
        max_idle_time_msecs: int,
        local_ssd_data_disk,
        data_disk_size_gb,
        boot_disk_size_gb,
    ) -> Tuple[Instance, List[QuantifiedResource]]:
        from ...cloud.resource_utils import unreserved_worker_data_disk_size_gib

        location = self.choose_location(
            cores, local_ssd_data_disk, data_disk_size_gb, preemptible, regions, machine_type
        )

        machine_name = self.generate_machine_name()
        activation_token = secrets.token_urlsafe(32)

        instance_config = self.resource_manager.instance_config(
            machine_type=machine_type,
            preemptible=preemptible,
            local_ssd_data_disk=local_ssd_data_disk,
            data_disk_size_gb=data_disk_size_gb,
            boot_disk_size_gb=boot_disk_size_gb,
            job_private=job_private,
            location=location,
        )
        instance = await Instance.create(
            app=app,
            inst_coll=self,
            name=machine_name,
            activation_token=activation_token,
            cores=cores,
            location=location,
            machine_type=machine_type,
            preemptible=preemptible,
            instance_config=instance_config,
        )
        self.add_instance(instance)
        # Cache Lambda bootstrap params on the instance for use in _set_up_lambda_vm.
        # These are in-memory only and not persisted; a driver restart will re-use defaults.
        if self.cloud == 'lambda':
            instance._lambda_max_idle_time_msecs = max_idle_time_msecs
            instance._lambda_unreserved_disk_size_gb = unreserved_worker_data_disk_size_gib(data_disk_size_gb, cores)
        total_resources_on_instance = await self.resource_manager.create_vm(
            file_store=app['file_store'],
            machine_name=machine_name,
            activation_token=activation_token,
            max_idle_time_msecs=max_idle_time_msecs,
            local_ssd_data_disk=local_ssd_data_disk,
            data_disk_size_gb=data_disk_size_gb,
            boot_disk_size_gb=boot_disk_size_gb,
            preemptible=preemptible,
            job_private=job_private,
            location=location,
            machine_type=machine_type,
            instance_config=instance_config,
        )

        return (instance, total_resources_on_instance)

    async def call_delete_instance(
        self, instance: Instance, reason: str, timestamp: Optional[int] = None, force: bool = False
    ):
        if instance.state == 'deleted' and not force:
            return
        if instance.state not in ('inactive', 'deleted'):
            await instance.deactivate(reason, timestamp)

        try:
            await self.resource_manager.delete_vm(instance)
        except VMDoesNotExist:
            log.info(f'{instance} delete already done')
            await self.remove_instance(instance, reason, timestamp)

    async def check_on_instance(self, instance: Instance, debug=False):
        active_and_healthy = await instance.check_is_active_and_healthy()

        if instance.state == 'active' and instance.failed_request_count > 5:
            log.exception(
                f'deleting {instance} with {instance.failed_request_count} failed request counts after more than 5 minutes'
            )
            await self.call_delete_instance(instance, 'not_responding')
            return

        if active_and_healthy:
            return

        try:
            vm_state = await self.resource_manager.get_vm_state(instance)
        except VMDoesNotExist:
            await self.remove_instance(instance, 'does_not_exist')
            return

        # Check for Lambda Labs VM becoming active for the first time
        lambda_setup_completed = getattr(instance, '_lambda_setup_completed', False)

        if instance.inst_coll.cloud == 'lambda' and isinstance(vm_state, VMStateRunning) and not lambda_setup_completed:
            log.info(f'LambdaVM {instance.name} is now active - running one-time setup')
            try:
                await self._set_up_lambda_vm(instance)
                log.info(f'LambdaVM {instance.name} setup completed successfully')
            except Exception as e:
                log.error(f'LambdaVM {instance.name} setup failed: {e}')
                # Don't re-raise - let VM continue normal lifecycle

        # Cases are mutually exclusive and therefore order-independent
        if instance.state == 'pending' and isinstance(vm_state, (VMStateCreating, VMStateRunning)):
            if instance.inst_coll.cloud == 'lambda':
                if vm_state.time_since_last_state_change() > 15 * 60 * 1000:
                    log.exception(f'{instance} (state: {vm_state}) has made no progress in last 15m, deleting')
                    await self.call_delete_instance(instance, 'activation_timeout')
            elif vm_state.time_since_last_state_change() > 5 * 60 * 1000:
                log.exception(f'{instance} (state: {vm_state}) has made no progress in last 5m, deleting')
                await self.call_delete_instance(instance, 'activation_timeout')
        elif instance.state in ('pending', 'active') and isinstance(vm_state, VMStateTerminated):
            log.info(f'{instance} live but stopping or terminated, deactivating')
            await instance.deactivate('terminated')
        elif instance.state == 'inactive':
            log.info(f'{instance} (vm_state: {vm_state}) is inactive, deleting')
            await self.call_delete_instance(instance, 'inactive')
        elif instance.state == 'deleted' and not isinstance(vm_state, VMStateTerminated):
            log.exception('Instance state is deleted when cloud state is not terminated')
        else:
            log.info(f'Other instance state for {instance} vm_state {vm_state}')
            assert (
                (instance.state == 'active' and isinstance(vm_state, (VMStateCreating, VMStateRunning)))
                or (instance.state == 'deleted' and isinstance(vm_state, VMStateTerminated))
                or isinstance(vm_state, UnknownVMState)
            )

        await instance.update_timestamp()

    async def monitor_instances(self):
        if self.instances_by_last_updated:
            # [:50] are the fifty smallest (oldest)
            instances = self.instances_by_last_updated[:50]

            async def check(instance):
                since_last_updated = time_msecs() - instance.last_updated
                if since_last_updated > 60 * 1000:
                    await self.check_on_instance(instance)

            await asyncio.gather(*[check(instance) for instance in instances])

    async def monitor_instances_loop(self):
        await periodically_call(1, self.monitor_instances)
