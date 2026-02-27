import asyncio
import collections
import logging
import os
import re
import secrets
from datetime import datetime
from typing import Any, Counter, Dict, List, Optional, Tuple

import paramiko
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

    async def _lambda_setup_logging(self, instance: Instance, setup_succeeded: bool = True):
        """
        Capture diagnostic information to Lambda filesystem for debugging.

        This runs after worker startup attempt and writes diagnostic files to
        the Lambda filesystem which persists after VM deletion.

        Args:
            instance: The Lambda instance
            setup_succeeded: True if called after successful setup, False if from exception handler
        """
        if self.cloud != 'lambda':
            return

        try:
            log.info(f'Capturing diagnostic logs for Lambda VM {instance.name}')

            region = instance.instance_config.region_for(instance.location)
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            log_dir = f'/home/ubuntu/lambda-fs-{region}/diagnostics/{timestamp}/{instance.name}'

            # Setup: Create diagnostic directory and write run info
            setup_commands = [
                f'mkdir -p {log_dir}',
                f'echo "{timestamp} - Setup {"succeeded" if setup_succeeded else "failed"}" > {log_dir}/diagnostic_run_info.txt',
                f'echo "Instance: {instance.name}" >> {log_dir}/diagnostic_run_info.txt',
                f'echo "IP: {instance.ip_address}" >> {log_dir}/diagnostic_run_info.txt',
            ]

            # Priority 1: Worker process state
            worker_process_commands = [
                f'ps aux | grep -E "batch.worker|python3" > {log_dir}/worker_processes.txt 2>&1 || echo "ps command failed" > {log_dir}/worker_processes.txt',
                f'pgrep -fa batch.worker.worker > {log_dir}/worker_pid_details.txt 2>&1 || echo "No worker process found" > {log_dir}/worker_pid_details.txt',
            ]

            # Priority 2: Worker logs (most important)
            worker_log_commands = [
                f'cp /home/ubuntu/worker.log {log_dir}/worker.log 2>&1 || echo "Worker log not found" > {log_dir}/worker.log',
                f'tail -200 /home/ubuntu/worker.log > {log_dir}/worker_log_tail.txt 2>&1 || echo "Worker log not found" > {log_dir}/worker_log_tail.txt',
                f'grep -i "error\\|exception\\|failed\\|unauthorized" /home/ubuntu/worker.log > {log_dir}/worker_errors.txt 2>&1 || echo "No errors found or log missing" > {log_dir}/worker_errors.txt',
            ]

            # Priority 3: Network/Port status
            network_commands = [
                f'netstat -tuln | grep 5000 > {log_dir}/port_5000_status.txt 2>&1 || ss -tuln | grep 5000 > {log_dir}/port_5000_status.txt 2>&1 || echo "No listener on port 5000" > {log_dir}/port_5000_status.txt',
                f'netstat -tuln > {log_dir}/all_listening_ports.txt 2>&1 || ss -tuln > {log_dir}/all_listening_ports.txt 2>&1',
            ]

            # Priority 4: Mount status
            mount_commands = [
                f'mount | grep /host/rootfs/86038d3483f6 > {log_dir}/chroot_mounts.txt 2>&1 || echo "No chroot mounts found" > {log_dir}/chroot_mounts.txt',
            ]

            # Mount verification (individual checks)
            mount_check_script = """
for mount_point in dev proc sys tmp run; do
    if mount | grep -q "/host/rootfs/86038d3483f6/$mount_point"; then
        echo "$mount_point: mounted"
    else
        echo "$mount_point: NOT mounted"
    fi
done
if mount | grep -q "/host/rootfs/86038d3483f6/etc/resolv.conf"; then
    echo "resolv.conf: mounted"
else
    echo "resolv.conf: NOT mounted"
fi
"""
            mount_verification_cmd = f'bash -c \'{mount_check_script}\' > {log_dir}/mount_verification.txt 2>&1'

            # Priority 5: Chroot environment validation
            chroot_commands = [
                f'chroot /host/rootfs/86038d3483f6 /bin/bash -c "echo Chroot execution works" > {log_dir}/chroot_test.txt 2>&1 || echo "Chroot execution failed" > {log_dir}/chroot_test.txt',
                f'chroot /host/rootfs/86038d3483f6 /opt/venv/bin/python3 --version > {log_dir}/chroot_python_version.txt 2>&1 || echo "Python not available in chroot" > {log_dir}/chroot_python_version.txt',
                f'chroot /host/rootfs/86038d3483f6 /opt/venv/bin/python3 -c "import batch.worker.worker; print(\'Module found\')" > {log_dir}/chroot_worker_module.txt 2>&1 || echo "Worker module not found or import failed" > {log_dir}/chroot_worker_module.txt',
            ]

            # Priority 6: System state
            system_commands = [
                f'uname -a > {log_dir}/system_info.txt 2>&1',
                f'uptime > {log_dir}/uptime.txt 2>&1',
                f'df -h > {log_dir}/disk_space.txt 2>&1',
                f'free -h > {log_dir}/memory_status.txt 2>&1',
                f'docker info > {log_dir}/docker_info.txt 2>&1 || echo "Docker not available or not running" > {log_dir}/docker_info.txt',
            ]

            # Combine all commands
            all_commands = (
                setup_commands
                + worker_process_commands
                + worker_log_commands
                + network_commands
                + mount_commands
                + [mount_verification_cmd]
                + chroot_commands
                + system_commands
            )

            # Execute via SSH
            with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
                private_key = paramiko.RSAKey.from_private_key(key_file)

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            try:
                ssh.connect(hostname=instance.ip_address, username='ubuntu', pkey=private_key)

                # Execute all diagnostic commands
                for cmd in all_commands:
                    stdin, stdout, stderr = ssh.exec_command(cmd)
                    stdout.channel.recv_exit_status()  # Wait for completion

                log.info(f'Diagnostic logs captured to {log_dir} on Lambda filesystem')

            finally:
                ssh.close()

        except Exception as e:
            # Don't fail setup if logging fails
            log.warning(f'Failed to capture diagnostic logs for {instance.name}: {e}')

    async def _set_up_lambda_vm(self, instance: Instance):
        """
        Placeholder function called once when a Lambda Labs VM first becomes active.
        This is where Lambda-specific setup logic will be implemented.
        """
        log.info(f'Hello world - LambdaVM {instance.name} setup placeholder called!')
        try:
            before_setup = getattr(instance, '_lambda_setup_completed', False)
            log.info(f'LambdaVM {instance.name} _lambda_setup_completed before setup: {before_setup}')

            lambda_ip_addr = await self._get_lambda_ip_address(instance)

            await instance.activate(lambda_ip_addr, time_msecs())
            await instance.mount_squashfs()
            await instance.prepare_chroot_environment()
            await instance.start_batch_worker()

            instance._lambda_setup_completed = True
            await self._lambda_setup_logging(instance, setup_succeeded=True)
            log.info(f'LambdaVM {instance.name} setup completed: {instance._lambda_setup_completed}')
        except Exception as e:
            log.error(f'Error executing startup script for Lambda VM {instance.name}: {e}')
            raise e

        # TODO: Add Lambda Labs VM setup logic here

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
        # Compute setup context for Lambda instances
        batch_logs_storage_uri = ''
        batch_instance_id = 'test-lambda-instance'
        max_idle_time_msecs_param = max_idle_time_msecs  # Use the parameter value, defaults should be 300000
        unreserved_disk_size_gb_param = (
            5000  # NOTE: THIS IS A LARGE NUMBER FOR TESTING, DO NOT LET THIS SLIDE IN PRODUCTION.
        )

        if self.cloud == 'lambda':
            # from ...cloud.resource_utils import unreserved_worker_data_disk_size_gib

            batch_logs_storage_uri = app['file_store'].batch_logs_storage_uri
            batch_instance_id = app['file_store'].instance_id
            # unreserved_disk_size_gb_param = unreserved_worker_data_disk_size_gib(data_disk_size_gb, cores)

        location = self.choose_location(
            cores, local_ssd_data_disk, data_disk_size_gb, preemptible, regions, machine_type
        )

        machine_name = self.generate_machine_name()
        activation_token = secrets.token_urlsafe(32)

        if self.cloud == 'lambda':
            instance_config = self.resource_manager.instance_config(
                machine_type=machine_type,
                preemptible=preemptible,
                local_ssd_data_disk=local_ssd_data_disk,
                data_disk_size_gb=data_disk_size_gb,
                boot_disk_size_gb=boot_disk_size_gb,
                job_private=job_private,
                location=location,
                # Setup context values (only populated for Lambda)
                batch_logs_storage_uri=batch_logs_storage_uri,
                batch_instance_id=batch_instance_id,
                max_idle_time_msecs=max_idle_time_msecs_param,
                unreserved_disk_size_gb=unreserved_disk_size_gb_param,
            )
        else:
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

        # DEBUG: Always log for Lambda Labs VMs to see what's happening
        if instance.inst_coll.cloud == 'lambda':
            log.info(
                f'LAMBDA DEBUG: {instance.name} - cloud={instance.inst_coll.cloud}, vm_state={type(vm_state).__name__}, setup_completed={lambda_setup_completed}'
            )

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
            if debug:
                # DEBUG: Log all instance fields for Lambda Labs debugging
                log.error(f'DEBUG INSTANCE FIELDS for LambdaVM {instance.name}:')
                log.error(f'LambdaVM {instance.name} - state: {instance.state}')
                log.error(f'LambdaVM {instance.name} - cores_mcpu: {instance.cores_mcpu}')
                log.error(f'LambdaVM {instance.name} - free_cores_mcpu: {instance._free_cores_mcpu}')
                log.error(f'LambdaVM {instance.name} - time_created: {instance.time_created}')
                log.error(f'LambdaVM {instance.name} - failed_request_count: {instance._failed_request_count}')
                log.error(f'LambdaVM {instance.name} - last_updated: {instance._last_updated}')
                log.error(f'LambdaVM {instance.name} - ip_address: {instance.ip_address}')
                log.error(f'LambdaVM {instance.name} - version: {instance.version}')
                log.error(f'LambdaVM {instance.name} - location: {instance.location}')
                log.error(f'LambdaVM {instance.name} - machine_type: {instance.machine_type}')
                log.error(f'LambdaVM {instance.name} - preemptible: {instance.preemptible}')
                log.error(f'LambdaVM {instance.name} - instance_config: {instance.instance_config}')
                log.error(f'LambdaVM {instance.name} - instance_config.to_dict(): {instance.instance_config.to_dict()}')
                log.error(f'LambdaVM {instance.name} - inst_coll: {instance.inst_coll}')
                log.error(f'LambdaVM {instance.name} - inst_coll.name: {instance.inst_coll.name}')
                log.error(f'LambdaVM {instance.name} - inst_coll.cloud: {instance.inst_coll.cloud}')
                log.error(
                    f'LambdaVM {instance.name} - inst_coll.machine_name_prefix: {instance.inst_coll.machine_name_prefix}'
                )
                log.error(f'LambdaVM {instance.name} - inst_coll.is_pool: {instance.inst_coll.is_pool}')
                log.error(f'LambdaVM {instance.name} - vm_state: {vm_state}')
                log.error(f'LambdaVM {instance.name} - vm_state.spec: {vm_state.spec}')
                log.error(
                    f'LambdaVM {instance.name} - vm_state.time_since_last_state_change(): {vm_state.time_since_last_state_change()}'
                )
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
