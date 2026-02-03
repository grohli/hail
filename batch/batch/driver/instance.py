import base64
import json
import logging
import secrets
import shlex
from typing import Dict, Optional

import aiohttp
import paramiko

from gear import CommonAiohttpAppKeys, Database, transaction
from hailtop.humanizex import naturaldelta_msec
from hailtop.utils import retry_transient_errors, time_msecs, time_msecs_str

from ..cloud.utils import instance_config_from_config_dict
from ..globals import INSTANCE_VERSION
from ..instance_config import InstanceConfig

log = logging.getLogger('instance')

LAMBDA_SQUASHFS_IMAGE_ID = '86038d3483f6'
LAMBDA_CHROOT_PATH = f'/host/rootfs/{LAMBDA_SQUASHFS_IMAGE_ID}'


class Instance:
    @staticmethod
    def from_record(app, inst_coll, record):
        return Instance(
            app,
            inst_coll,
            record['name'],
            record['state'],
            record['cores_mcpu'],
            record['free_cores_mcpu'],
            record['time_created'],
            record['failed_request_count'],
            record['last_updated'],
            record['ip_address'],
            record['version'],
            record['location'],
            record['machine_type'],
            record['preemptible'],
            instance_config_from_config_dict(json.loads(base64.b64decode(record['instance_config']).decode())),
            activation_token=None,
        )

    @staticmethod
    async def create(
        app,
        inst_coll,
        name: str,
        activation_token,
        cores: int,
        location: str,
        machine_type: str,
        preemptible: bool,
        instance_config: InstanceConfig,
    ) -> 'Instance':
        db: Database = app['db']

        state = 'pending'
        now = time_msecs()
        token = secrets.token_urlsafe(32)

        worker_cores_mcpu = cores * 1000

        @transaction(db)
        async def insert(tx):
            await tx.just_execute(
                """
INSERT INTO instances (name, state, activation_token, token, cores_mcpu,
  time_created, last_updated, version, location, inst_coll, machine_type, preemptible, instance_config)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""",
                (
                    name,
                    state,
                    activation_token,
                    token,
                    worker_cores_mcpu,
                    now,
                    now,
                    INSTANCE_VERSION,
                    location,
                    inst_coll.name,
                    machine_type,
                    preemptible,
                    base64.b64encode(json.dumps(instance_config.to_dict()).encode()).decode(),
                ),
            )
            await tx.just_execute(
                """
INSERT INTO instances_free_cores_mcpu (name, free_cores_mcpu)
VALUES (%s, %s);
""",
                (
                    name,
                    worker_cores_mcpu,
                ),
            )

        await insert()

        return Instance(
            app,
            inst_coll,
            name,
            state,
            worker_cores_mcpu,
            worker_cores_mcpu,
            now,
            0,
            now,
            None,
            INSTANCE_VERSION,
            location,
            machine_type,
            preemptible,
            instance_config,
        )

    def __init__(
        self,
        app,
        inst_coll,
        name,
        state,
        cores_mcpu,
        free_cores_mcpu,
        time_created,
        failed_request_count,
        last_updated: int,
        ip_address,
        version,
        location: str,
        machine_type: str,
        preemptible: bool,
        instance_config: InstanceConfig,
        activation_token: Optional[str] = None,
    ):
        self.db: Database = app['db']
        self.client_session = app[CommonAiohttpAppKeys.CLIENT_SESSION]
        self.inst_coll = inst_coll
        # pending, active, inactive, deleted
        self._state = state
        self.name = name
        self.cores_mcpu = cores_mcpu
        self._free_cores_mcpu = free_cores_mcpu
        self.time_created = time_created
        self._failed_request_count = failed_request_count
        self._last_updated = last_updated
        self.ip_address = ip_address
        self.version = version
        self.location = location
        self.machine_type = machine_type
        self.preemptible = preemptible
        self.instance_config = instance_config
        self._activation_token = activation_token

    @property
    def state(self):
        return self._state

    async def activate(self, ip_address, timestamp):
        assert self._state == 'pending'

        rv = await self.db.check_call_procedure(
            'CALL activate_instance(%s, %s, %s);', (self.name, ip_address, timestamp), 'activate_instance'
        )

        self.inst_coll.adjust_for_remove_instance(self)
        self._state = 'active'
        self.ip_address = ip_address
        self.inst_coll.adjust_for_add_instance(self)
        self.inst_coll.scheduler_state_changed.set()

        return rv['token']

    async def deactivate(self, reason: str, timestamp: Optional[int] = None):
        if self._state in ('inactive', 'deleted'):
            return

        if not timestamp:
            timestamp = time_msecs()

        rv = await self.db.execute_and_fetchone(
            'CALL deactivate_instance(%s, %s, %s);', (self.name, reason, timestamp), 'deactivate_instance'
        )

        if rv['rc'] == 1:
            log.info(f'{self} with in-memory state {self._state} was already deactivated; {rv}')
            assert rv['cur_state'] in ('inactive', 'deleted')

        self.inst_coll.adjust_for_remove_instance(self)
        self._state = 'inactive'
        self._free_cores_mcpu = self.cores_mcpu
        self.inst_coll.adjust_for_add_instance(self)

        # there might be jobs to reschedule
        self.inst_coll.scheduler_state_changed.set()

    async def kill(self):
        async def make_request():
            if self._state in ('inactive', 'deleted'):
                return
            try:
                await self.client_session.post(
                    f'http://{self.ip_address}:5000/api/v1alpha/kill', timeout=aiohttp.ClientTimeout(total=30)
                )
            except aiohttp.ClientResponseError as err:
                if err.status == 403:
                    log.info(f'cannot kill {self} -- does not exist at {self.ip_address}')
                    return
                raise

        await retry_transient_errors(make_request)

    async def mark_deleted(self, reason, timestamp):
        if self._state == 'deleted':
            return
        if self._state != 'inactive':
            await self.deactivate(reason, timestamp)

        rv = await self.db.execute_and_fetchone('CALL mark_instance_deleted(%s);', (self.name,))

        if rv['rc'] == 1:
            log.info(f'{self} with in-memory state {self._state} could not be marked deleted; {rv}')
            assert rv['cur_state'] == 'deleted'

        self.inst_coll.adjust_for_remove_instance(self)
        self._state = 'deleted'
        self.inst_coll.adjust_for_add_instance(self)

    @property
    def free_cores_mcpu(self):
        """A possibly negative measure of the free cores in millicpu.

        See free_cores_mcpu_nonnegative for a more useful property.
        """
        return self._free_cores_mcpu

    @property
    def free_cores_mcpu_nonnegative(self):
        """A nonnegative measure of the free cores in millicpu.

        free_cores_mcpu can be negative temporarily if the worker is oversubscribed.
        """
        return max(0, self.free_cores_mcpu)

    @property
    def used_cores_mcpu_nonnegative(self):
        """A nonnegative measure of the used cores in millicpu.

        The free_cores_mcpu can be negative temporarily if the worker is oversubscribed, so this
        property uses free_cores_mcpu_nonnegative to calculate used cores.

        """
        return self.cores_mcpu - self.free_cores_mcpu_nonnegative

    @property
    def percent_cores_used(self) -> float:
        """The percent of cores currently in use."""
        return self.used_cores_mcpu_nonnegative / self.cores_mcpu

    def cost_per_hour(self, resource_rates: Dict[str, float]) -> float:
        """
        The charges incurred from the cloud for this instance, ignoring attached disks.
        """
        return self.instance_config.cost_per_hour(resource_rates)

    def revenue_per_hour(self, resource_rates: Dict[str, float]) -> float:
        """
        The revenue generated by in-use resources on this instance.
        """
        return self.instance_config.entire_instance_price_per_hour(resource_rates) * self.percent_cores_used

    def adjust_free_cores_in_memory(self, delta_mcpu):
        self.inst_coll.adjust_for_remove_instance(self)
        self._free_cores_mcpu += delta_mcpu
        self.inst_coll.adjust_for_add_instance(self)

    async def mount_squashfs(self):
        """
        Mount the squashfs on the Lambda VM.

        This method must be called AFTER activate() and BEFORE prepare_chroot_environment().
        Due to the startup times of Lambda VMs and the lack of startup scripts and custom images,
        we need to mount the squashfs manually and after the VM is activated.
        """
        log.info(f'LambdaVM {self.name}: IP address: {self.ip_address}')
        with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
            private_key = paramiko.RSAKey.from_private_key(key_file)
        squashfs_name = 'batch-worker-lambda.squashfs'
        region = self.instance_config.region_for(self.location)
        squashfs_path = f'/home/ubuntu/lambda-fs-{region}/{squashfs_name}'
        # 86038d3483f6: this is the id of the Docker Image ID I used to create the squashfs.
        # TODO: Set BATCH_WORKER_IMAGE_ID in the instance config to this value.
        # Note for future functionality: We need to replace the "worker image" manually in LL
        # since this is outside of GCP. When we do this, we can also pass the image id to the
        # batch worker container so that it knows which squashfs file to use.
        mount_path = '/host/rootfs/86038d3483f6'
        mount_cmd = f'sudo mount {squashfs_path} {mount_path} -t squashfs -o loop'
        commands = [
            f'sudo mkdir -p {mount_path}',
            mount_cmd,
            f'sudo ls -la {mount_path}',
        ]

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=self.ip_address, username='ubuntu', pkey=private_key)
        for cmd in commands:
            stdin, stdout, stderr = ssh.exec_command(cmd)
            print(stdout.read().decode())
            print(stderr.read().decode())
        ssh.close()

    """
    REGARDING THE FOLLOWING LAMBDA-SPECIFIC FUNCTIONS:
    - prepare_chroot_environment()
    - start_batch_worker()
    - _build_worker_env_vars()
    Please refer to the worker startup script passed to GCP workers in create_instance.py.
    The commands executed/constructed in these functions are adapted from the GCP worker startup script.
    Since Lambda Labs does not support startup scripts, we must manually call the following functions.
    If there is an issue with the commands executed/constructed in these functions, please refer to the GCP worker startup script in create_instance.py.
    """

    async def prepare_chroot_environment(self):
        """
        Prepare the chroot environment by setting up necessary bind mounts.

        This method must be called AFTER mount_squashfs() and BEFORE start_batch_worker().

        The squashfs contains a full directory structure identical to what exists
        after 'docker export' on a standard GCP worker. We bind mount essential
        system directories to make the chroot functional.
        """
        if self.inst_coll.cloud != 'lambda':
            raise ValueError('prepare_chroot_environment() only valid for lambda instances')

        log.info(f'Preparing chroot environment for Lambda VM {self.name}')

        chroot_path = LAMBDA_CHROOT_PATH

        # Bind mounts required for the chroot to function
        bind_mount_commands = [
            # Special filesystems
            f'sudo mount --bind /dev {chroot_path}/dev',
            f'sudo mount -t proc proc {chroot_path}/proc',
            f'sudo mount -t sysfs sys {chroot_path}/sys',
            f'sudo mount -t tmpfs tmpfs {chroot_path}/tmp',
            # Docker socket (via /run) - needed for job container management
            f'sudo mount --bind /var/run {chroot_path}/run',
            # DNS resolution
            f'sudo mount --bind /etc/resolv.conf {chroot_path}/etc/resolv.conf',
        ]

        with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
            private_key = paramiko.RSAKey.from_private_key(key_file)

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh.connect(hostname=self.ip_address, username='ubuntu', pkey=private_key)

            for cmd in bind_mount_commands:
                log.info(f'Chroot environment mounting: Executing {cmd}...')
                stdin, stdout, stderr = ssh.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    error_output = stderr.read().decode()
                    log.error(f'Command failed with exit status {exit_status}: {error_output}')
                    raise RuntimeError(f'Failed to execute: {cmd}')

            log.info(f'LAMBDA DEBUG: Chroot environment prepared successfully for {self.name}')

        except Exception as e:
            log.error(f'LAMBDA DEBUG: Error preparing chroot environment for {self.name}: {e}')
            raise e

        finally:
            ssh.close()

    async def start_batch_worker(self):
        """
        Start the batch worker process on a Lambda Labs VM via SSH.

        The worker runs inside a chroot environment using the mounted squashfs.
        Environment variables are exported inline with the chroot command.
        """
        if self.inst_coll.cloud != 'lambda':
            raise ValueError('start_batch_worker() only valid for lambda instances')

        log.info(f'Starting batch worker on Lambda VM {self.name}')

        # Gather environment variables
        env_vars = await self._build_worker_env_vars()

        # Build the chroot command with inline exports
        chroot_command = self._build_chroot_command(env_vars)

        await self._execute_worker_start(chroot_command)

    async def _build_worker_env_vars(self) -> Dict[str, str]:
        """Gather all environment variables needed by the worker."""
        import os

        from ..batch_configuration import DEFAULT_NAMESPACE, DOCKER_PREFIX, DOCKER_ROOT_IMAGE, INTERNAL_GATEWAY_IP
        from ..cloud.utils import ACCEPTABLE_QUERY_JAR_URL_PREFIX

        # Get activation_token - should be cached on instance
        activation_token = self._activation_token
        if not activation_token:
            # Fallback: query from database if not cached
            record = await self.db.select_and_fetchone(
                'SELECT activation_token FROM instances WHERE name = %s', (self.name,)
            )
            activation_token = record['activation_token']

        # Get values from instance_config (populated at creation)
        ic = self.instance_config

        return {
            'CLOUD': 'lambda',
            'CORES': str(ic.cores),
            'NAME': self.name,
            'NAMESPACE': DEFAULT_NAMESPACE,
            'ACTIVATION_TOKEN': activation_token,
            'IP_ADDRESS': self.ip_address,
            'BATCH_LOGS_STORAGE_URI': getattr(ic, 'batch_logs_storage_uri', ''),
            'INSTANCE_ID': getattr(ic, 'batch_instance_id', 'test-lambda-instance'),
            'REGION': self.region,
            'DOCKER_PREFIX': DOCKER_PREFIX,
            'DOCKER_ROOT_IMAGE': DOCKER_ROOT_IMAGE,
            'INSTANCE_CONFIG': base64.b64encode(json.dumps(ic.to_dict()).encode()).decode(),
            'MAX_IDLE_TIME_MSECS': str(getattr(ic, 'max_idle_time_msecs', 300000)),
            'BATCH_WORKER_IMAGE': os.environ.get('HAIL_BATCH_WORKER_IMAGE', ''),
            'BATCH_WORKER_IMAGE_ID': LAMBDA_SQUASHFS_IMAGE_ID,
            'UNRESERVED_WORKER_DATA_DISK_SIZE_GB': str(getattr(ic, 'unreserved_disk_size_gb', 10)),
            'ACCEPTABLE_QUERY_JAR_URL_PREFIX': ACCEPTABLE_QUERY_JAR_URL_PREFIX,
            'INTERNAL_GATEWAY_IP': INTERNAL_GATEWAY_IP,
        }

    def _build_chroot_command(self, env_vars: Dict[str, str]) -> str:
        """
        Build the chroot command with inline environment variable exports.

        The command structure is:
            sudo chroot /host/rootfs/{IMAGE_ID} /bin/bash -c '
                export VAR1=value1
                export VAR2=value2
                ...
                export INTERNET_INTERFACE=$(...)
                /opt/venv/bin/python3 -u -m batch.worker.worker
            '
        """
        chroot_path = LAMBDA_CHROOT_PATH

        # Build export statements
        export_lines = []
        for key, value in env_vars.items():
            # Use shlex.quote to safely escape values
            export_lines.append(f'export {key}={shlex.quote(value)}')

        # INTERNET_INTERFACE must be computed inside the chroot
        export_lines.append(
            "export INTERNET_INTERFACE=$(ip link list | grep -E 'en[sop]|eth' | head -1 | awk -F': ' '{print $2}')"
        )

        # Build the inner script
        worker_process_commands = [
            '',
            '# Start the worker process',
            'cd /batch 2>/dev/null || cd /',
            'exec /opt/venv/bin/python3 -u -m batch.worker.worker',
        ]
        inner_script_lines = [*export_lines, *worker_process_commands]
        inner_script = '\n'.join(inner_script_lines)

        # Build the full chroot command
        # Using nohup and background to detach from SSH session
        chroot_cmd = f"sudo chroot {chroot_path} /bin/bash -c '{inner_script}'"

        # Wrap in nohup for background execution with logging
        full_command = f"nohup {chroot_cmd} > /home/ubuntu/worker.log 2>&1 &"

        return full_command

    async def _execute_worker_start(self, command: str):
        """Execute the worker start command via SSH."""
        log.info(f'Executing worker start on Lambda VM {self.name} at {self.ip_address}')
        log.debug(f'Command: {command}')

        with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
            private_key = paramiko.RSAKey.from_private_key(key_file)

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh.connect(hostname=self.ip_address, username='ubuntu', pkey=private_key)

            stdin, stdout, stderr = ssh.exec_command(command)

            # Don't wait for completion since it's backgrounded
            # Small delay to let the process start
            import asyncio

            await asyncio.sleep(2)

            # Verify the process started
            stdin, stdout, stderr = ssh.exec_command('pgrep -f "batch.worker.worker" || echo "NOT_RUNNING"')
            output = stdout.read().decode().strip()

            if output == 'NOT_RUNNING':
                # Check the log for errors
                stdin, stdout, stderr = ssh.exec_command('tail -50 /home/ubuntu/worker.log')
                log_output = stdout.read().decode()
                log.error(f'LAMBDA DEBUG: Worker process failed to start. Log output:\n{log_output}')
                raise RuntimeError(f'Worker process failed to start on {self.name}')

            log.info(f'Worker process started successfully on {self.name} with PID(s): {output}')

        finally:
            ssh.close()

    @property
    def failed_request_count(self):
        return self._failed_request_count

    async def check_is_active_and_healthy(self):
        if self._state == 'active' and self.ip_address:
            try:
                async with self.client_session.get(f'http://{self.ip_address}:5000/healthcheck') as resp:
                    actual_name = (await resp.json()).get('name')
                    if actual_name and actual_name != self.name:
                        return False
                await self.mark_healthy()
                return True
            except Exception:
                if (time_msecs() - self.last_updated) / 1000 > 300:
                    log.exception(f'while requesting {self} /healthcheck')
                await self.incr_failed_request_count()
        return False

    async def mark_healthy(self):
        if self._state != 'active':
            return

        now = time_msecs()
        changed = (self._failed_request_count > 1) or (now - self._last_updated) > 5000
        if not changed:
            return

        self.inst_coll.adjust_for_remove_instance(self)
        self._failed_request_count = 0
        self._last_updated = now
        self.inst_coll.adjust_for_add_instance(self)

        await self.db.execute_update(
            """
UPDATE instances
SET last_updated = %s,
  failed_request_count = 0
WHERE name = %s;
""",
            (now, self.name),
            'mark_healthy',
        )

    async def incr_failed_request_count(self):
        await self.db.execute_update(
            """
UPDATE instances
SET failed_request_count = failed_request_count + 1 WHERE name = %s;
""",
            (self.name,),
        )

        self.inst_coll.adjust_for_remove_instance(self)
        self._failed_request_count += 1
        self.inst_coll.adjust_for_add_instance(self)

    @property
    def last_updated(self):
        return self._last_updated

    async def update_timestamp(self):
        now = time_msecs()
        await self.db.execute_update('UPDATE instances SET last_updated = %s WHERE name = %s;', (now, self.name))

        self.inst_coll.adjust_for_remove_instance(self)
        self._last_updated = now
        self.inst_coll.adjust_for_add_instance(self)

    def time_created_str(self):
        return time_msecs_str(self.time_created)

    def last_updated_str(self):
        return naturaldelta_msec(time_msecs() - self.last_updated)

    @property
    def region(self):
        return self.instance_config.region_for(self.location)

    def __str__(self):
        return f'instance {self.name}'
