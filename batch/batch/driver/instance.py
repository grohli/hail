import asyncio
import base64
import json
import logging
import os
import secrets
import shlex
from typing import Dict, Optional

import aiohttp
import paramiko

from gear import CommonAiohttpAppKeys, Database, transaction
from gear.cloud_config import get_global_config
from hailtop.config import get_deploy_config
from hailtop.humanizex import naturaldelta_msec
from hailtop.utils import retry_transient_errors, time_msecs, time_msecs_str

from ..cloud.utils import instance_config_from_config_dict
from ..globals import INSTANCE_VERSION
from ..instance_config import InstanceConfig

log = logging.getLogger('instance')


# Path the batch-driver pod mounts the `hail-internal-token` secret at. The secret
# holds a `tokens.json` whose `"default"` entry is a Hail session id for a developer
# user; the Lambda worker uses it as `X-Hail-Internal-Authorization` to clear the
# public gateway's OAuth proxy. See `LambdaWorkerAPI.extra_hail_headers`.
HAIL_INTERNAL_TOKEN_FILE = '/hail-internal-token/tokens.json'


def _read_hail_internal_token() -> Optional[str]:
    try:
        with open(HAIL_INTERNAL_TOKEN_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)['default']
    except FileNotFoundError:
        return None


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
            activation_token=activation_token,
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

    async def _build_worker_env_vars(
        self,
        file_store,
        max_idle_time_msecs: int,
        unreserved_disk_size_gb: int,
    ) -> Dict[str, str]:
        """Mirror the GCP docker run env-var set, with CLOUD=lambda and a GCP-shaped ZONE."""
        from ..batch_configuration import DEFAULT_NAMESPACE, DOCKER_PREFIX, DOCKER_ROOT_IMAGE, INTERNAL_GATEWAY_IP
        from ..cloud.utils import ACCEPTABLE_QUERY_JAR_URL_PREFIX

        activation_token = self._activation_token
        if not activation_token:
            record = await self.db.select_and_fetchone(
                'SELECT activation_token FROM instances WHERE name = %s', (self.name,)
            )
            activation_token = record['activation_token']
        assert activation_token is not None

        assert self.ip_address is not None, (
            f'ip_address must be set before calling _build_worker_env_vars on {self.name}'
        )

        ic = self.instance_config
        region = ic.region_for(self.location)
        # Preserve the rsplit('/', 1)[1] parse in GCPWorkerAPI.from_env()
        synthetic_zone = f'projects/hail-vdc/zones/{region}-b'

        hail_internal_token = _read_hail_internal_token()
        assert hail_internal_token is not None, (
            f'{HAIL_INTERNAL_TOKEN_FILE} is missing; create the `hail-internal-token` '
            f'k8s secret (see batch/deployment.yaml)'
        )

        return {
            'CLOUD': 'lambda',
            'PROJECT': os.environ.get('PROJECT', 'hail-vdc'),
            'ZONE': synthetic_zone,
            'REGION': region,
            'CORES': str(ic.cores),
            'NAME': self.name,
            'NAMESPACE': DEFAULT_NAMESPACE,
            'ACTIVATION_TOKEN': activation_token,
            'IP_ADDRESS': self.ip_address,
            'BATCH_LOGS_STORAGE_URI': file_store.batch_logs_storage_uri,
            'INSTANCE_ID': file_store.instance_id,
            'DOCKER_PREFIX': DOCKER_PREFIX,
            'DOCKER_ROOT_IMAGE': DOCKER_ROOT_IMAGE,
            'INSTANCE_CONFIG': base64.b64encode(json.dumps(ic.to_dict()).encode()).decode(),
            'MAX_IDLE_TIME_MSECS': str(max_idle_time_msecs),
            'BATCH_WORKER_IMAGE': os.environ['HAIL_BATCH_WORKER_IMAGE'],
            'UNRESERVED_WORKER_DATA_DISK_SIZE_GB': str(unreserved_disk_size_gb),
            'ACCEPTABLE_QUERY_JAR_URL_PREFIX': ACCEPTABLE_QUERY_JAR_URL_PREFIX,
            'INTERNAL_GATEWAY_IP': INTERNAL_GATEWAY_IP,
            'GOOGLE_APPLICATION_CREDENTIALS': '/gsa-key.json',
            'HAIL_INTERNAL_TOKEN': hail_internal_token,
        }

    async def prepare_host_runtime(self) -> None:
        """SCP the GSA key, configure host docker auth, and seed the host directories /
        config files that the worker container's bind mounts expect."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._scp_gsa_key_blocking)
        await loop.run_in_executor(None, self._configure_docker_auth_blocking)
        await loop.run_in_executor(None, self._prepare_host_dirs_and_config_blocking)

    def _scp_gsa_key_blocking(self) -> None:
        with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
            private_key = paramiko.RSAKey.from_private_key(key_file)
        transport = paramiko.Transport((self.ip_address, 22))
        transport.connect(username='ubuntu', pkey=private_key)
        try:
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put('/lambda-gsa-key/key.json', '/home/ubuntu/gsa-key.json')
            sftp.chmod('/home/ubuntu/gsa-key.json', 0o600)
            sftp.close()
        finally:
            transport.close()

    def _configure_docker_auth_blocking(self) -> None:
        with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
            private_key = paramiko.RSAKey.from_private_key(key_file)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=self.ip_address, username='ubuntu', pkey=private_key)
        try:
            # Run as root so root's ~/.docker/config.json gets the Artifact Registry
            # credential helper; the worker container is launched via `sudo docker run`
            # below, which uses root's config to authenticate to us-docker.pkg.dev.
            for cmd in (
                'sudo gcloud auth activate-service-account --key-file=/home/ubuntu/gsa-key.json',
                'sudo gcloud auth configure-docker us-docker.pkg.dev --quiet',
            ):
                stdin, stdout, stderr = ssh.exec_command(cmd)
                rc = stdout.channel.recv_exit_status()
                if rc != 0:
                    err = stderr.read().decode()
                    raise RuntimeError(f'`{cmd}` exited {rc}: {err}')
        finally:
            ssh.close()

    def _prepare_host_dirs_and_config_blocking(self) -> None:
        # Mirrors the bootstrap that GCP's `create_instance.py` startup script does
        # before `docker run`: create the directories that the worker container's
        # bind mounts target, then populate `/global-config/*` (one file per key,
        # matching `gear.cloud_config.read_config_secret`) and write
        # `/deploy-config/deploy-config.json`. Lambda VMs have no data disk, so
        # `/host` is just a plain dir on the boot disk (xfs_quota is disabled
        # worker-side for CLOUD=='lambda').
        global_config = get_global_config()
        # Lambda VMs aren't in the GCP VPC so they reach the driver via the public
        # `https://internal.hail.is/...` endpoint, which is fronted by the `gateway`
        # Envoy. `LambdaWorkerAPI.extra_hail_headers` supplies an
        # `X-Hail-Internal-Authorization: Bearer ...` Hail session id that the gateway's
        # ext_authz filter forwards to auth's `verify_dev_credentials`; the
        # `activating_instances_only` decorator behind it then checks X-Hail-Instance-*.
        deploy_config_json = json.dumps(get_deploy_config().with_location('external').get_config())

        script_lines = [
            'set -e',
            'sudo mkdir -p /host /batch /batch/jvm-container-logs '
            '/logs /global-config /deploy-config /cloudfuse /etc/netns',
        ]
        for key, value in global_config.items():
            script_lines.append(
                f'echo -n {shlex.quote(value)} | sudo tee /global-config/{shlex.quote(key)} > /dev/null'
            )
        script_lines.append(
            f'echo -n {shlex.quote(deploy_config_json)} | sudo tee /deploy-config/deploy-config.json > /dev/null'
        )
        script = '\n'.join(script_lines)

        with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
            private_key = paramiko.RSAKey.from_private_key(key_file)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=self.ip_address, username='ubuntu', pkey=private_key)
        try:
            stdin, stdout, stderr = ssh.exec_command('bash -s', timeout=60)
            stdin.write(script)
            stdin.channel.shutdown_write()
            rc = stdout.channel.recv_exit_status()
            if rc != 0:
                out = stdout.read().decode()
                err = stderr.read().decode()
                raise RuntimeError(f'host dir/config prep on {self.name} exited {rc}: stdout={out!r} stderr={err!r}')
        finally:
            ssh.close()

    async def run_worker_container(
        self,
        file_store,
        max_idle_time_msecs: int,
        unreserved_disk_size_gb: int,
    ) -> None:
        env_vars = await self._build_worker_env_vars(file_store, max_idle_time_msecs, unreserved_disk_size_gb)
        env_args = ' '.join(f'-e {k}={shlex.quote(v)}' for k, v in env_vars.items())
        batch_worker_image = env_vars['BATCH_WORKER_IMAGE']
        # `ubuntu` is not in the docker group on Lambda VMs; use passwordless sudo.
        # `BATCH_WORKER_IMAGE_ID` and `INTERNET_INTERFACE` are read at worker.py import time
        # and have to be derived on the VM (post-pull image id; the host's default-route NIC).
        # GCP's create_instance.py does the equivalent in its startup script.
        docker_cmd = (
            f'set -e && '
            f'sudo docker pull {shlex.quote(batch_worker_image)} && '
            f'BATCH_WORKER_IMAGE_ID=$(sudo docker inspect {shlex.quote(batch_worker_image)} '
            f"--format='{{{{.Id}}}}' | cut -d: -f2) && "
            f"INTERNET_INTERFACE=$(ip -o route show default | awk '{{print $5; exit}}') && "
            f'sudo docker run -d --name worker '
            f'-e BATCH_WORKER_IMAGE_ID="$BATCH_WORKER_IMAGE_ID" '
            f'-e INTERNET_INTERFACE="$INTERNET_INTERFACE" '
            f'{env_args} '
            f'-v /home/ubuntu/gsa-key.json:/gsa-key.json:ro '
            f'-v /var/run/docker.sock:/var/run/docker.sock '
            f'-v /usr/bin/docker:/usr/bin/docker '
            f'-v /batch:/batch:shared '
            f'-v /logs:/logs '
            f'-v /global-config:/global-config '
            f'-v /deploy-config:/deploy-config '
            f'-v /cloudfuse:/cloudfuse:shared '
            f'-v /etc/netns:/etc/netns '
            f'-v /sys/fs/cgroup:/sys/fs/cgroup '
            f'--mount type=bind,source=/host,target=/host '
            f'-p 5000:5000 '
            f'--device /dev/fuse '
            f'--privileged --cap-add SYS_ADMIN --userns host --pid host --cgroupns host '
            f'--network host '
            f'--gpus all '
            f'{shlex.quote(batch_worker_image)} '
            f'python3 -u -m batch.worker.worker'
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._ssh_exec_blocking, docker_cmd, 'docker pull + run')

    def _ssh_exec_blocking(self, command: str, label: str) -> str:
        with open('/lambda-ssh-key/lambda-ssh-key', 'r') as key_file:
            private_key = paramiko.RSAKey.from_private_key(key_file)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=self.ip_address, username='ubuntu', pkey=private_key)
        try:
            stdin, stdout, stderr = ssh.exec_command(command, timeout=300)
            rc = stdout.channel.recv_exit_status()
            out = stdout.read().decode()
            err = stderr.read().decode()
            if rc != 0:
                raise RuntimeError(f'{label} on {self.name} exited {rc}: stdout={out!r} stderr={err!r}')
            return out
        finally:
            ssh.close()

    def lambda_worker_log_url(self, file_store) -> str:
        return (
            f'{file_store.batch_logs_storage_uri}/batch/lambda-worker-logs/'
            f'{file_store.instance_id}/{self.name}.log'
        )

    async def upload_worker_docker_logs(self, file_store) -> None:
        """SSH to the Lambda VM, capture `sudo docker logs worker` (stdout+stderr),
        and overwrite the GCS object at `lambda_worker_log_url`. This is the only
        place the worker's stdout/stderr survives after the VM is killed. Safe to
        call repeatedly; each call rewrites the full snapshot.

        Errors (SSH unreachable, docker not yet running, GCS write failure) are
        logged and swallowed — they must never interfere with the lifecycle loop.
        """
        if not self.ip_address:
            return
        loop = asyncio.get_event_loop()
        try:
            # `|| true` keeps rc=0 if the container isn't there yet so we still
            # upload whatever was captured (often nothing, which is itself signal).
            out = await loop.run_in_executor(
                None,
                self._ssh_exec_blocking,
                'sudo docker logs worker 2>&1 || true',
                'fetch worker docker logs',
            )
        except Exception as e:
            log.warning(f'{self}: could not fetch worker docker logs over SSH: {e}')
            return
        url = self.lambda_worker_log_url(file_store)
        try:
            await file_store.fs.write(url, out.encode('utf-8'))
        except Exception:
            log.exception(f'{self}: could not upload worker docker logs to {url}')

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
