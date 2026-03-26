"""Common deployment utilities for Lustre and GPFS deployments on AWS."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import subprocess
import time
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import paramiko
import yaml

# ============================================================================
# Logging
# ============================================================================


def setup_logger(
    name: str,
    log_level: int = logging.INFO,
) -> tuple[logging.Logger, str]:
    """Configure logger with file (DEBUG) + console (log_level) handlers."""
    log_file = f'{name}-{datetime.now().strftime("%Y%m%d-%H%M%S")}.log'

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'),
    )
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(ch)

    logger.info(f'Logging to {log_file}')
    return logger, log_file


# ============================================================================
# SSH Operations
# ============================================================================


def ssh_exec(
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    command: str,
    timeout: int = 30,
    logger: logging.Logger | None = None,
) -> tuple[int, str, str]:
    """Execute SSH command. Returns (exit_code, stdout, stderr)."""
    if logger:
        logger.debug(f'[{host_ip}] $ {command}')
    try:
        _stdin, stdout, stderr = ssh_client.exec_command(
            command,
            timeout=timeout,
        )
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8').strip()
        error = stderr.read().decode('utf-8').strip()
        if logger:
            if output:
                logger.debug(f'[{host_ip}] {output}')
            if error:
                logger.debug(f'[{host_ip}] stderr: {error}')
            if exit_code != 0:
                logger.error(f'[{host_ip}] Exit code: {exit_code}')
        return exit_code, output, error
    except Exception as e:
        if logger:
            logger.error(f'[{host_ip}] SSH failed: {e}')
        return -1, '', str(e)


def wait_for_ssh(
    host_ip: str,
    key_path: str,
    user: str,
    timeout: int = 300,
    logger: logging.Logger | None = None,
) -> paramiko.SSHClient | None:
    """Wait for SSH to become available. Returns SSHClient or None."""
    if logger:
        logger.info(f'Waiting for SSH on {host_ip}')
    start = time.time()
    while time.time() - start < timeout:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                host_ip,
                username=user,
                key_filename=key_path,
                timeout=10,
            )
            if logger:
                logger.info(f'SSH ready on {host_ip}')
            return ssh
        except (paramiko.SSHException, OSError):
            time.sleep(5)
    if logger:
        logger.error(f'[{host_ip}] SSH timeout after {timeout}s')
    return None


def reboot_and_wait(
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    key_path: str,
    user: str,
    logger: logging.Logger | None = None,
) -> paramiko.SSHClient | None:
    """Reboot instance and wait for SSH to return."""
    if logger:
        logger.info(f'Rebooting {host_ip}')
    with contextlib.suppress(Exception):
        ssh_exec(ssh_client, host_ip, 'sudo reboot', timeout=5, logger=logger)
    ssh_client.close()
    time.sleep(10)
    return wait_for_ssh(host_ip, key_path, user, timeout=600, logger=logger)


# ============================================================================
# SSH Config
# ============================================================================


def update_ssh_config(
    instances: list[dict[str, str]],
    marker_name: str,
    logger: logging.Logger | None = None,
) -> None:
    """Update ~/.ssh/config with deployment instances (idempotent)."""
    if logger:
        logger.info('Updating SSH config')

    path = Path.home() / '.ssh' / 'config'
    path.parent.mkdir(mode=0o700, exist_ok=True)

    start = f'# >>> {marker_name} deployment instances >>>'
    end = f'# <<< {marker_name} deployment instances <<<'

    existing = path.read_text() if path.exists() else ''
    existing = re.sub(
        rf'{re.escape(start)}.*?{re.escape(end)}\n*',
        '',
        existing,
        flags=re.DOTALL,
    ).rstrip('\n')

    lines = [start]
    for i in instances:
        lines += [
            f'Host {i["name"]}',
            f'    HostName {i["ip"]}',
            f'    User {i["user"]}',
            f'    IdentityFile {i["key_path"]}',
            '    StrictHostKeyChecking no',
            '',
        ]
    lines.append(end)

    path.write_text(existing + '\n\n' + '\n'.join(lines).strip() + '\n')
    path.chmod(0o600)


# ============================================================================
# AWS Credential Copying
# ============================================================================


def copy_aws_credentials(
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    ssh_user: str,
    region: str,
    logger: logging.Logger | None = None,
) -> None:
    """Copy local ~/.aws credentials to remote host via SFTP."""
    if logger:
        logger.info('Copying AWS credentials')

    creds = Path.home() / '.aws' / 'credentials'
    config = Path.home() / '.aws' / 'config'

    if not creds.exists():
        if logger:
            logger.warning('~/.aws/credentials not found, skipping')
        return

    ssh_exec(ssh_client, host_ip, 'mkdir -p ~/.aws', logger=logger)

    sftp = ssh_client.open_sftp()
    try:
        home = f'/home/{ssh_user}'
        with sftp.file(f'{home}/.aws/credentials', 'w') as f:
            f.write(creds.read_text())
        sftp.chmod(f'{home}/.aws/credentials', 0o600)
        if config.exists():
            with sftp.file(f'{home}/.aws/config', 'w') as f:
                f.write(config.read_text())
            sftp.chmod(f'{home}/.aws/config', 0o600)
    finally:
        sftp.close()

    # Set region if not configured
    _ec, out, _ = ssh_exec(
        ssh_client,
        host_ip,
        "aws configure get region || echo 'NOT_SET'",
        logger=logger,
    )
    if 'NOT_SET' in out:
        ssh_exec(
            ssh_client,
            host_ip,
            f'aws configure set region {region}',
            logger=logger,
        )

    if logger:
        logger.info('AWS credentials copied')


# ============================================================================
# Cluster State File
# ============================================================================

_DEPLOY_DIR = os.path.dirname(__file__)


def _state_path(state_file: str) -> str:
    """Return absolute path for a state file in the deploy directory."""
    return os.path.join(_DEPLOY_DIR, state_file)


def load_cluster_state(
    state_file: str,
    logger: logging.Logger | None = None,
) -> dict | None:
    """Load cluster state from JSON file. Returns dict or None."""
    path = _state_path(state_file)
    if os.path.exists(path):
        with open(path) as f:
            state = json.load(f)
        if logger:
            cid = state.get('cluster_id')
            logger.info(f'Loaded cluster state (cluster_id={cid})')
        return state
    return None


def save_cluster_state(
    state: dict,
    state_file: str,
    logger: logging.Logger | None = None,
) -> None:
    """Save cluster state to JSON file."""
    path = _state_path(state_file)
    with open(path, 'w') as f:
        json.dump(state, f, indent=2)
    if logger:
        logger.info('Cluster state saved')


# ============================================================================
# EC2 Instance Creation
# ============================================================================


def _find_existing_instance(
    ec2_client,
    name: str,
    logger: logging.Logger | None = None,
) -> str | None:
    """Return instance ID if a running/pending/stopped *name* exists."""
    existing = ec2_client.describe_instances(
        Filters=[
            {'Name': 'tag:Name', 'Values': [name]},
            {
                'Name': 'instance-state-name',
                'Values': ['pending', 'running', 'stopped'],
            },
        ],
    )
    for r in existing.get('Reservations', []):
        for inst in r.get('Instances', []):
            instance_id = inst['InstanceId']
            state = inst['State']['Name']
            if logger:
                logger.info(
                    f'{name} already exists: {instance_id}'
                    f' ({state}) — reusing',
                )
            if state == 'stopped':
                ec2_client.start_instances(
                    InstanceIds=[instance_id],
                )
                ec2_client.get_waiter('instance_running').wait(
                    InstanceIds=[instance_id],
                )
            elif state == 'pending':
                ec2_client.get_waiter('instance_running').wait(
                    InstanceIds=[instance_id],
                )
            return instance_id
    return None


def create_instance(  # noqa: PLR0913
    ec2_client,
    name: str,
    ami_id: str,
    instance_type: str,
    key_name: str,
    security_group: str,
    availability_zone: str,
    root_volume_size: int = 50,
    data_volumes: list[tuple[str, int]] | None = None,
    placement_group: str | None = None,
    iops: int = 3000,
    throughput: int = 125,
    logger: logging.Logger | None = None,
) -> str:
    """Create an EC2 instance with optional data volumes.

    Returns instance_id.
    """
    if logger:
        logger.info(f'Launching {name}')

    found = _find_existing_instance(ec2_client, name, logger)
    if found:
        return found

    bdm = [
        {
            'DeviceName': '/dev/sda1',
            'Ebs': {
                'DeleteOnTermination': True,
                'Iops': iops,
                'VolumeSize': root_volume_size,
                'VolumeType': 'gp3',
                'Throughput': throughput,
            },
        },
    ]

    for dev, size in data_volumes or []:
        bdm.append(
            {
                'DeviceName': dev,
                'Ebs': {
                    'DeleteOnTermination': True,
                    'Iops': iops,
                    'VolumeSize': size,
                    'VolumeType': 'gp3',
                    'Throughput': throughput,
                },
            },
        )

    placement = {'AvailabilityZone': availability_zone}
    if placement_group:
        placement['GroupName'] = placement_group

    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=1,
        MaxCount=1,
        Placement=placement,
        BlockDeviceMappings=bdm,
        NetworkInterfaces=[
            {
                'AssociatePublicIpAddress': True,
                'DeviceIndex': 0,
                'Groups': [security_group],
            },
        ],
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': name}],
            },
        ],
        MetadataOptions={
            'HttpEndpoint': 'enabled',
            'HttpPutResponseHopLimit': 2,
            'HttpTokens': 'required',
        },
    )

    instance_id = response['Instances'][0]['InstanceId']
    if logger:
        logger.info(f'{name} launched: {instance_id}')

    ec2_client.get_waiter('instance_running').wait(InstanceIds=[instance_id])
    if logger:
        logger.info(f'{name} running: {instance_id}')

    return instance_id


def allocate_eip(
    ec2_client,
    instance_id: str,
    name: str,
    logger: logging.Logger | None = None,
) -> tuple[str, str, str, str]:
    """Allocate EIP, associate with instance.

    Returns (alloc_id, public_ip, private_ip, private_dns).
    """
    if logger:
        logger.info(f'Allocating EIP for {name}')

    # Check if instance already has an associated EIP
    existing = ec2_client.describe_addresses(
        Filters=[{'Name': 'instance-id', 'Values': [instance_id]}],
    )
    if existing['Addresses']:
        addr = existing['Addresses'][0]
        alloc_id = addr['AllocationId']
        public_ip = addr['PublicIp']
        inst = ec2_client.describe_instances(
            InstanceIds=[instance_id],
        )['Reservations'][0]['Instances'][0]
        private_ip = inst['PrivateIpAddress']
        private_dns = inst.get('PrivateDnsName', '')
        if logger:
            logger.info(f'{name}: reusing EIP {public_ip}')
        return alloc_id, public_ip, private_ip, private_dns

    eip = ec2_client.allocate_address(Domain='vpc')
    alloc_id, public_ip = eip['AllocationId'], eip['PublicIp']

    ec2_client.create_tags(
        Resources=[alloc_id],
        Tags=[{'Key': 'Name', 'Value': f'{name}-eip'}],
    )
    ec2_client.associate_address(AllocationId=alloc_id, InstanceId=instance_id)

    inst = ec2_client.describe_instances(
        InstanceIds=[instance_id],
    )['Reservations'][0]['Instances'][0]
    private_ip = inst['PrivateIpAddress']
    private_dns = inst.get('PrivateDnsName', '')

    if logger:
        logger.info(f'{name}: public={public_ip} private={private_ip}')

    return alloc_id, public_ip, private_ip, private_dns


# ============================================================================
# YAML Config
# ============================================================================


def load_deploy_config(section: str) -> dict:
    """Load deploy-config.yaml merging common with *section* (lustre or gpfs).

    Section-specific keys override common keys.
    Expands ``ssh_key_path`` via :func:`os.path.expanduser`.
    """
    config_path = Path(__file__).parent / 'deploy-config.yaml'
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    common = dict(raw.get('common', {}))
    specific = dict(raw.get(section, {}))
    merged = {**common, **specific}

    if 'ssh_key_path' in merged:
        merged['ssh_key_path'] = os.path.expanduser(merged['ssh_key_path'])

    return merged


# ============================================================================
# AWS Validation
# ============================================================================


def validate_aws_config(
    key_path: str,
    security_group: str,
    aws_region: str,
    aws_az: str,
) -> None:
    """Validate common AWS configuration parameters.

    Raises:
        AssertionError: with descriptive message on failure.
    """
    assert os.path.exists(key_path), f'SSH key file not found: {key_path}'
    assert security_group.startswith('sg-'), (
        f'Invalid security group: {security_group}'
    )
    assert aws_az.startswith(aws_region), (
        f'AZ {aws_az} does not match region {aws_region}'
    )


# ============================================================================
# Instance Launch (Parallel)
# ============================================================================


def launch_instances_parallel(  # noqa: PLR0913
    ec2_client,
    launch_specs: list[dict],
    key_name: str,
    security_group: str,
    availability_zone: str,
    root_volume_size: int = 50,
    logger: logging.Logger | None = None,
) -> dict:
    """Launch instances in parallel and allocate EIPs.

    Each entry in *launch_specs* is a dict with keys:
        name, ami_id, instance_type, data_volumes (list of (dev, size)),
        placement_group (str, optional).

    Returns:
        dict mapping name -> {name, instance_id, eip_allocation, eip,
        private_ip, private_dns}.
    """

    def _launch(spec):
        kwargs = {
            'name': spec['name'],
            'ami_id': spec['ami_id'],
            'instance_type': spec['instance_type'],
            'key_name': key_name,
            'security_group': security_group,
            'availability_zone': availability_zone,
            'root_volume_size': root_volume_size,
            'logger': logger,
        }
        if spec.get('data_volumes'):
            kwargs['data_volumes'] = spec['data_volumes']
        if spec.get('placement_group'):
            kwargs['placement_group'] = spec['placement_group']

        instance_id = create_instance(ec2_client, **kwargs)
        alloc, eip, priv_ip, priv_dns = allocate_eip(
            ec2_client,
            instance_id,
            spec['name'],
            logger=logger,
        )
        return {
            'name': spec['name'],
            'instance_id': instance_id,
            'eip_allocation': alloc,
            'eip': eip,
            'private_ip': priv_ip,
            'private_dns': priv_dns,
        }

    tasks = [(s['name'], lambda s=s: _launch(s)) for s in launch_specs]
    return run_parallel(tasks, logger=logger)


# ============================================================================
# AMI & Maven Resolution
# ============================================================================


def resolve_rhel_ami(
    ec2_client,
    region: str,
    logger: logging.Logger | None = None,
) -> tuple[str, str]:
    """Resolve latest RHEL 8 x86_64 AMI. Returns (ami_id, ami_name)."""
    response = ec2_client.describe_images(
        Owners=['309956199498'],  # Red Hat official
        Filters=[
            {'Name': 'name', 'Values': ['RHEL-8*']},
            {'Name': 'architecture', 'Values': ['x86_64']},
            {'Name': 'state', 'Values': ['available']},
        ],
    )
    images = sorted(response['Images'], key=lambda x: x['CreationDate'])
    if not images:
        raise RuntimeError(f'No RHEL 8 AMIs found in region {region}')
    ami = images[-1]
    if logger:
        logger.info(f'RHEL AMI: {ami["ImageId"]} ({ami["Name"]})')
    return ami['ImageId'], ami['Name']


def resolve_ubuntu_ami(
    ec2_client,
    region: str,
    logger: logging.Logger | None = None,
) -> tuple[str, str]:
    """Resolve latest Ubuntu 22.04 x86_64 AMI. Returns (ami_id, ami_name)."""
    response = ec2_client.describe_images(
        Owners=['099720109477'],  # Canonical official
        Filters=[
            {
                'Name': 'name',
                'Values': ['ubuntu/images/hvm-ssd/ubuntu-jammy-22.04*'],
            },
            {'Name': 'architecture', 'Values': ['x86_64']},
            {'Name': 'state', 'Values': ['available']},
        ],
    )
    images = sorted(response['Images'], key=lambda x: x['CreationDate'])
    if not images:
        raise RuntimeError(f'No Ubuntu 22.04 AMIs found in region {region}')
    ami = images[-1]
    if logger:
        logger.info(f'Ubuntu AMI: {ami["ImageId"]} ({ami["Name"]})')
    return ami['ImageId'], ami['Name']


def resolve_maven_version(
    base_url: str = 'https://dlcdn.apache.org/maven/maven-3',
    logger: logging.Logger | None = None,
) -> tuple[str, str]:
    """Resolve latest Maven 3.x version. Returns (version, download_url)."""
    import requests  # noqa: PLC0415

    resp = requests.get(f'{base_url}/', timeout=15)
    resp.raise_for_status()
    versions = re.findall(r'href="(3\.\d+\.\d+)/"', resp.text)
    if not versions:
        raise RuntimeError(
            f'Could not parse Maven versions from {base_url}/'
            ' — page format may have changed.',
        )
    latest = sorted(
        versions,
        key=lambda v: [int(x) for x in v.split('.')],
    )[-1]
    url = f'{base_url}/{latest}/binaries/apache-maven-{latest}-bin.zip'
    if logger:
        logger.info(f'Maven: {latest}')
    return latest, url


# ============================================================================
# RHEL Setup (Lustre Server)
# ============================================================================


def setup_rhel(
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    logger: logging.Logger | None = None,
) -> bool:
    """Install Lustre server packages on RHEL. Returns True on success."""
    if logger:
        logger.info('Configuring Lustre repositories')

    ssh_exec(
        ssh_client,
        host_ip,
        """sudo bash -c 'cat > /etc/yum.repos.d/lustre-server.repo <<"EOF"
[lustre-server]
name=Lustre Server - EL8
baseurl=https://downloads.whamcloud.com/public/lustre/latest-release/el8.10/server/
gpgcheck=0
enabled=1
EOF'""",
        logger=logger,
    )

    ssh_exec(
        ssh_client,
        host_ip,
        """sudo bash -c 'cat > /etc/yum.repos.d/e2fsprogs.repo <<"EOF"
[e2fsprogs]
name=e2fsprogs - EL8
baseurl=https://downloads.whamcloud.com/public/e2fsprogs/latest/el8/
gpgcheck=0
enabled=1
EOF'""",
        logger=logger,
    )

    if logger:
        logger.info('Installing Lustre packages')
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo dnf clean all && sudo dnf makecache',
        timeout=100,
        logger=logger,
    )
    ec, _, _ = ssh_exec(
        ssh_client,
        host_ip,
        'sudo dnf install -y lustre kmod-lustre kmod-lustre-osd-ldiskfs '
        'lustre-osd-ldiskfs-mount e2fsprogs chrony',
        timeout=600,
        logger=logger,
    )
    if ec != 0:
        if logger:
            logger.error('Lustre package install failed')
        return False

    ssh_exec(
        ssh_client,
        host_ip,
        'sudo systemctl enable --now chronyd',
        logger=logger,
    )

    ssh_exec(
        ssh_client,
        host_ip,
        """sudo tee /etc/modprobe.d/lnet.conf >/dev/null <<'EOF'
options lnet networks=tcp
EOF""",
        logger=logger,
    )

    if logger:
        logger.info('RHEL setup done (reboot needed)')
    return True


# ============================================================================
# Ubuntu Setup (Client)
# ============================================================================


def setup_ubuntu(  # noqa: PLR0913
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    maven_version: str,
    maven_url: str,
    ssh_user: str,
    region: str,
    logger: logging.Logger | None = None,
) -> bool:
    """Install system packages, AWS CLI, Maven on Ubuntu.

    Returns True on success.
    """
    # deadsnakes PPA for python3.11
    if logger:
        logger.info('Adding deadsnakes PPA')
    ssh_exec(
        ssh_client,
        host_ip,
        "sudo bash -c '"
        'echo "deb https://ppa.launchpadcontent.net/'
        'deadsnakes/ppa/ubuntu jammy main"'
        " > /etc/apt/sources.list.d/deadsnakes-ppa.list'",
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo gpg --keyserver keyserver.ubuntu.com '
        '--recv-keys F23C5A6CF475977595C89F51BA6932366A755776',
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo gpg --export F23C5A6CF475977595C89F51BA6932366A755776 '
        '| sudo tee /etc/apt/trusted.gpg.d/deadsnakes.gpg > /dev/null',
        logger=logger,
    )

    # System packages
    if logger:
        logger.info('Installing system packages')
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo add-apt-repository universe -y 2>/dev/null || true',
        timeout=60,
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo apt update',
        timeout=120,
        logger=logger,
    )
    packages = (
        'ansible bison build-essential flex g++-12 gcc-12 git jq '
        'librdkafka-dev libsasl2-dev libssl-dev libxml2-dev libxslt1-dev '
        'linux-headers-$(uname -r) lz4 openjdk-11-jdk python3-pip '
        'python3.11 python3.11-dev python3.11-distutils python3.11-venv '
        'unzip zip zlib1g-dev'
    )
    ec, _, _ = ssh_exec(
        ssh_client,
        host_ip,
        f'sudo apt install -y {packages}',
        timeout=600,
        logger=logger,
    )
    if ec != 0:
        if logger:
            logger.error('System package install failed')
        return False

    # AWS CLI v2
    if logger:
        logger.info('Installing AWS CLI v2')
    ssh_exec(
        ssh_client,
        host_ip,
        'cd ~ && curl -s'
        ' "https://awscli.amazonaws.com/'
        'awscli-exe-linux-x86_64.zip"'
        ' -o awscliv2.zip'
        ' && unzip -qo awscliv2.zip'
        ' && sudo ./aws/install'
        ' && rm -rf awscliv2.zip aws',
        timeout=120,
        logger=logger,
    )

    # Maven
    maven_dir = f'apache-maven-{maven_version}'
    if logger:
        logger.info(f'Installing Maven {maven_version}')
    ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~ && wget -q {maven_url}'
        f' && unzip -qo {maven_dir}-bin.zip'
        f' && rm {maven_dir}-bin.zip',
        timeout=120,
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        f"echo 'export PATH=$HOME/{maven_dir}/bin:$PATH' >> ~/.bashrc",
        logger=logger,
    )

    # AWS credentials
    copy_aws_credentials(ssh_client, host_ip, ssh_user, region, logger)

    return True


# ============================================================================
# Docker
# ============================================================================


def install_docker(
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    ssh_user: str,
    logger: logging.Logger | None = None,
) -> bool:
    """Install Docker Engine and Docker Compose on Ubuntu.

    After calling this, close and reopen the SSH session
    (ssh.close() + wait_for_ssh()) to activate docker group membership.

    Returns True on success.
    """
    if logger:
        logger.info('Installing Docker prerequisites')
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo apt-get install -y ca-certificates curl',
        timeout=120,
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo install -m 0755 -d /etc/apt/keyrings',
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg'
        ' -o /etc/apt/keyrings/docker.asc',
        timeout=60,
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo chmod a+r /etc/apt/keyrings/docker.asc',
        logger=logger,
    )

    if logger:
        logger.info('Adding Docker repository')
    docker_repo_cmd = (
        'echo "deb [arch=$(dpkg --print-architecture)'
        ' signed-by=/etc/apt/keyrings/docker.asc]'
        ' https://download.docker.com/linux/ubuntu'
        ' $(. /etc/os-release && echo'
        ' \\"${UBUNTU_CODENAME:-$VERSION_CODENAME}\\")'
        ' stable" | sudo tee'
        ' /etc/apt/sources.list.d/docker.list > /dev/null'
    )
    ssh_exec(ssh_client, host_ip, docker_repo_cmd, logger=logger)

    if logger:
        logger.info('Installing Docker Engine')
    ssh_exec(
        ssh_client,
        host_ip,
        'sudo apt-get update',
        timeout=120,
        logger=logger,
    )
    ec, _, _ = ssh_exec(
        ssh_client,
        host_ip,
        'sudo apt-get install -y docker-ce docker-ce-cli'
        ' containerd.io docker-buildx-plugin docker-compose-plugin',
        timeout=300,
        logger=logger,
    )
    if ec != 0:
        if logger:
            logger.error('Failed to install Docker Engine')
        return False

    ssh_exec(
        ssh_client,
        host_ip,
        'sudo groupadd docker || true',
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        f'sudo usermod -aG docker {ssh_user}',
        logger=logger,
    )

    if logger:
        logger.info('Docker installed')
    return True


# ============================================================================
# Kafka
# ============================================================================


def parse_kafka_topics(topic_string: str) -> dict[str, int]:
    """Parse Kafka topics string into partition mapping.

    Expects format like: "fset1-topic-1p,fset1-topic-2p,fset1-topic-4p"

    Returns dict mapping topic names to partition counts.

    Example:
        >>> parse_kafka_topics("topic-1p,topic-2p,topic-4p")
        {'topic-1p': 1, 'topic-2p': 2, 'topic-4p': 4}
    """
    topic_partitions: dict[str, int] = {}
    for raw_topic in topic_string.split(','):
        topic = raw_topic.strip()
        if not topic:
            continue
        if '-' in topic and 'p' in topic:
            parts = topic.rsplit('-', 1)
            if len(parts) == 2 and parts[1].endswith('p'):  # noqa: PLR2004
                try:
                    partitions = int(parts[1][:-1])
                    topic_partitions[topic] = partitions
                except ValueError:
                    topic_partitions[topic] = 1
            else:
                topic_partitions[topic] = 1
        else:
            topic_partitions[topic] = 1
    return topic_partitions


# ============================================================================
# Permissions
# ============================================================================


def setup_permissions(  # noqa: PLR0913
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    path: str,
    ssh_user: str,
    group_name: str = 'icicle-users',
    logger: logging.Logger | None = None,
) -> None:
    """Create group, add user, set ownership and permissions on path."""
    if logger:
        logger.info(f'Setting permissions on {path}')
    ssh_exec(
        ssh_client,
        host_ip,
        f'sudo groupadd -f {group_name}',
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        f'sudo usermod -aG {group_name} {ssh_user}',
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        f'sudo chgrp -R {group_name} {path}',
        logger=logger,
    )
    ssh_exec(ssh_client, host_ip, f'sudo chmod 2775 {path}', logger=logger)
    ssh_exec(
        ssh_client,
        host_ip,
        f'sudo find {path} -type d -exec chmod 2775 {{}} +',
        timeout=120,
        logger=logger,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        f'sudo find {path} -type f -exec chmod 664 {{}} +',
        timeout=120,
        logger=logger,
    )


# ============================================================================
# GitHub Token
# ============================================================================


def read_github_token() -> str:
    """Read GITHUB_TOKEN from shell environment (~/.zshrc).

    Raises ValueError if the token is not found.
    """
    result = subprocess.run(
        ['zsh', '-c', 'source ~/.zshrc && echo $GITHUB_TOKEN'],
        capture_output=True,
        text=True,
        check=False,
    )
    token = result.stdout.strip()
    if not token:
        raise ValueError(
            "GITHUB_TOKEN not found. Please ensure it's exported in ~/.zshrc",
        )
    return token


# ============================================================================
# Icicle Installation
# ============================================================================


def install_icicle(  # noqa: PLR0913
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    github_token: str,
    username: str,
    repo_url: str,
    branch: str,
    python_version: str = '3.11',
    logger: logging.Logger | None = None,
) -> bool:
    """Clone repo, create venv, pip install. Returns True on success."""
    if logger:
        logger.info(f'Installing icicle ({branch})')

    ssh_exec(
        ssh_client,
        host_ip,
        'git config --global credential.helper store',
        logger=logger,
    )
    cred_cmd = (
        f'printf "https://{username}:%s@github.com\\n"'  # pragma: allowlist secret  # noqa: E501
        f' "{github_token}"'
        ' > ~/.git-credentials && chmod 600 ~/.git-credentials'
    )
    ssh_exec(ssh_client, host_ip, cred_cmd)

    # Clone or update if already cloned
    ec, _, _ = ssh_exec(
        ssh_client,
        host_ip,
        'test -d ~/icicle',
        logger=logger,
    )
    if ec == 0:
        if logger:
            logger.info('~/icicle already exists — updating')
        ssh_exec(
            ssh_client,
            host_ip,
            f'cd ~/icicle && git fetch'
            f' && git checkout {branch}'
            f' && git pull origin {branch}',
            timeout=120,
            logger=logger,
        )
    else:
        ec, _, _ = ssh_exec(
            ssh_client,
            host_ip,
            f'cd ~ && git clone {repo_url}',
            timeout=120,
            logger=logger,
        )
        if ec != 0:
            if logger:
                logger.error('Clone failed')
            return False
        ssh_exec(
            ssh_client,
            host_ip,
            f'cd ~/icicle && git checkout {branch}',
            logger=logger,
        )

    ec, _, _ = ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~/icicle && python{python_version} -m venv .venv',
        timeout=60,
        logger=logger,
    )
    if ec != 0:
        if logger:
            logger.error('venv creation failed')
        return False

    ssh_exec(
        ssh_client,
        host_ip,
        "echo 'source ~/icicle/.venv/bin/activate' >> ~/.bashrc",
        logger=logger,
    )

    ec, _, _ = ssh_exec(
        ssh_client,
        host_ip,
        "cd ~/icicle && source .venv/bin/activate && pip install -e .'[dev]'",
        timeout=600,
        logger=logger,
    )
    if ec != 0:
        if logger:
            logger.error('pip install failed')
        return False

    return True


# ============================================================================
# Filebench
# ============================================================================


def install_filebench(
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    version: str,
    url: str,
    logger: logging.Logger | None = None,
) -> bool:
    """Download, compile, and install Filebench. Returns True on success."""
    if logger:
        logger.info(f'Installing Filebench {version}')
    ssh_exec(ssh_client, host_ip, f'cd ~ && wget {url}', timeout=60)
    ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~ && tar -xzf filebench-{version}.tar.gz',
    )
    ssh_exec(ssh_client, host_ip, f'cd ~ && rm filebench-{version}.tar.gz')
    ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~/filebench-{version} && ./configure',
        timeout=60,
    )
    ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~/filebench-{version} && make',
        timeout=150,
    )
    ec, _, _ = ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~/filebench-{version} && sudo make install',
        timeout=60,
    )
    if ec != 0:
        if logger:
            logger.error('Filebench install failed')
        return False
    ssh_exec(ssh_client, host_ip, 'filebench -h || which filebench')
    if logger:
        logger.info(f'Filebench {version} installed')
    return True


# ============================================================================
# GPFS Setup
# ============================================================================


def install_gpfs_from_s3(
    ssh_client: paramiko.SSHClient,
    host_ip: str,
    s3_path: str,
    logger: logging.Logger | None = None,
) -> bool:
    """Download GPFS installer from S3, verify MD5, and run silent install.

    Args:
        ssh_client: Paramiko SSH client.
        host_ip: Host IP address.
        s3_path: Full S3 URI, e.g.
            "s3://bucket/Storage_Scale_Developer-5.2.3.2-x86_64-Linux.zip"
        logger: Optional logger instance.

    Returns True on success.
    """
    filename = s3_path.rsplit('/', 1)[-1]
    base = filename.replace('.zip', '')

    if logger:
        logger.info(f'Downloading GPFS from {s3_path}')
    ec, _, err = ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~ && aws s3 cp {s3_path} .',
        timeout=300,
        logger=logger,
    )
    if ec != 0:
        if logger:
            logger.error(f'GPFS download failed: {err}')
        return False

    if logger:
        logger.info('Extracting and verifying GPFS installer')
    ssh_exec(ssh_client, host_ip, f'cd ~ && unzip -qo {filename}')
    ssh_exec(ssh_client, host_ip, f'cd ~ && rm {filename}')
    ssh_exec(ssh_client, host_ip, f'cd ~ && md5sum -c {base}-install.md5')

    if logger:
        logger.info('Running GPFS silent installer')
    ssh_exec(ssh_client, host_ip, f'cd ~ && chmod +x {base}-install')
    ec, _, err = ssh_exec(
        ssh_client,
        host_ip,
        f'cd ~ && sudo ./{base}-install --silent',
        timeout=300,
        logger=logger,
    )
    if ec != 0:
        if logger:
            logger.error(f'GPFS install failed: {err}')
        return False

    if logger:
        logger.info('GPFS installed successfully')
    return True


def setup_root_ssh_and_exchange_keys(
    nodes: list[tuple[paramiko.SSHClient, str, str]],
    logger: logging.Logger | None = None,
) -> None:
    """Configure root SSH and exchange keys across all nodes.

    Args:
        nodes: list of (ssh_client, host_ip, private_dns) tuples.
        logger: Optional logger instance.

    For each node: enables PermitRootLogin, generates ed25519 root key.
    Then exchanges all public keys so root can SSH between any pair.
    Finally tests bidirectional connectivity.
    """
    if logger:
        logger.info(
            f'Setting up root SSH across {len(nodes)} nodes',
        )

    # Phase 1: Configure root SSH and generate keys on each node
    for ssh, ip, _dns in nodes:
        ssh_exec(
            ssh,
            ip,
            'sudo sed -i -E'
            " 's/^#?PermitRootLogin.*/PermitRootLogin prohibit-password/'"
            ' /etc/ssh/sshd_config',
            logger=logger,
        )
        ssh_exec(
            ssh,
            ip,
            'sudo sed -i -E'
            " 's/^#?PubkeyAuthentication.*/PubkeyAuthentication yes/'"
            ' /etc/ssh/sshd_config',
            logger=logger,
        )
        ssh_exec(
            ssh,
            ip,
            'sudo systemctl reload ssh || sudo systemctl reload sshd',
            logger=logger,
        )
        ssh_exec(
            ssh,
            ip,
            'sudo install -d -m 700 /root/.ssh',
            logger=logger,
        )
        # Only generate key if it doesn't already exist (avoids
        # overwrite prompt that hangs non-interactive SSH)
        ssh_exec(
            ssh,
            ip,
            'sudo test -f /root/.ssh/id_ed25519'
            " || sudo ssh-keygen -t ed25519 -N ''"
            ' -f /root/.ssh/id_ed25519 -q',
            logger=logger,
        )
        ssh_exec(
            ssh,
            ip,
            'sudo bash -c'
            " 'cat /root/.ssh/id_ed25519.pub"
            " >> /root/.ssh/authorized_keys'",
            logger=logger,
        )
        ssh_exec(
            ssh,
            ip,
            'sudo chmod 600 /root/.ssh/authorized_keys',
            logger=logger,
        )

    # Phase 2: Collect all public keys
    pubkeys: list[str] = []
    for ssh, ip, _dns in nodes:
        _, key, _ = ssh_exec(
            ssh,
            ip,
            'sudo cat /root/.ssh/id_ed25519.pub',
        )
        pubkeys.append(key.strip())

    # Phase 3: Distribute all keys to all nodes
    for ssh, ip, _dns in nodes:
        for key in pubkeys:
            ssh_exec(
                ssh,
                ip,
                f"echo '{key}' | sudo tee -a /root/.ssh/authorized_keys",
            )
        # Deduplicate authorized_keys
        ssh_exec(
            ssh,
            ip,
            "sudo bash -c 'sort -u /root/.ssh/authorized_keys"
            ' > /tmp/ak && mv /tmp/ak /root/.ssh/authorized_keys'
            " && chmod 600 /root/.ssh/authorized_keys'",
        )

    # Phase 4: Test connectivity between all pairs
    for ssh_src, ip_src, _dns_src in nodes:
        for _ssh_dst, _ip_dst, dns_dst in nodes:
            ec, _, _ = ssh_exec(
                ssh_src,
                ip_src,
                f'sudo ssh -o StrictHostKeyChecking=no'
                f" root@{dns_dst} 'hostname -f'",
                logger=logger,
            )
            if ec != 0 and logger:
                logger.warning(
                    f'Root SSH from {ip_src} to {dns_dst} failed',
                )

    if logger:
        logger.info('Root SSH setup completed')


# ============================================================================
# Parallel Execution
# ============================================================================


def run_parallel(
    tasks: list[tuple[str, callable]],
    max_workers: int | None = None,
    logger: logging.Logger | None = None,
) -> dict:
    """Run tasks in parallel using threads.

    Args:
        tasks: list of (label, callable) tuples. Each callable takes no args.
        max_workers: max threads (default: len(tasks)).
        logger: optional logger.

    Returns:
        dict mapping label -> result.

    Raises:
        RuntimeError: if any task fails, with details of all failures.
    """
    if max_workers is None:
        max_workers = len(tasks)

    results = {}
    errors = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_label = {executor.submit(fn): label for label, fn in tasks}
        for future in as_completed(future_to_label):
            label = future_to_label[future]
            try:
                results[label] = future.result()
                if logger:
                    logger.info(f'[parallel] {label} completed')
            except Exception as e:
                errors[label] = e
                if logger:
                    logger.error(f'[parallel] {label} failed: {e}')

    if errors:
        detail = '; '.join(f'{k}: {v}' for k, v in errors.items())
        raise RuntimeError(f'Parallel tasks failed: {detail}')

    return results
