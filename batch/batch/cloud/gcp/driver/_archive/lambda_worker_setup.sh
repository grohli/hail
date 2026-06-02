#!/bin/bash

# Lambda Labs Batch Worker Setup Script
# This script transforms a bare Lambda Labs Ubuntu VM into a Hail Batch Worker
# Excludes GCP-specific components (gcsfuse, gcloud auth, etc.)

set -ex

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[LAMBDA-SETUP]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[LAMBDA-SETUP]${NC} $1"
}

error() {
    echo -e "${RED}[LAMBDA-SETUP]${NC} $1"
}

log "Starting Lambda Labs Batch Worker setup..."

# Update system packages
log "Updating system packages..."
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get upgrade -y

# Install basic system dependencies (from Dockerfile.worker)
log "Installing basic system packages..."
apt-get install -y \
    iproute2 \
    iptables \
    ca-certificates-java \
    openjdk-11-jre-headless \
    liblapack3 \
    xfsprogs \
    libyajl-dev \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates \
    software-properties-common \
    build-essential \
    pkg-config \
    make \
    git \
    gcc \
    libtool \
    libsystemd-dev \
    libcap-dev \
    libseccomp-dev \
    autoconf \
    automake \
    jq \
    rsync

# Set up Python 3.9 as default
log "Configuring Python environment..."
update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1

# Install pip if not present
if ! command -v pip3 &> /dev/null; then
    log "Installing pip..."
    curl https://bootstrap.pypa.io/get-pip.py | python3
fi

# Upgrade pip to specific version (from hail-ubuntu base)
python3 -m pip install 'pip>=23,<24.1'

# Install Docker
log "Installing Docker..."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io

# Start Docker service
systemctl start docker
systemctl enable docker

# Add ubuntu user to docker group (for Lambda Labs VMs)
usermod -aG docker ubuntu

# Install NVIDIA Container Toolkit (for GPU support)
log "Installing NVIDIA Container Toolkit..."
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -s -L https://nvidia.github.io/libnvidia-container/ubuntu22.04/libnvidia-container.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

apt-get update
apt-get install -y nvidia-container-toolkit

# Configure Docker for NVIDIA runtime
nvidia-ctk runtime configure --runtime=docker
systemctl restart docker

# Create necessary directories (from Dockerfile.worker)
log "Creating required directories..."
mkdir -p /batch
mkdir -p /logs
mkdir -p /global-config
mkdir -p /deploy-config
mkdir -p /cloudfuse
mkdir -p /etc/netns
mkdir -p /host
mkdir -p /batch/jvm-container-logs/

# Set up PySpark environment
log "Setting up Spark environment..."
python3 -m pip install pyspark==3.5.0

# Set Spark environment variables
export SPARK_HOME="/usr/local/lib/python3.9/dist-packages/pyspark"
echo "export SPARK_HOME=/usr/local/lib/python3.9/dist-packages/pyspark" >> /etc/environment
echo "export PATH=\$PATH:\$SPARK_HOME/sbin:\$SPARK_HOME/bin" >> /etc/environment
echo "export PYSPARK_PYTHON=python3" >> /etc/environment

# Build and install crun (container runtime from Dockerfile.worker)
log "Building crun container runtime..."
cd /tmp
git clone --depth 1 --branch 1.4.4 https://github.com/containers/crun.git
cd crun
./autogen.sh
./configure
make
make install

# Install async-profiler (Java profiling tool)
log "Installing async-profiler..."
cd /opt
curl -L https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz | tar -zxvf -

# Create core Spark configuration
log "Creating Spark configuration..."
cat > ${SPARK_HOME}/conf/core-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.AbstractFileSystem.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
  </property>
  <property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
  </property>
  <property>
    <name>fs.gs.auth.service.account.enable</name>
    <value>true</value>
  </property>
</configuration>
EOF

# Install essential Python packages for Hail batch worker
log "Installing Python dependencies..."
# Note: PyJWT, cryptography, psutil, nest_asyncio are already pre-installed on Lambda Labs VMs
python3 -m pip install \
    aiofiles \
    aiohttp \
    aiodns \
    humanfriendly \
    python-json-logger \
    uvloop \
    orjson \
    kubernetes_asyncio \
    prometheus_client \
    tabulate

# Set up basic iptables rules (from GCP startup script)
log "Configuring network rules..."
# Private job network = 172.20.0.0/16
# Public job network = 172.21.0.0/16
iptables --table nat --append POSTROUTING --source 172.20.0.0/15 --jump MASQUERADE

# Allow traffic from private job network
iptables --append FORWARD --source 172.20.0.0/16 --jump ACCEPT

# Get the main network interface for Lambda Labs VMs
INTERNET_INTERFACE=$(ip link list | grep -E "en[sop]|eth" | head -1 | awk -F": " '{print $2}')
if [ -n "$INTERNET_INTERFACE" ]; then
    log "Configuring internet access via interface: $INTERNET_INTERFACE"
    iptables --append FORWARD --out-interface $INTERNET_INTERFACE ! --destination 10.0.0.0/8 --jump ACCEPT
fi

# Create a basic global config directory structure
log "Setting up configuration directories..."
echo "lambda" > /global-config/cloud
echo "lambda-worker" > /global-config/default_namespace

# Create a basic deploy config
cat > /deploy-config/deploy-config.json << 'EOF'
{
  "location": "lambda",
  "cloud": "lambda",
  "domain": "lambda.local"
}
EOF

# Set permissions
chown -R ubuntu:ubuntu /batch /logs /global-config /deploy-config /cloudfuse

# Clean up
log "Cleaning up..."
apt-get autoremove -y
apt-get autoclean
rm -rf /var/lib/apt/lists/*
rm -rf /tmp/crun

# Create a status file to indicate setup completion
echo "$(date): Lambda Labs Batch Worker setup completed successfully" > /batch/setup-complete.txt
chown ubuntu:ubuntu /batch/setup-complete.txt

log "Lambda Labs Batch Worker setup completed successfully!"
log "Key components installed:"
log "  - Docker with NVIDIA runtime support"
log "  - Python 3.9 with essential packages"
log "  - Java 11 + PySpark 3.5.0"
log "  - crun container runtime"
log "  - Basic networking configuration"
log "  - Required directory structure"

warn "Note: This setup excludes GCP-specific components (gcsfuse, gcloud auth)"
warn "Additional Hail-specific packages (hailtop, gear, batch) need to be installed separately"

log "Setup script execution completed. VM is ready for batch worker configuration."

log "Installing Hail packages..."

# Option 1: Install from source (if you have the code)
cd /path/to/hail/source
python3 -m pip install ./hail/python/hailtop
python3 -m pip install ./gear
python3 -m pip install ./batch

# Option 2: Install from PyPI (if available)

python3 -m pip install hailtop

# Option 3: Install from git repository
python3 -m pip install git+https://github.com/hail-is/hail.git@main#subdirectory=hail/python
