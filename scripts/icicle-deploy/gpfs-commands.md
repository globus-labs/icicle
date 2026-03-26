# GPFS (IBM Storage Scale) Commands

All commands run from a GPFS server or client node. Most require `sudo`.
GPFS binaries are in `/usr/lpp/mmfs/bin/`.

## Cluster Status

```bash
# Cluster state of all nodes
sudo mmgetstate -aL

# List cluster nodes
sudo mmlscluster

# Node health
sudo mmhealth node show

# Network verification
sudo mmnetverify
```

## Filesystem Status

```bash
# List filesystems
sudo mmlsfs all

# Show filesystem details
sudo mmlsfs fs1

# Filesystem mount status across nodes
sudo mmlsmount fs1

# Disk/NSD layout
sudo mmlsdisk fs1 -L

# List NSDs
sudo mmlsnsd -m

# Disk usage
df -h /ibm/fs1
```

## Fileset Operations

Fileset creation is handled by the deployment notebooks (`deploy-gpfs-new.ipynb`).
Use the following commands to verify creation and manage filesets:

```bash
# List filesets
sudo mmlsfileset fs1

# Show fileset details (requires -d for disk usage, absolute junction path)
sudo mmlsfileset fs1 -J /ibm/fs1/fset1 -d --block-size auto

# Verify fileset junction path
ls -ld /ibm/fs1/fset1

# Unlink fileset
sudo mmunlinkfileset fs1 fset1

# Delete fileset
sudo mmdelfileset fs1 fset1
```

## File Operations

```bash
# List file attributes (block size, replication, etc.)
sudo mmlsattr /ibm/fs1/fset1/somefile

# Show file placement policy
sudo mmlsattr -L /ibm/fs1/fset1/somefile

# List files in a fileset
ls -la /ibm/fs1/fset1/
```

## Watch / File Audit Logging

```bash
# List all watches
sudo mmwatch all status

# Enable a watch for Kafka
sudo mmwatch fs1 enable \
  --fileset fset1 \
  --events ALL \
  --event-handler kafkasink \
  --sink-brokers KAFKA_IP:9092 \
  --sink-topic fset1-topic-1p \
  --description 'Watch fset1'

# List watch details
sudo mmwatch fs1 list

# Disable a watch
sudo mmwatch fs1 disable --watch-id WATCH_ID

# Delete a watch
sudo mmwatch fs1 delete --watch-id WATCH_ID
```

## Node Management

```bash
# Add a node to the cluster
sudo mmaddnode -N hostname

# Accept license (server or client)
sudo mmchlicense server --accept -N hostname
sudo mmchlicense client --accept -N hostname

# Start GPFS on a node
sudo mmstartup -N hostname

# Shut down GPFS on a node
sudo mmshutdown -N hostname

# Set quorum
sudo mmchnode --quorum -N hostname

# Mount filesystem on a node
sudo mmmount fs1 -N hostname

# Unmount filesystem on a node
sudo mmumount fs1 -N hostname
```

## NSD (Network Shared Disk) Management

```bash
# Create NSD from a stanza file
sudo mmcrnsd -F /tmp/nsd.stanza

# Add disk to filesystem
sudo mmadddisk fs1 -F /tmp/adddisk.stanza

# NSD stanza format:
# %nsd: device=/dev/nvme1n1 nsd=nsd2 servers=hostname usage=dataAndMetadata pool=system failureGroup=2
```

## Troubleshooting

```bash
# Check GPFS daemon status
sudo systemctl status gpfs

# View GPFS logs
sudo tail -100 /var/mmfs/gen/mmfslog

# Rebuild kernel module (after kernel upgrade)
sudo /usr/lpp/mmfs/bin/mmbuildgpl

# Force start GPFS daemons
sudo mmstartup -a
```

## Permissions

```bash
# Create shared group
sudo groupadd icicle-users
sudo usermod -aG icicle-users ubuntu

# Set group ownership and setgid on fileset
sudo chgrp -R icicle-users /ibm/fs1/fset1
sudo chmod 2775 /ibm/fs1/fset1
```
