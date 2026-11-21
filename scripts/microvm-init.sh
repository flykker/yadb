#!/bin/sh
# Firecracker guest init for the yadb multi-node gate.
#
# Deliberately minimal and dependency-free: mount the pseudo filesystems, bring
# up the one virtio NIC from the kernel command line, mount the per-VM writable
# disk at /var/yadb and hand over to the start script that disk carries. There is
# no systemd, no network manager and no DHCP: the address is fixed by the
# harness so a node's identity is never guessed.

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

mount -t proc     proc     /proc
mount -t sysfs    sysfs    /sys
mount -t devtmpfs devtmpfs /dev
mkdir -p /dev/pts /dev/shm
mount -t devpts   devpts   /dev/pts
mount -t tmpfs    tmpfs    /dev/shm
mount -t tmpfs    tmpfs    /tmp
mount -t tmpfs    tmpfs    /run

hostname "$(sed -n 's/.*yadb\.host=\([^ ]*\).*/\1/p' /proc/cmdline)"

IP="$(sed -n 's/.*yadb\.ip=\([^ ]*\).*/\1/p' /proc/cmdline)"
GW="$(sed -n 's/.*yadb\.gw=\([^ ]*\).*/\1/p' /proc/cmdline)"
if [ -n "$IP" ]; then
    ip link set lo up
    ip link set eth0 up
    ip addr add "$IP/24" dev eth0
    [ -n "$GW" ] && ip route add default via "$GW"
fi

# The writable disk carries the Raft config, the credentials, the data root and
# the start script, so one read-only rootfs can serve every node. Only /dev/vdb
# is ever tried: /dev/vda is the read-only root and mounting it here would put
# the config on the image instead of the node's own disk. `/var/yadb` is baked
# into the image as an empty mount point because the root is mounted read-only.
if ! mount -t ext4 -o rw /dev/vdb /var/yadb; then
    echo "yadb-init: /dev/vdb did not mount at /var/yadb" >&2
    exec sh
fi

if [ ! -x /var/yadb/start.sh ]; then
    echo "yadb-init: no /var/yadb/start.sh on the writable disk" >&2
    exec sh
fi

# Re-exec under a supervisor loop so a daemon exit is visible in the console
# instead of the guest silently powering off. A harness kill of the VMM process
# is a power cut and never reaches this path.
/var/yadb/start.sh
echo "yadb-init: start.sh exited with $?" >&2
exec sh
