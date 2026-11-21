# Three-node gate on Firecracker microVMs

`scripts/yadb-microvm.py` runs three `yadbd` daemons in three Firecracker VMs on
one host bridge. It exists because a container cannot show two things the
deployment gate needs:

* **the guest's volatile memory disappears.** A container is a process on the
  host; `SIGKILL` leaves its dirty pages in the host page cache, so an
  acknowledged-but-unflushed write survives. A killed VMM loses the guest RAM
  and the guest page cache with it, so an acknowledgement that was not pushed to
  the block device is genuinely gone.
* **each node has its own kernel and TCP/IP stack.** Peer traffic crosses a tap
  device and a bridge instead of a userspace relay on loopback, and `ip link set
  ... down` is a real partition rather than a closed socket.

It is still one physical machine. A host panic, a host disk failure, a host
power-supply loss and physical independence of the nodes are **out of scope**
and the harness says so in its output. The backing file's own durability
depends on Firecracker honouring the guest's flush; the harness sets
`cache_type: Writeback`, the only mode that fsyncs the backing file on a guest
flush.

## One-time preparation

The harness expects `$YADB_VMSTORE` (default `/home/flykker/vmstore`) to hold:

| Path | What it is |
| --- | --- |
| `vmlinux-6.18.62` | guest kernel with `virtio_mmio`, `ext4`, `devtmpfs`, `overlayfs` built in |
| `root/` | unpacked Ubuntu 24.04 cloud-image root filesystem |
| `root/init` | installed from `scripts/microvm-init.sh` by the harness |
| `root/usr/local/bin/yadbd` | the daemon to test, built with `cargo build -p yadbd` |
| `root/bin/busybox` | static busybox, so `init` needs no dynamic linker |
| `root/var/yadb/` | empty mount point for the per-node writable disk |

The image is mounted read-only, so `/var/yadb` has to exist in it before the
image is built. `rootfs.ext4` is assembled from `root/` by the harness and
rebuilt automatically when `root/init` or `root/usr/local/bin/yadbd` is newer
than it.

`sudo` must work without a password: tap devices, the bridge and Firecracker
need `CAP_NET_ADMIN` or `/dev/kvm`, and the harness re-execs itself under
`sudo` so the PID it records is the VMM's own.

## Running it

```sh
cargo build --offline -j 12 -p yadbd
install -m 0755 target/debug/yadbd "$YADB_VMSTORE/root/usr/local/bin/yadbd"
python3 scripts/yadb-microvm.py --port-base 23600
```

Options: `--only up|services|powercut|lose|all` (default `all`), `--keep` to
keep the run directory, `--rebuild-rootfs` to force a rebuild.

The run directory holds one console log per node (`console-node-N.log`), the
generated VM configs and one writable disk image per node. `--keep` prints its
path; the images are 768 MiB each and live under `$YADB_TEST_TMPDIR` (default
`/tmp/opencode`), so remove the directory when the evidence is no longer needed.

## What each scenario claims

* `up` — every group elects a leader on every node, `examples/platform.yaml`
  applies, and every replica applies its complete log. The guest also reports
  the structured `readiness` object over `/healthz` and `/openraft/status`.
* `services` — one SQL insert, one block write, one pgwire insert and one S3
  object are acknowledged through a node that is *not* the project group's
  leader, then read back byte-exactly through all three nodes. S3 goes over the
  real `--s3-listen` port with SigV4, not the internal `/s3/...` route, and the
  database is read over the real `--pg-listen` wire, not the HTTP query route.
* `powercut` — all three VMMs are `SIGKILL`ed, then all three are restarted
  from their own disks. There is no peer left to copy from, so the pre-cut SQL
  row, block range, pgwire row and object can only come off each node's own
  backing file.
  The applied index of every group must not go backwards.
* `lose` — one VMM is killed, the survivors must elect leaders for every group
  and keep acknowledging SQL, block and S3 writes, and the killed node must
  restart from its disk, catch up and read the writes made while it was down.

## Limits worth restating in any report

* The host page cache and the host disk are untouched by a VMM kill. A `SIGKILL`
  of Firecracker is a power cut **at the guest level**.
* All three nodes share one CPU, one RAM budget and one physical disk, so this
  proves the protocol tolerates one dead node, not that the nodes are
  independent failure domains.
* No external fencing authority is configured, and the harness does not test
  one.
* The daemon's `readiness` object names exactly these limits; do not quote a
  green run without them.
