#!/usr/bin/env python3
"""Offline protocol integration tests using independent stdlib wire clients.

Run after cargo build --offline: python3 tests/protocols.py
Add --native-clients for psql/AWS CLI, --libiscsi-dir for libiscsi utilities,
and --libiscsi-probe for the compiled libiscsi_probe.c. Not a kernel multipath test.

The cluster is an OpenRaft Multi-Raft deployment: three nodes, each running a
control group (1) and two data groups (2 and 3), with the project route map
`{"1": 2, "2": 3}`. Credentials, TLS material, group configs and manifests are
provisioned by `scripts/yadb-openraft-dev.py`; only the listener ports are
re-derived here, because this harness keeps the documented fixed bases instead
of the launcher's offset scheme (the launcher's Raft block would collide with
the pgwire block). Each node's group config names the *proxy* address of every
peer, so every outbound Raft RPC of a directed link crosses a proxy that a test
can cut, and no test decodes a transport frame.

`--metadata-runtime openraft` serves default reads from the node's own applied
state ("reads are local-applied" in `/openraft/status`). A read on a node other
than the project-group leader can therefore lag a commit; assertions that need
read-your-writes poll with `eventually` instead of racing the apply loop.

Unavailable capabilities are reported as `SKIP <scenario>: <reason>` instead
of being turned into a pass: reader leases are legacy-gated, and Multi-Raft
repair and collection are disabled. A live cross-process `InstallSnapshot` at
the HTTP level is opt-in through `--install-snapshot-live`: reaching the
daemon's shipped snapshot trigger costs a burst of committed entries measured in
hours on this host, so the default run reports a SKIP naming that cost, and an
unfinished burst reports a SKIP with the entries it did commit. The install path
itself is proven by the crate-level process test
`three_process_mtls_failover_and_quorum_loss`. The disabled GC and scrub routes
are tested for refusal and fragment preservation, not collection. A skipped
scenario is still a non-zero exit if anything else fails.
"""
import argparse
import datetime
from collections import deque
import hashlib
import hmac
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import select
import socket
import socketserver
import struct
import subprocess
import tempfile
import time
import threading
import urllib.parse
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
SUBJECT = "sa:billing-backend"
# The only subject on every daemon's --admin-subject allowlist.
ADMIN = "local-operator"
# Both are generated per run by the provisioning helper.
SECRET = ""
RAFT_TOKEN = ""
# Separate from the protocol user secret: the daemon verifies signed subjects
# (and `/openraft/status`, `/v1/apply`, scrub and GC) with this one.
SUBJECT_SECRET = ""

# Documented fixed listener bases; node i listens on base + i.
HTTP_BASE = 19000
PG_BASE = 19100
S3_BASE = 19200
ISCSI_BASE = 19300
# One response-dropping HTTP proxy per node, in front of its S3 listener.
HTTP_PROXY_BASE = 19400
# Directed Raft transport proxy: base + (group-1)*100 + source*10 + target.
RAFT_PROXY_BASE = 19500
# Real Raft listener: base + (group-1)*10 + node.
RAFT_BASE = 19800

NODES = (1, 2, 3)
GROUPS = (1, 2, 3)
CONTROL_GROUP = 1
PROJECT_GROUPS = {"1": 2, "2": 3}
# billing is project id 1 and therefore owned by data group 2.
BILLING_GROUP = PROJECT_GROUPS["1"]

# Forcing a cross-process InstallSnapshot without a crates/ change relies on
# openraft 0.9.25's defaults (Config::default in the daemon): snapshot_policy
# `since_last:5000`, max_in_snapshot_log_to_keep 1000 and
# replication_lag_threshold 5000. The policy triggers once the committed index
# reaches `snapshot_last_log + 5000`, and this run has no snapshot in the billing
# group yet, so the leader builds one at about log index 5000 and purges the
# prefix below index 4000. A node stopped at index ~940 can then only be brought
# up to date by a snapshot. The suite has already committed about 940 entries in
# this group when the scenario starts, so 5000 more entries is the floor that
# reaches the trigger: 5500 was not "1000 past the trigger", it was 500 past it.
SNAPSHOT_BURST_ENTRIES = 5000
# What that burst costs, measured on this build and repeated in the SKIP reason,
# --help and the docs. The rate is not constant, and the decay is the point: on
# one uninterrupted run the first 1254 entries took 519 s (2.4 entries/s), the
# next 1253 took 2196 s (0.6 entries/s), and a direct probe of the leader's
# last_log_index then measured 0.29 entries/s. Each applied entry persists a
# state delta that grows with the log, so the tail is the expensive part and the
# scenario cannot be timed from the head of the burst.
SNAPSHOT_BURST_COST = (
    "2.4 entries/s for the first 1250 entries, 0.6 for the next 1250, and 0.29 "
    "measured after that, as the persisted state delta grows: 59 min for 2507 "
    "entries and hours for the tail")
# The scenario's own output says where the install path is really proven, so a
# reader does not mistake this for the primary evidence.
INSTALL_PROOF_ELSEWHERE = (
    "the install path itself is proven by the crate-level process test "
    "three_process_mtls_failover_and_quorum_loss in yadb-metakv, which drives "
    "three child processes over mTLS, calls raft.trigger().snapshot(), waits for "
    "the leader to purge past the offline node's index and then asserts that "
    "node's snapshot count and every value after restart; this scenario would "
    "add HTTP-level coverage only, and no PASS of it has been observed on this "
    "host")
# The burst writes as the project member that examples/platform.yaml grants
# ReadWrite on billing, because the daemon's query route refuses the admin
# subject for data writes.
BURST_SUBJECT = "sa:billing-backend"


def http_port(node):
    return HTTP_BASE + node


def pg_port(node):
    return PG_BASE + node


def s3_port(node):
    return S3_BASE + node


def iscsi_port(node):
    return ISCSI_BASE + node


def raft_port(group, node):
    return RAFT_BASE + (group - 1) * 10 + node


def raft_proxy_port(group, source, target):
    return RAFT_PROXY_BASE + (group - 1) * 100 + source * 10 + target


def _load_dev_module():
    """Import the OpenRaft launcher's provisioning helpers.

    The file name contains a hyphen, so it cannot be imported normally. The
    module only defines constants and functions; importing it has no side
    effects and reuses the exact credential, certificate and manifest format.
    """
    path = ROOT / "scripts" / "yadb-openraft-dev.py"
    spec = importlib.util.spec_from_file_location("yadb_openraft_dev", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DEV = _load_dev_module()


class TransportProxy(socketserver.ThreadingTCPServer):
    """One directed Raft link (source -> target) of one group.

    The source node's group config names this address as the peer, so every
    outbound RPC of that direction crosses it. `blocked` refuses new
    connections and closes the ones in flight, which is what a partition looks
    like to OpenRaft: the link is gone rather than merely slow.
    """
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, group, source, target):
        self.group = group
        self.source = source
        self.target = target
        self.blocked = threading.Event()
        self.events = deque(maxlen=40)
        self.events_lock = threading.Lock()
        self.active = 0
        self.active_lock = threading.Lock()
        super().__init__(("127.0.0.1", raft_proxy_port(group, source, target)),
                         TransportHandler)

    @property
    def label(self):
        return f"g{self.group} {self.source}->{self.target}"

    def record(self, event):
        with self.events_lock:
            self.events.append((round(time.monotonic(), 3), event))

    def enter(self):
        with self.active_lock:
            self.active += 1

    def leave(self):
        with self.active_lock:
            self.active -= 1

    def settle(self, timeout=10):
        """Wait until no relay is in flight, so a cut is complete."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.active_lock:
                if self.active == 0:
                    return
            time.sleep(0.01)
        raise RuntimeError(f"transport proxy {self.label} still relays a connection")


class TransportHandler(socketserver.BaseRequestHandler):
    def handle(self):
        server = self.server
        server.enter()
        try:
            # Account for a handler before checking the cut: settle() must not
            # miss a relay that passed the check but has not yet been counted.
            if server.blocked.is_set():
                server.record("refused while blocked")
                return
            try:
                upstream = socket.create_connection(
                    ("127.0.0.1", raft_port(server.group, server.target)), timeout=5)
            except OSError as error:
                server.record(f"upstream {type(error).__name__}")
                return
            server.record("open")
            sockets = (self.request, upstream)
            try:
                # The relay must not add latency of its own: with Nagle enabled
                # on both legs, a small request/response pair waits for a
                # delayed ACK on either side and every Raft RPC pays tens of
                # milliseconds, which the peers then see as a slow cluster.
                for sock in sockets:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.request.settimeout(0.2)
                upstream.settimeout(0.2)
                while not server.blocked.is_set():
                    ready, _, _ = select.select(sockets, [], [], .05)
                    for sock in ready:
                        data = sock.recv(65536)
                        if not data:
                            return
                        (upstream if sock is self.request else self.request).sendall(data)
            except OSError as error:
                server.record(type(error).__name__)
            finally:
                for sock in sockets:
                    try:
                        sock.close()
                    except OSError:
                        pass
        finally:
            server.leave()


class ResponseDropProxy(socketserver.ThreadingTCPServer):
    """Forward one HTTP request, read the whole upstream response, drop it.

    The mutation reaches the daemon and may commit; the client only sees the
    connection close. That is exactly the ambiguous outcome a client must
    survive without retrying the command.
    """
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 8

    def __init__(self, port, target_port, label):
        self.port = port
        self.target_port = target_port
        self.label = label
        self.upstream_status = None
        self.upstream_body = b""
        self.error = None
        self.observed = threading.Event()
        super().__init__(("127.0.0.1", port), ResponseDropHandler)


def _read_head(stream, limit=128 * 1024):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = stream.recv(65536)
        if not chunk:
            raise EOFError("connection closed before a complete head")
        data += chunk
        assert len(data) <= limit, "oversized HTTP head"
    return data.split(b"\r\n\r\n", 1)


def _content_length(head):
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            return int(value.strip())
    return 0


class ResponseDropHandler(socketserver.BaseRequestHandler):
    def handle(self):
        server = self.server
        try:
            self.request.settimeout(60)
            head, body = _read_head(self.request)
            length = _content_length(head)
            while len(body) < length:
                chunk = self.request.recv(min(65536, length - len(body)))
                if not chunk:
                    return
                body += chunk
            with socket.create_connection(("127.0.0.1", server.target_port),
                                          timeout=60) as upstream:
                upstream.settimeout(60)
                upstream.sendall(head + b"\r\n\r\n" + body)
                reply_head, reply_body = _read_head(upstream)
                status = int(reply_head.split(b"\r\n")[0].split(b" ")[1])
                while len(reply_body) < _content_length(reply_head):
                    chunk = upstream.recv(65536)
                    if not chunk:
                        break
                    reply_body += chunk
                server.upstream_status = status
                server.upstream_body = reply_body
        except (OSError, EOFError, ValueError, IndexError) as error:
            server.error = f"{type(error).__name__}: {error}"
        finally:
            server.observed.set()
            # Not one response byte: the client must see a transport error.
            try:
                self.request.close()
            except OSError:
                pass


def exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise EOFError("connection closed")
        data += chunk
    return data


def request(port, method, path, body=b"", headers=None, timeout=30):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    c.request(method, path, body, headers or {})
    r = c.getresponse()
    status, hs, data = r.status, dict(r.getheaders()), r.read()
    c.close()
    return status, hs, data


def assert_multiraft_gc_refusal(port, path, fragments):
    """Check the HTTP refusal and verify each seeded fragment remains readable."""
    status, _, body = request(port, "POST", path, headers=admin_headers())
    assert status == 409, (path, status, body)
    summary = json.loads(body)
    reason = summary.get("gc_refusal")
    assert isinstance(reason, str) and "mark-and-sweep disabled for Multi-Raft" in reason, summary
    assert summary["fragments_deleted"] == 0 and summary["bytes_reclaimed"] == 0, summary
    if path == "/v1/pools/scrub":
        assert summary["orphan_collection_error"] == "scrub disabled for Multi-Raft", summary
        assert summary["orphan_blobs_collected"] == 0, summary
    for fragment_port, fragment_path, expected in fragments:
        result = request(fragment_port, "GET", fragment_path,
                         headers={"x-yadb-raft-token": RAFT_TOKEN})
        assert result[0] == 200 and result[2] == expected, (fragment_path, result)


def assert_vdisk_fence_rpc(port, pool_name, slot_index):
    """Drive the durable epoch and pin table over the shipped internal surface.

    The handler-level test calls these functions directly, and the volume tests
    run on `SimFs`. This drill goes through the router of a running node, with
    the real token check and a real on-disk epoch, so it proves the routes are
    wired and that a refusal leaves the fragment readable. It does not run a
    collector: none exists yet, and the four collection drills still cannot run.
    """
    internal = {"x-yadb-raft-token": RAFT_TOKEN}
    json_headers = {**internal, "Content-Type": "application/json"}
    base = f"/internal/vdisk/{pool_name}/{slot_index}"

    def get_epoch():
        status, _, body = request(port, "GET", base + "/epoch", headers=internal)
        assert status == 200, ("epoch", status, body)
        return json.loads(body)["epoch"]

    def advance(expected):
        status, _, body = request(
            port, "POST", base + "/advance",
            json.dumps({"expected_epoch": expected}).encode(), json_headers)
        return status, json.loads(body) if body else {}

    def pin(epoch, ttl_ms):
        status, _, body = request(
            port, "POST", base + "/pin",
            json.dumps({"epoch": epoch, "ttl_ms": ttl_ms}).encode(), json_headers)
        return status, json.loads(body) if body else {}

    def unpin(pin_id):
        status, _, body = request(
            port, "POST", base + "/unpin",
            json.dumps({"pin_id": pin_id}).encode(), json_headers)
        return status, json.loads(body) if body else {}

    def live_pins():
        status, _, body = request(port, "GET", base + "/pins", headers=internal)
        assert status == 200, ("pins", status, body)
        return json.loads(body)["pins"]

    def delete(blob_hex, query=""):
        status, _, body = request(
            port, "DELETE", f"{base}/blob/{blob_hex}{query}", headers=internal)
        # A refusal is a JSON envelope and a success is the plain word
        # `deleted`/`absent`; decoding only the former keeps both checkable.
        if body.startswith(b"{"):
            return status, json.loads(body)
        return status, body

    # The shared token is the whole authentication of this surface. Without it
    # the epoch is not readable, so an unauthenticated caller cannot even learn
    # which value to advance from.
    assert request(port, "GET", base + "/epoch")[0] == 401

    start = get_epoch()
    blob = ((0xFFFFFFFD << 96) | (1 << 64) | (1 << 40)
            | ((slot_index + 1) << 16) | (248 << 8) | 1)
    blob_hex = f"{blob:032x}"
    path = f"{base}/blob/{blob_hex}"
    payload = b"fence-drill-fragment"
    assert request(port, "PUT", path, payload, internal)[0] == 200

    # A pin names the target's current epoch or nothing. A pin on the epoch a
    # collector might advance to would prove nothing about bytes already
    # written, so it is refused rather than stored.
    status, body = pin(start + 1, 60_000)
    assert status == 409 and body["error"] == "pin_epoch_not_current", (status, body)
    # A zero or over-long lifetime is refused rather than clamped: a pin that
    # expires under the read it protects is worse than no pin at all.
    assert pin(start, 0)[0] == 400, "zero TTL must be refused"
    assert pin(start, 300_001)[0] == 400, "an over-long TTL must be refused"

    status, grant = pin(start, 60_000)
    assert status == 200 and grant["epoch"] == start, (status, grant)
    assert [p["pin_id"] for p in live_pins()] == [grant["pin_id"]], live_pins()

    # 1. The advance refusal is what protects a fragment the reader did not
    #    write: the collector cannot move past the epoch a reader holds.
    status, body = advance(start)
    assert status == 409 and body["error"] == "epoch_pinned", (status, body)
    # 2. The per-fragment refusal protects the fragment the pin names.
    status, body = delete(blob_hex, f"?collector_epoch={start + 1}")
    assert status == 409 and body["error"] == "fragment_pinned", (status, body)
    # Neither refusal may consume the fragment or move the epoch.
    survived = request(port, "GET", path, headers=internal)
    assert survived[0] == 200 and survived[2] == payload, survived
    assert get_epoch() == start

    status, body = unpin(grant["pin_id"])
    assert (status, body) == (200, {"released": True}), (status, body)
    assert live_pins() == []
    # Releasing again is a no-op, not an error and not a resurrection.
    status, body = unpin(grant["pin_id"])
    assert (status, body) == (200, {"released": False}), (status, body)

    status, body = advance(start)
    assert status == 200 and body["epoch"] == start + 1, (status, body)
    assert get_epoch() == start + 1
    # The advance is a compare-and-swap: the old expectation is now stale.
    status, body = advance(start)
    assert status == 409 and body["error"] == "epoch_mismatch", (status, body)

    # An exact fence must name the fragment's own epoch, and equality is not
    # "older" for a collector. Both are distinct refusals, not a generic one.
    status, body = delete(blob_hex, f"?fence_epoch={start + 1}")
    assert status == 409 and body["error"] == "fenced_by_epoch", (status, body)
    status, body = delete(blob_hex, f"?collector_epoch={start}")
    assert status == 409 and body["error"] == "fragment_not_older", (status, body)
    status, body = delete(blob_hex, f"?fence_epoch={start}&collector_epoch={start + 1}")
    assert status == 400 and body["error"] == "bad_fence_params", (status, body)
    survived = request(port, "GET", path, headers=internal)
    assert survived[0] == 200 and survived[2] == payload, survived

    # Only a strictly newer epoch collects, and a replay is "absent".
    status, body = delete(blob_hex, f"?collector_epoch={start + 1}")
    assert (status, body) == (200, b"deleted"), (status, body)
    assert request(port, "GET", path, headers=internal)[0] == 404
    status, body = delete(blob_hex, f"?collector_epoch={start + 1}")
    assert (status, body) == (200, b"absent"), (status, body)

    # An expired pin stops blocking: the target's own clock, not a caller's
    # promise, is the deadline. Re-attempting the advance is safe because it is
    # a compare-and-swap and a refusal changes nothing.
    # Two seconds, not a hundred milliseconds: the assertion below must observe
    # the pin while it is certainly live, so the TTL cannot expire inside the
    # round trip that proves it blocks the advance.
    status, grant = pin(start + 1, 2_000)
    assert status == 200, (status, grant)
    assert advance(start + 1)[0] == 409, "a live pin must refuse the advance"
    eventually(lambda: advance(start + 1),
               lambda result: result[0] == 200,
               "expired pin stops blocking the advance", timeout=10)
    assert get_epoch() == start + 2
    assert live_pins() == []


def signed(port, method, path, body=b"", extra=None, secret=None, timeout=30):
    secret = SECRET if secret is None else secret
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = now[:8]
    headers = {"host": f"127.0.0.1:{port}", "x-amz-date": now,
               "x-amz-content-sha256": hashlib.sha256(body).hexdigest()}
    # SigV4 signs lowercased header names, and the daemon rejects a
    # `SignedHeaders` entry that is not lowercase with "invalid header name"
    # rather than a signature mismatch. Normalise here so a caller may spell a
    # header the way the wire does (`Idempotency-Key`) without having to know
    # that rule.
    headers.update({name.lower(): value for name, value in (extra or {}).items()})
    names = ";".join(sorted(headers))
    uri, _, query = path.partition("?")
    encode = lambda s: urllib.parse.quote(s, safe="-_.~")
    cp = "/".join(encode(urllib.parse.unquote(s)) for s in uri.split("/"))
    pairs = [(encode(urllib.parse.unquote(k)), encode(urllib.parse.unquote(v)))
             for p in query.split("&") if p for k, _, v in [p.partition("=")]]
    cq = "&".join(f"{k}={v}" for k, v in sorted(pairs))
    ch = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    canonical = f"{method}\n{cp}\n{cq}\n{ch}\n{names}\n{headers['x-amz-content-sha256']}"
    scope = f"{date}/us-east-1/s3/aws4_request"
    text = f"AWS4-HMAC-SHA256\n{now}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = ("AWS4" + secret).encode()
    for v in [date, "us-east-1", "s3", "aws4_request"]:
        key = hmac.new(key, v.encode(), hashlib.sha256).digest()
    sig = hmac.new(key, text.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = f"AWS4-HMAC-SHA256 Credential=backend/{scope}, SignedHeaders={names}, Signature={sig}"
    return request(port, method, path, body, headers, timeout)


def complete_multipart_xml(parts):
    """The `CompleteMultipartUpload` document AWS clients send for `parts`."""
    body = "".join(
        f"<Part><PartNumber>{number}</PartNumber><ETag>&quot;{etag}&quot;</ETag></Part>"
        for number, etag in parts
    )
    return f"<CompleteMultipartUpload>{body}</CompleteMultipartUpload>".encode()


def multipart_etag(parts):
    """S3's multipart ETag: MD5 of the concatenated part digests, then `-N`."""
    digest = hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest()
    return f'"{digest}-{len(parts)}"'


class Pg:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=30)
        startup = struct.pack("!I", 196608) + b"user\0backend\0database\0billing/ledger\0\0"
        self.s.sendall(struct.pack("!I", len(startup)+4)+startup)
        assert self.packet() == (b"R", struct.pack("!I", 3))
        self.send(b"p", SECRET.encode()+b"\0")
        self.until_ready()

    def send(self, k, b):
        self.s.sendall(k + struct.pack("!I", len(b)+4)+b)

    def packet(self):
        kind = exact(self.s, 1)
        n, = struct.unpack("!I", exact(self.s, 4))
        return kind, exact(self.s, n-4)

    def until_ready(self):
        out = []
        while True:
            kind, data = self.packet()
            out.append((kind, data))
            if kind == b"Z":
                return out

    def query(self, sql, fail=False):
        self.send(b"Q", sql.encode()+b"\0")
        out = self.until_ready()
        assert any(k == b"E" for k, _ in out) == fail, out
        return out

    def rows(self, sql):
        return self.data_rows(self.query(sql))

    def data_rows(self, out):
        rows = []
        for k, data in out:
            if k != b"D":
                continue
            n, = struct.unpack("!H", data[:2])
            data = data[2:]
            row = []
            for _ in range(n):
                length, = struct.unpack("!i", data[:4])
                data = data[4:]
                row.append(None if length == -1 else data[:length].decode())
                if length != -1:
                    data = data[length:]
            rows.append(row)
        return rows

    # Extended query protocol framing (text format only).

    def parse(self, sql, name="", oids=()):
        body = name.encode()+b"\0"+sql.encode()+b"\0"+struct.pack("!H", len(oids))
        body += b"".join(struct.pack("!I", oid) for oid in oids)
        self.send(b"P", body)

    def bind(self, portal="", statement="", params=(), formats=(), results=()):
        body = portal.encode()+b"\0"+statement.encode()+b"\0"
        body += struct.pack("!H", len(formats))
        body += b"".join(struct.pack("!H", f) for f in formats)
        body += struct.pack("!H", len(params))
        for value in params:
            if value is None:
                body += struct.pack("!i", -1)
            else:
                raw = value if isinstance(value, bytes) else value.encode()
                body += struct.pack("!i", len(raw))+raw
        body += struct.pack("!H", len(results))
        body += b"".join(struct.pack("!H", f) for f in results)
        self.send(b"B", body)

    def describe(self, target=b"S", name=""):
        self.send(b"D", target+name.encode()+b"\0")

    def execute(self, portal="", max_rows=0):
        self.send(b"E", portal.encode()+b"\0"+struct.pack("!i", max_rows))

    def close_object(self, target=b"S", name=""):
        self.send(b"C", target+name.encode()+b"\0")

    def sync(self):
        self.send(b"S", b"")
        return self.until_ready()

    def copy_in(self, sql, lines):
        """COPY ... FROM STDIN in text format; returns the messages after CopyDone."""
        self.send(b"Q", sql.encode()+b"\0")
        kind, data = self.packet()
        assert kind == b"G", (kind, data)
        for line in lines:
            self.send(b"d", line)
        self.send(b"c", b"")
        return self.until_ready()

    def extended(self, sql, params=(), oids=(), results=()):
        self.parse(sql, oids=oids)
        self.bind(params=params, results=results)
        self.describe(b"P")
        self.execute()
        return self.sync()

    def close(self):
        self.send(b"X", b"")
        self.s.close()


class Iscsi:
    def __init__(self, port, discovery=False, secret=None, negotiation=None):
        secret = SECRET if secret is None else secret
        self.s = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.cmd = 1
        self.itt = 1
        self.login = True
        keys = {"InitiatorName": "iqn.2026-11.test:client", "SessionType": "Discovery" if discovery else "Normal",
                "AuthMethod": "CHAP"}
        if not discovery:
            keys["TargetName"] = "iqn.2026-11.io.yadb:billing:vm-root"
        r, _ = self.exchange(3, 0, self.text(keys), login=True)
        assert r[36] == 0
        r, data = self.exchange(3, 0, b"CHAP_A=5\0", login=True)
        fields = dict(s.split("=", 1) for s in data.decode().split("\0") if "=" in s)
        challenge = bytes.fromhex(fields["CHAP_C"][2:])
        digest = hashlib.md5(bytes([int(fields["CHAP_I"])]) + secret.encode() + challenge).hexdigest()
        r, _ = self.exchange(3, 0x81, self.text({"CHAP_N": "backend", "CHAP_R": "0x"+digest}), login=True)
        if secret != SECRET:
            assert r[36] == 2
            self.s.close()
            return
        assert r[1] == 0x81
        keys = {"HeaderDigest": "None", "DataDigest": "None", "MaxRecvDataSegmentLength": "8192"}
        keys.update(negotiation or {})
        r, data = self.exchange(3, 0x87, self.text(keys), login=True)
        self.negotiated = dict(s.split("=", 1) for s in data.decode().split("\0") if "=" in s)
        assert r[1] == 0x87 and r[36] == 0
        self.login = False

    @staticmethod
    def text(keys):
        return "".join(f"{k}={v}\0" for k, v in keys.items()).encode()

    def send(self, h, data=b""):
        h[5:8] = len(data).to_bytes(3, "big")
        self.s.sendall(h+data+b"\0"*((-len(data)) % 4))

    def recv(self):
        h = exact(self.s, 48)
        n = int.from_bytes(h[5:8], "big")
        data = exact(self.s, n)
        exact(self.s, (-n) % 4)
        return h, data

    def header(self, op, flags, login=False):
        h = bytearray(48)
        h[0] = op | (0x40 if login or op == 4 else 0)
        h[1] = flags
        h[8:14] = b"\x80\x01\x02\x03\x04\x05" if login else bytes(6)
        h[16:20] = self.itt.to_bytes(4, "big")
        h[24:28] = self.cmd.to_bytes(4, "big")
        return h

    def exchange(self, op, flags, data=b"", login=False):
        self.send(self.header(op, flags, login), data)
        return self.recv()

    def scsi(self, cdb, expected=0, write=None, immediate=True):
        self.itt += 1
        h = self.header(1, 0x80 | (0x20 if write is not None else 0x40 if expected else 0) | 1)
        h[20:24] = expected.to_bytes(4, "big")
        h[32:32+len(cdb)] = cdb
        self.send(h, write if write is not None and immediate else b"")
        self.cmd += 1
        data = b""
        while True:
            r, payload = self.recv()
            if r[0] == 0x31:
                assert write is not None
                offset = int.from_bytes(r[40:44], "big")
                length = int.from_bytes(r[44:48], "big")
                d = self.header(5, 0x80)
                d[20:24] = r[20:24]
                d[40:44] = offset.to_bytes(4, "big")
                self.send(d, write[offset:offset+length])
            elif r[0] == 0x25:
                data += payload
                if r[1] & 1:
                    assert r[3] == 0, (r, payload)
                    return data
            else:
                assert r[0] == 0x21 and r[3] == 0, (r, payload)
                return data

    def read(self, lba=0, blocks=1):
        cdb = bytearray(10)
        cdb[0] = 0x28
        cdb[2:6] = lba.to_bytes(4, "big")
        cdb[7:9] = blocks.to_bytes(2, "big")
        return self.scsi(cdb, blocks*512)

    def write(self, data, lba=0, immediate=True):
        cdb = bytearray(10)
        cdb[0] = 0x2a
        cdb[2:6] = lba.to_bytes(4, "big")
        cdb[7:9] = (len(data)//512).to_bytes(2, "big")
        self.scsi(cdb, len(data), data, immediate)


def wait(port, timeout=30):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        try:
            if request(port, "GET", "/healthz")[0] == 200:
                return
        except OSError:
            pass
        time.sleep(.05)
    raise RuntimeError("daemon not ready")


def admin_headers():
    return DEV.signed_headers(SUBJECT_SECRET, ADMIN)


def openraft_status(node, timeout=2):
    status, body = DEV.http_json(http_port(node), "/openraft/status", admin_headers(), timeout)
    if status != 200 or not isinstance(body, dict):
        raise RuntimeError(f"node {node}: /openraft/status HTTP {status}")
    return body


def group_observation(node, group):
    for entry in openraft_status(node).get("local_observations", []):
        if entry.get("group") == group:
            return entry
    raise RuntimeError(f"node {node}: /openraft/status has no observation for group {group}")


def wait_group_leader(group, nodes=NODES, timeout=30):
    """Wait until every listed node names the same elected leader for one group.

    A restarted node binds its listeners before its groups have elected a
    leader, and a request forwarded in that window is refused, which is correct
    cluster behaviour. Multi-Raft elects one leader per group, so this waits for
    that group's election instead of expecting a single cluster-wide leader.
    """
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = [group_observation(node, group) for node in nodes]
            leaders = {entry["leader_id"] for entry in last}
            if len(leaders) == 1 and None not in leaders:
                return leaders.pop()
        except (OSError, RuntimeError):
            last = None
        time.sleep(.05)
    raise RuntimeError(f"group {group}: no leader agreed by nodes {tuple(nodes)}: {last}")


def wait_group_leader_other(group, nodes, exclude, timeout=45):
    """Wait until `nodes` agree on a leader that is not `exclude`.

    `leader_id` keeps naming the old leader until that leader's lease expires,
    so mere agreement is not enough after a leader is cut off: a test that heals
    the links before the peers have replaced it would let the old leader commit
    its pending entry.
    """
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = [group_observation(node, group) for node in nodes]
            leaders = {entry["leader_id"] for entry in last}
            if len(leaders) == 1:
                (found,) = leaders
                if found is not None and found != exclude:
                    return found
        except (OSError, RuntimeError):
            last = None
        time.sleep(.05)
    raise RuntimeError(f"group {group}: nodes {tuple(nodes)} did not replace "
                       f"leader {exclude}: {last}")


def wait_all_leaders(timeout=60):
    """Wait until every node observes a leader for every group."""
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = [[group_observation(node, group) for group in GROUPS]
                    for node in NODES]
            if all(entry["leader_id"] is not None for row in last for entry in row):
                return last
        except (OSError, RuntimeError):
            last = None
        time.sleep(.05)
    raise RuntimeError(f"not every node observed three group leaders: {last}")


SKIPS = []


def skip(scenario, reason):
    """Record a capability this runtime does not expose; never a failure."""
    SKIPS.append((scenario, reason))
    print(f"SKIP {scenario}: {reason}")


def assert_sql_no_ack(result):
    """A worker exception is a test failure, not a SQL refusal."""
    if isinstance(result, BaseException):
        raise result
    assert any(kind == b"E" for kind, _ in result), result
    assert not any(kind == b"C" for kind, _ in result), result


def eventually(read, check, label, timeout=15):
    """Poll a read until `check` accepts it; the runtime guarantees convergence."""
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        last = read()
        if check(last):
            return last
        time.sleep(.02)
    raise AssertionError(f"{label}: not observed within {timeout}s; last={last!r}")


def observation_index(text):
    """The log index in the `last_applied` field of `/openraft/status`.

    The daemon renders `last_applied` with `{:?}`, so it reads `Some(LogId {
    leader_id: ..., index: N })`. Returns None when the node has applied nothing
    yet (`last_applied` is `None`).
    """
    match = re.search(r"index: (\d+)", text or "")
    return int(match.group(1)) if match else None


def snapshot_meta_index(line):
    """The last log index a snapshot covers, from an openraft INFO line.

    `SnapshotMeta` renders as `{snapshot_id: ..., last_log:T1-N2-1234, ...}`:
    `LogId` prints `T<term>-N<node>-<index>`, and the option around it is
    printed through `DisplayOption`, which writes the inner value with no
    `Some(...)` wrapper. The installer's `Done install complete snapshot, meta:
    ...` line prints it that way. An empty snapshot prints `last_log:None` and
    yields None.
    """
    match = re.search(r"last_log:(?:Some\()?(?:T\d+-N\d+-)?(\d+)", line)
    return int(match.group(1)) if match else None


def appended_lines(path, offset):
    """Lines written to `path` after byte `offset`.

    Reading the whole appended region once is cheap next to the burst that
    produces it, and a torn final line cannot match an evidence marker.
    """
    with path.open("rb") as handle:
        handle.seek(offset)
        return handle.read().decode("utf-8", "replace").splitlines()


def log_evidence(path, offset, marker, check, label, timeout=300):
    """Wait for an appended line that contains `marker` and passes `check`.

    The leader that streams a snapshot and the node that installs it both log at
    INFO, so the proof is a process's own output rather than an inference from
    state that happens to match.
    """
    deadline = time.monotonic()+timeout
    while True:
        matched = [line for line in appended_lines(path, offset)
                   if marker in line and check(line)]
        if matched:
            return matched
        assert time.monotonic() < deadline, (
            f"{label}: no `{marker}` line in {path} after byte {offset} "
            f"within {timeout}s")
        time.sleep(.25)


def burst_query(node, sql, local=False, timeout=180):
    """Run one statement on `node` through the daemon's own SQL route.

    The burst and its reads use this route rather than pgwire: measured back to
    back with one node stopped, the same INSERT cost 614 ms here against 963 ms
    through the pgwire bridge, and the burst is long enough that the difference
    decides whether the opt-in scenario is practical. Both routes commit through
    the same engine.

    `local` leaves `force_leader` off, which serves a read from this node's own
    applied state instead of forwarding it to the project-group leader. That is
    the only way to prove a restarted node itself holds the burst rows: a
    forwarded read would be answered by the leader either way.

    A burst of thousands of commits is a load generator, so it retries the two
    ways a loaded host can lose an answer: a transport error, and the 502/503/504
    the daemon returns when its metadata bridge gave up after a fixed six-second
    wait and the write's outcome is unknown. The burst writes with UPSERT, which
    makes such a retry idempotent; every other refusal still fails the scenario,
    and the row count is checked against the statements attempted, so a retry
    that silently lost a row is caught rather than hidden. The wait is bounded by
    `timeout` per statement and backs off, because on a loaded host the retry can
    be answered by the same six-second budget while the earlier write is still
    queued.
    """
    headers = {"Content-Type": "application/json",
               **DEV.signed_headers(SUBJECT_SECRET, BURST_SUBJECT)}
    body = json.dumps({"sql": sql, "force_leader": not local}).encode()
    deadline = time.monotonic()+timeout
    attempt = 0
    while True:
        try:
            status, _, out = request(http_port(node), "POST", "/db/billing/ledger/query",
                                     body, headers, timeout=120)
        except OSError as error:
            last = f"{type(error).__name__}: {error}"
        else:
            if status == 200:
                return json.loads(out)
            last = (status, out[:200])
            if status not in (502, 503, 504):
                break
        assert time.monotonic() < deadline, (sql, last)
        time.sleep(min(2**attempt, 10))
        attempt += 1
    raise AssertionError((sql, last))


def burst_rows(node, local=False):
    """The burst table's count, id bounds and checksum, as pgwire-style strings.

    Stringified so the leader's answer and a restarted node's local answer
    compare equal without depending on which protocol produced them.
    """
    result = burst_query(
        node, "SELECT COUNT(*), MIN(id), MAX(id), SUM(balance) FROM snapshot_burst;",
        local=local)
    return [[None if value is None else str(value) for value in row]
            for row in result["rows"]]


class BurstTimeout(RuntimeError):
    """The burst did not reach its entry target inside its own deadline.

    Distinct from an assertion failure so the opt-in scenario can report an
    unfinished burst as a SKIP with the numbers it did reach, instead of failing
    the suite: on a loaded host this is the expected outcome, and it is evidence
    about cost, not about correctness.
    """

    def __init__(self, written, entries, seconds):
        super().__init__(
            f"burst stopped at {entries} entries after {written} statements "
            f"in {seconds:.0f}s")
        self.written = written
        self.entries = entries
        self.seconds = seconds


def burst_commits(leader, target_entries, timeout=7200):
    """Commit one-row UPSERTs until the billing group's log grew by `target_entries`.

    A one-row auto-commit statement costs about five Raft entries (the row, its
    keys and the group's revision bookkeeping each append), so the burst is
    counted in entries read from `last_log_index` and never in statements. The
    wall time returned is what this build actually paid.

    Rows are what grows the log, not row payload: one statement carrying a
    hundred rows still appended only eight entries, measured on this build, so
    wider statements would make the burst slower, not shorter. `UPSERT` rather
    than `INSERT` is what lets `burst_query` retry an unknown outcome; ids stay
    unique, so the resulting rows are identical to the inserts.

    Progress is printed every 500 entries with the rate and the remaining time at
    that rate: the burst runs for tens of minutes, and its rate falls as the
    persisted state delta grows, so a silent log would hide both a stall and the
    fact that the estimate has moved.
    """
    before = group_observation(leader, BILLING_GROUP)["last_log_index"]
    assert isinstance(before, int), before
    started = time.monotonic()
    written = 0
    reported = 0
    while True:
        for _ in range(250):
            written += 1
            burst_query(leader, f"UPSERT INTO snapshot_burst VALUES ({written},{written});")
        delta = group_observation(leader, BILLING_GROUP)["last_log_index"] - before
        if delta >= target_entries:
            return written, delta, time.monotonic()-started
        if delta >= reported+500:
            reported = delta
            elapsed = time.monotonic()-started
            rate = delta/elapsed
            print(f"  burst: {delta}/{target_entries} entries, {written} statements, "
                  f"{elapsed:.0f}s, {rate:.1f} entries/s, "
                  f"~{(target_entries-delta)/rate/60:.0f} min left at that rate",
                  flush=True)
        if time.monotonic() >= started+timeout:
            raise BurstTimeout(written, delta, time.monotonic()-started)


def s3_visible(port, key, body, extra=None, timeout=15):
    """Default S3 GET on a possibly-lagging follower, until `body` is applied."""
    return eventually(lambda: signed(port, "GET", key, extra=extra),
                      lambda r: r[0] == 200 and r[2] == body,
                      f"S3 GET {key} on {port} == {body!r}", timeout)


def s3_absent(port, key, timeout=15):
    return eventually(lambda: signed(port, "GET", key),
                      lambda r: r[0] == 404,
                      f"S3 GET {key} on {port} == 404", timeout)



def native(command, env=None):
    """Run a real client and include its output in failures."""
    result = subprocess.run(command, text=True, capture_output=True, env=env, timeout=45)
    if result.returncode:
        raise AssertionError(
            f"native client failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    return result.stdout


def native_error(command, env, expected):
    result = subprocess.run(command, text=True, capture_output=True, env=env, timeout=45)
    assert result.returncode != 0 and expected in result.stderr, (result.stdout, result.stderr)


def native_client_checks(temp, pg_port, s3_port):
    pg_env = os.environ.copy()
    pg_env["PGPASSWORD"] = SECRET
    pg_env["PGSSLMODE"] = "prefer"
    pg_env["PGCONNECT_TIMEOUT"] = "10"
    out = native([
        "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1",
        "-h", "127.0.0.1", "-p", str(pg_port), "-U", "backend",
        "-d", "billing/ledger", "-c", "SELECT balance FROM wire WHERE id=1",
    ], pg_env)
    assert out.strip() == "90", repr(out)
    native([
        "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
        "-p", str(pg_port), "-U", "backend", "-d", "billing/ledger",
        "-c", "BEGIN; UPDATE wire SET balance=91 WHERE id=1; COMMIT;",
    ], pg_env)
    assert native([
        "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-h",
        "127.0.0.1", "-p", str(pg_port), "-U", "backend", "-d",
        "billing/ledger", "-c", "SELECT balance FROM wire WHERE id=1",
    ], pg_env).strip() == "91"
    native([
        "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
        "-p", str(pg_port), "-U", "backend", "-d", "billing/ledger",
        "-c", "UPDATE wire SET balance=90 WHERE id=1;",
    ], pg_env)
    bad_pg = pg_env.copy()
    bad_pg["PGPASSWORD"] = "incorrect-secret"
    native_error([
        "psql", "-X", "-h", "127.0.0.1", "-p", str(pg_port), "-U", "backend",
        "-d", "billing/ledger", "-c", "SELECT balance FROM wire",
    ], bad_pg, "invalid credentials")

    aws_env = os.environ.copy()
    aws_env.update({
        "AWS_ACCESS_KEY_ID": "backend",
        "AWS_SECRET_ACCESS_KEY": SECRET,
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_CLI_AUTO_PROMPT": "off",
    })
    config = Path(temp)/"aws-config"
    config.write_text("[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n")
    aws_env["AWS_CONFIG_FILE"] = str(config)
    aws_env["AWS_SHARED_CREDENTIALS_FILE"] = str(Path(temp)/"unused-credentials")
    endpoint = f"http://127.0.0.1:{s3_port}"
    object_path = Path(temp) / "native-aws.txt"
    object_path.write_text("native aws cli\n")
    native([
        "aws", "--endpoint-url", endpoint, "s3api", "put-object",
        "--bucket", "invoices-2026", "--key", "native/aws.txt",
        "--body", str(object_path), "--checksum-algorithm", "CRC32",
        "--no-cli-pager",
    ], aws_env)
    head = native([
        "aws", "--endpoint-url", endpoint, "s3api", "head-object",
        "--bucket", "invoices-2026", "--key", "native/aws.txt",
        "--no-cli-pager",
    ], aws_env)
    assert '"ContentLength": 15' in head, head
    downloaded = Path(temp) / "native-downloaded.txt"
    native([
        "aws", "--endpoint-url", endpoint, "s3api", "get-object",
        "--bucket", "invoices-2026", "--key", "native/aws.txt",
        str(downloaded), "--no-cli-pager",
    ], aws_env)
    assert downloaded.read_text() == "native aws cli\n"
    listed = native([
        "aws", "--endpoint-url", endpoint, "s3api", "list-objects-v2",
        "--bucket", "invoices-2026", "--prefix", "native/", "--no-cli-pager",
    ], aws_env)
    assert "native/aws.txt" in listed, listed
    # Exercise defaults too: older AWS CLI versions commonly send Content-MD5.
    native([
        "aws", "--endpoint-url", endpoint, "s3api", "put-object",
        "--bucket", "invoices-2026", "--key", "native/юникод +&%.txt",
        "--body", str(object_path), "--no-cli-pager",
    ], aws_env)
    native([
        "aws", "--endpoint-url", endpoint, "s3api", "get-object",
        "--bucket", "invoices-2026", "--key", "native/юникод +&%.txt",
        "--range", "bytes=1-4", str(downloaded), "--no-cli-pager",
    ], aws_env)
    assert downloaded.read_bytes() == b"ativ"
    bad_aws = aws_env.copy()
    bad_aws["AWS_SECRET_ACCESS_KEY"] = "incorrect-secret"
    native_error([
        "aws", "--endpoint-url", endpoint, "s3api", "head-object",
        "--bucket", "invoices-2026", "--key", "native/aws.txt", "--no-cli-pager",
    ], bad_aws, "403")

    # A multipart upload driven by the CLI itself: 40 MiB in 8 MiB parts, so the
    # client really initiates, uploads five parts and completes. The result must
    # be byte-identical on download, with the S3 multipart ETag, and readable
    # through HEAD and a Range GET.
    multipart_env = aws_env.copy()
    multipart_config = Path(temp)/"aws-multipart-config"
    multipart_config.write_text(
        "[default]\nregion = us-east-1\ns3 =\n"
        "    addressing_style = path\n"
        "    multipart_threshold = 8MB\n"
        "    multipart_chunksize = 8MB\n"
    )
    multipart_env["AWS_CONFIG_FILE"] = str(multipart_config)
    part_size = 8 * 1024 * 1024
    big_parts = [bytes([i + 1]) * part_size for i in range(5)]
    big_path = Path(temp) / "native-multipart.bin"
    big_path.write_bytes(b"".join(big_parts))
    native([
        "aws", "--endpoint-url", endpoint, "s3", "cp", str(big_path),
        "s3://invoices-2026/native/multipart.bin", "--no-progress",
    ], multipart_env)
    expected_etag = multipart_etag(big_parts)
    head = json.loads(native([
        "aws", "--endpoint-url", endpoint, "s3api", "head-object",
        "--bucket", "invoices-2026", "--key", "native/multipart.bin",
        "--no-cli-pager",
    ], multipart_env))
    assert head["ContentLength"] == len(b"".join(big_parts)), head
    assert head["ETag"] == expected_etag, head
    big_download = Path(temp) / "native-multipart-downloaded.bin"
    native([
        "aws", "--endpoint-url", endpoint, "s3", "cp",
        "s3://invoices-2026/native/multipart.bin", str(big_download),
        "--no-progress",
    ], multipart_env)
    assert big_download.read_bytes() == big_path.read_bytes()
    ranged = Path(temp) / "native-multipart-range.bin"
    native([
        "aws", "--endpoint-url", endpoint, "s3api", "get-object",
        "--bucket", "invoices-2026", "--key", "native/multipart.bin",
        "--range", f"bytes={part_size}-{2 * part_size - 1}", str(ranged),
        "--no-cli-pager",
    ], multipart_env)
    assert ranged.read_bytes() == big_parts[1]
    native([
        "aws", "--endpoint-url", endpoint, "s3", "rm",
        "s3://invoices-2026/native/multipart.bin",
    ], multipart_env)
    native_error([
        "aws", "--endpoint-url", endpoint, "s3api", "head-object",
        "--bucket", "invoices-2026", "--key", "native/multipart.bin",
        "--no-cli-pager",
    ], multipart_env, "404")
    print("PASS native AWS CLI 2 multipart upload (40 MiB, 8 MiB parts, ETag, HEAD, Range)")
    print("PASS native psql 17 and AWS CLI 2 (Simple Query, transaction, S3 PUT/HEAD/GET/LIST)")


def rewrite_group_configs(root):
    """Point every node's group config at the proxy address of every peer.

    `prepare` requires `listen` to equal the node's own peer address, so a node
    keeps its real Raft address and only the *other* peers move behind that
    node's directed proxy. TLS stays end-to-end: the proxy relays bytes to the
    real listener, and the pinned certificate and server name still verify.
    """
    for node in NODES:
        for group in GROUPS:
            path = root / "config" / f"node-{node}-group-{group}.json"
            config = DEV.LOCAL.load(path)
            peers = []
            for peer in config["peers"]:
                peer = dict(peer)
                target = peer["id"]
                peer["address"] = (
                    f"127.0.0.1:{raft_port(group, node)}" if target == node
                    else f"127.0.0.1:{raft_proxy_port(group, node, target)}")
                peers.append(peer)
            config["peers"] = peers
            config["listen"] = f"127.0.0.1:{raft_port(group, node)}"
            DEV.LOCAL.replace(path, config)


def node_command(root, node):
    """Daemon argv for one node in this harness's fixed port layout.

    The launcher derives its own offsets from `--port-base`; this harness keeps
    the documented protocols.py bases, so the flags are spelled out. Everything
    that is not a port comes from the provisioned root.
    """
    return [
        str(ROOT / "target" / "debug" / "yadbd"),
        "--listen", f"127.0.0.1:{http_port(node)}",
        "--node-id", str(node),
        "--data-dir", str(root / "data" / f"node-{node}"),
        "--metadata-runtime", "openraft",
        "--openraft-config", str(root / "config" / f"node-{node}-manifest.json"),
        "--peers", ",".join(f"{peer}=127.0.0.1:{http_port(peer)}" for peer in NODES),
        "--admin-subject", ADMIN,
        "--protocol-credentials", str(root / "credentials.json"),
        "--pg-listen", f"127.0.0.1:{pg_port(node)}",
        "--s3-listen", f"127.0.0.1:{s3_port(node)}",
        "--s3-target", DEV.S3_TARGET,
        "--iscsi-listen", f"127.0.0.1:{iscsi_port(node)}",
        "--iscsi-portals", ",".join(f"127.0.0.1:{iscsi_port(peer)}" for peer in NODES),
    ]


def launch_node(root, node, env):
    """Spawn one daemon, appending to its log, in its own session."""
    log = (root / "logs" / f"node-{node}.log").open("ab", buffering=0)
    try:
        return subprocess.Popen(node_command(root, node), env=env,
                                stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
    finally:
        log.close()


def run(native_clients=False, libiscsi_dir=None, libiscsi_probe=None,
        install_snapshot_live=False):
    global SECRET, RAFT_TOKEN, SUBJECT_SECRET
    base = "/data/data/com.foxdebug.acodefree/cache/opencode"
    with tempfile.TemporaryDirectory(prefix="yadb-protocols-", dir=base) as temp:
        temp = Path(temp)
        # Provisioning is the launcher's, not a copy: credentials, TLS keys,
        # pinned certificates, group configs and the version-2 manifest.
        DEV.provision(temp, 19000)
        rewrite_group_configs(temp)
        secrets_ = DEV.load_secrets(temp)
        SECRET = DEV.ensure_credentials(temp)
        RAFT_TOKEN = secrets_["raft"]
        SUBJECT_SECRET = secrets_["subject"]
        env = DEV.daemon_env(temp)
        nodes = [None, None, None]
        proxies = {}
        def start_nodes(subset):
            # A replica that already exists must be reopened, not re-created.
            # The launcher owns that bookkeeping (`mode`/`initialize` per group
            # config), so a restart of the same storage directories is its
            # decision, not a second copy of the rule here.
            #
            # The decision covers all three replicas of a group at once, so it
            # must be taken once per batch. Calling it before each individual
            # launch makes the first start racy: node 1 creates its replica
            # directory while nodes 2 and 3 have none yet, which the launcher
            # correctly refuses as a partial bootstrap that it will not guess
            # about.
            DEV.sync_replica_modes(temp)
            return [launch_node(temp, i, env) for i in subset]
        try:
            # One directed proxy per (group, source, target): a test can cut a
            # single group's link without touching the other two groups.
            for group in GROUPS:
                for source in NODES:
                    for target in NODES:
                        if source == target:
                            continue
                        proxy = TransportProxy(group, source, target)
                        proxies[group, source, target] = proxy
                        threading.Thread(target=proxy.serve_forever, daemon=True).start()
            nodes[:] = start_nodes(NODES)
            for i in NODES:
                wait(http_port(i))
            # A bound listener is not an elected cluster: the bootstrap apply
            # must reach the control group's leader, so wait for every group's
            # election rather than racing it.
            wait_all_leaders()
            body = json.dumps({"yaml": (ROOT/"examples/platform.yaml").read_text()}).encode()
            assert request(http_port(2), "POST", "/v1/apply", body,
                           {"Content-Type": "application/json", **admin_headers()})[0] == 200
            for i in NODES:
                deadline = time.monotonic()+20
                while "billing/ledger" not in json.loads(request(http_port(i), "GET", "/v1/status")[2])["databases"]:
                    assert time.monotonic() < deadline, "control cache not replicated"
                    time.sleep(.05)
            # Exercise the internal DELETE route on every actual VDisk host,
            # using a reserved test project/channel unrelated to engine IDs.
            vdisk_tombstones = []
            gc_fragments = []
            health = json.loads(request(http_port(2), "GET", "/v1/pools/health")[2])
            for pool in health["pools"]:
                for slot in pool["slots"]:
                    blob = ((0xFFFFFFFE << 96) | (1 << 64) | (1 << 40)
                            | ((slot["slot_index"]+1) << 16) | (250 << 8) | 1)
                    path = f'/internal/vdisk/{pool["pool_name"]}/{slot["slot_index"]}/blob/{blob:032x}'
                    port = http_port(slot["node_id"])
                    internal = {"x-yadb-raft-token": RAFT_TOKEN}
                    assert request(port, "PUT", path, b"orphan-fragment", internal)[0] == 200
                    result = request(port, "DELETE", path, headers=internal)
                    assert result[0] == 200 and result[2] == b"deleted", result
                    assert request(port, "GET", path, headers=internal)[0] == 404
                    assert request(port, "DELETE", path, headers=internal)[2] == b"absent"
                    assert request(port, "PUT", path, b"late-fragment", internal)[0] == 412
                    vdisk_tombstones.append((port, path))
                    # A different reserved channel gives GC an unreferenced
                    # fragment to preserve, not the tombstoned DELETE fixture.
                    gc_blob = ((0xFFFFFFFE << 96) | (1 << 64) | (1 << 40)
                               | ((slot["slot_index"]+1) << 16) | (249 << 8) | 1)
                    gc_path = f'/internal/vdisk/{pool["pool_name"]}/{slot["slot_index"]}/blob/{gc_blob:032x}'
                    payload = b"gc-refusal-fragment"
                    assert request(port, "PUT", gc_path, payload, internal)[0] == 200
                    before = request(port, "GET", gc_path, headers=internal)
                    assert before[0] == 200 and before[2] == payload, before
                    gc_fragments.append((port, gc_path, payload))
            assert gc_fragments, "no VDisk fragments seeded for GC refusal"
            control_leader = wait_group_leader(CONTROL_GROUP)
            for path in ("/v1/pools/gc", "/v1/pools/scrub"):
                assert_multiraft_gc_refusal(http_port(control_leader), path, gc_fragments)
            print("PASS Multi-Raft GC and scrub explicitly refuse without deleting fragments")
            skip("VDisk orphan collection",
                 "scrub's repair/orphan scan is disabled on Multi-Raft: "
                 "crates/yadbd/src/main.rs:7636 answers 409 instead of 200")
            skip("per-group GC collection",
                 "mark-and-sweep is disabled for Multi-Raft: "
                 "crates/yadbd/src/gc.rs:149 refuses before any pass")
            print("PASS VDisk exact DELETE, idempotent tombstone and delayed PUT fencing")
            # The epoch and pin surface is wired into the shipped router, not
            # only into the handler functions the unit test calls. One slot is
            # enough: every slot is served by the same handlers, and the drill
            # is about the routes and the durable state behind them.
            first_pool = health["pools"][0]
            first_slot = first_pool["slots"][0]
            assert_vdisk_fence_rpc(
                http_port(first_slot["node_id"]),
                first_pool["pool_name"],
                first_slot["slot_index"])
            print("PASS VDisk epoch, pin, advance and collectable-delete refusals")
            a, b = Pg(pg_port(2)), Pg(pg_port(3))
            a.query("CREATE TABLE wire (id INT PRIMARY KEY, balance INT);")
            a.query("INSERT INTO wire VALUES (1,100), (2,200);")
            a.query("BEGIN; UPDATE wire SET balance=balance-10 WHERE id=1;")
            assert a.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            assert b.rows("SELECT balance FROM wire WHERE id=1;") == [["100"]]
            a.query("COMMIT;")
            assert b.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            a.query("BEGIN; UPDATE wire SET balance=0 WHERE id=1;")
            a.query("SELECT missing FROM wire;", fail=True)
            assert a.query("SELECT balance FROM wire;", fail=True)[-1] == (b"Z", b"E")
            # PostgreSQL treats COMMIT in an aborted transaction as ROLLBACK.
            out = a.query("COMMIT;")
            assert (b"C", b"ROLLBACK\0") in out and out[-1] == (b"Z", b"I"), out
            assert b.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            a.query("BEGIN; UPDATE wire SET balance=0 WHERE id=1;")
            assert a.query("INVALID SQL;", fail=True)[-1] == (b"Z", b"E")
            assert a.query("ROLLBACK;")[-1] == (b"Z", b"I")
            assert b.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            # A valid but failing DML aborts the transaction, rejects later
            # statements with 25P02, and leaves the connection usable on rollback.
            a.query("BEGIN; UPDATE wire SET balance=0 WHERE id=1;")
            failed = a.query("INSERT INTO wire VALUES (2,999);", fail=True)
            assert failed[-1] == (b"Z", b"E"), failed
            blocked = a.query("UPDATE wire SET balance=5 WHERE id=2;", fail=True)
            assert blocked[-1] == (b"Z", b"E") and any(
                kind == b"E" and b"C25P02\0" in data for kind, data in blocked), blocked
            rolled_back = a.query("ROLLBACK;")
            assert (b"C", b"ROLLBACK\0") in rolled_back and rolled_back[-1] == (b"Z", b"I"), rolled_back
            assert a.rows("SELECT balance FROM wire ORDER BY id;") == [["90"], ["200"]]
            assert b.rows("SELECT balance FROM wire ORDER BY id;") == [["90"], ["200"]]
            a.query("CREATE INDEX by_balance ON wire (balance);")
            assert b.rows("SELECT id FROM wire WHERE balance=90;") == [["1"]]
            b.query("UPDATE wire SET balance=201 WHERE id=2;")
            assert a.rows("SELECT id FROM wire WHERE balance=201;") == [["2"]]
            assert a.rows("SELECT id FROM wire WHERE balance=200;") == []
            # Extended query protocol: text parameters are bound as SQL literals.
            a.query("CREATE TABLE ext (id INT PRIMARY KEY, note TEXT);")
            out = a.extended("SELECT id, balance FROM wire WHERE id = $1;", params=("1",), oids=(23,))
            assert a.data_rows(out) == [["1", "90"]], out
            assert (b"C", b"SELECT 1\0") in out and out[-1] == (b"Z", b"I"), out
            out = a.extended("INSERT INTO ext VALUES ($1, $2);", params=("1", "it's; fine"), oids=(23, 25))
            assert (b"C", b"INSERT 0 1\0") in out, out
            assert a.rows("SELECT note FROM ext WHERE id=1;") == [["it's; fine"]]
            # A parameter value that looks like SQL stays a harmless string literal.
            out = a.extended("INSERT INTO ext VALUES ($1, $2);", params=("2", "'; DROP TABLE ext; --"), oids=(23, 25))
            assert (b"C", b"INSERT 0 1\0") in out, out
            assert a.rows("SELECT note FROM ext WHERE id=2;") == [["'; DROP TABLE ext; --"]]
            assert a.rows("SELECT id FROM ext ORDER BY id;") == [["1"], ["2"]]
            # Re-Parse of the unnamed statement replaces the earlier one.
            a.parse("SELECT note FROM ext WHERE id = $1;")
            a.parse("SELECT id FROM ext WHERE id = $1;")
            a.bind(params=("1",))
            a.execute()
            out = a.sync()
            assert [k for k, _ in out].count(b"1") == 2, out
            assert a.data_rows(out) == [["1"]], out
            # Describe(statement): ParameterDescription then RowDescription for a
            # read-only statement, NoData for a mutating one (which is not executed).
            a.parse("SELECT id FROM ext WHERE id = $1;", oids=(23,))
            out = a.sync()
            assert [k for k, _ in out] == [b"1", b"Z"], out
            a.describe(b"S")
            out = a.sync()
            assert out[0][0] == b"t" and struct.unpack("!H", out[0][1][:2])[0] == 1, out
            assert struct.unpack("!I", out[0][1][2:6])[0] == 23 and out[1][0] == b"T", out
            a.parse("INSERT INTO ext VALUES ($1, $2);", oids=(23, 25))
            a.sync()
            a.describe(b"S")
            out = a.sync()
            assert out[0][0] == b"t" and out[1] == (b"n", b""), out
            assert a.rows("SELECT id FROM ext ORDER BY id;") == [["1"], ["2"]]
            # An error skips everything until Sync; the connection stays usable.
            a.parse("SELECT missing FROM ext;")
            a.bind()
            a.execute()
            a.parse("SELECT id FROM ext;")
            a.bind()
            a.execute()
            out = a.sync()
            # Only the first Parse/Bind completed; the rest were skipped until Sync.
            assert [k for k, _ in out] == [b"1", b"2", b"E", b"Z"], out
            assert out[-1] == (b"Z", b"I"), out
            assert a.rows("SELECT id FROM ext ORDER BY id;") == [["1"], ["2"]]
            # Missing and extra parameters are protocol errors.
            a.parse("SELECT id FROM ext WHERE id = $2;")
            a.bind(params=("1",))
            out = a.sync()
            assert any(k == b"E" and b"C42P02\0" in d for k, d in out), out
            assert out[-1] == (b"Z", b"I"), out
            a.parse("SELECT id FROM ext;")
            a.bind(params=("1",))
            out = a.sync()
            assert any(k == b"E" and b"C08P01\0" in d for k, d in out), out
            # Binary parameter and result formats are refused.
            a.parse("SELECT id FROM ext WHERE id = $1;")
            a.bind(params=(b"\x00\x00\x00\x01",), formats=(1,))
            out = a.sync()
            assert any(k == b"E" and b"C0A000\0" in d and b"binary parameter" in d for k, d in out), out
            a.parse("SELECT id FROM ext;")
            a.bind(results=(1,))
            out = a.sync()
            assert any(k == b"E" and b"C0A000\0" in d and b"binary result" in d for k, d in out), out
            # Named statements and portals are independent of the unnamed ones.
            a.parse("SELECT note FROM ext WHERE id = $1;", name="find_ext")
            a.bind(portal="p1", statement="find_ext", params=("2",))
            a.execute("p1")
            out = a.sync()
            assert a.data_rows(out) == [["'; DROP TABLE ext; --"]], out
            a.close_object(b"P", "p1")
            a.close_object(b"S", "find_ext")
            out = a.sync()
            assert [k for k, _ in out].count(b"3") == 2, out
            # Execute drives the same transaction state as Simple Query.
            out = a.extended("BEGIN;")
            assert out[-1] == (b"Z", b"T"), out
            out = a.extended("UPDATE ext SET note = $1 WHERE id = $2;", params=("changed", "1"), oids=(25, 23))
            assert (b"C", b"UPDATE 1\0") in out, out
            out = a.extended("ROLLBACK;")
            assert (b"C", b"ROLLBACK\0") in out and out[-1] == (b"Z", b"I"), out
            assert a.rows("SELECT note FROM ext WHERE id=1;") == [["it's; fine"]]
            # COMMIT in an aborted extended transaction becomes ROLLBACK.
            out = a.extended("BEGIN;")
            assert out[-1] == (b"Z", b"T"), out
            a.parse("SELECT missing FROM ext;")
            a.bind()
            a.execute()
            out = a.sync()
            assert out[-1] == (b"Z", b"E"), out
            out = a.extended("COMMIT;")
            assert (b"C", b"ROLLBACK\0") in out and out[-1] == (b"Z", b"I"), out
            # Close, and an Execute against a missing portal.
            a.close_object(b"S", "")
            out = a.sync()
            assert (b"3", b"") in out, out
            a.execute("absent")
            out = a.sync()
            assert any(k == b"E" and b"C34000\0" in d for k, d in out), out
            print("PASS pgwire extended query: literal binding, describe, errors, transactions")
            a.close()
            # COPY is typed from the declared schema (`SHOW COLUMNS`), runs as one
            # transaction outside BEGIN, and a bad row must roll the whole load
            # back: a cleanup that silently failed would leave a partial table or
            # an open transaction the client cannot see.
            b = Pg(pg_port(2))
            b.query("CREATE TABLE bulk (id INT PRIMARY KEY, name TEXT);")
            assert [c[0] for c in b.rows("SHOW COLUMNS FROM bulk;")] == ["id", "name"]
            out = b.copy_in("COPY bulk (id, name) FROM STDIN;",
                            [b"1\talpha\n", b"2\tbeta\n"])
            assert (b"C", b"COPY 2\0") in out and out[-1] == (b"Z", b"I"), out
            assert b.rows("SELECT id FROM bulk ORDER BY id;") == [["1"], ["2"]]
            # A row that does not parse aborts the COPY: nothing from it survives
            # and the session is idle again, not stuck in a failed transaction.
            out = b.copy_in("COPY bulk (id, name) FROM STDIN;",
                            [b"3\tgamma\n", b"bad\tdelta\n"])
            assert any(k == b"E" for k, _ in out), out
            assert out[-1] == (b"Z", b"I"), out
            assert b.rows("SELECT id FROM bulk ORDER BY id;") == [["1"], ["2"]]
            # CRLF is one terminator; the CR is not part of the value.
            out = b.copy_in("COPY bulk (id, name) FROM STDIN;", [b"4\tcrlf\r\n"])
            assert (b"C", b"COPY 1\0") in out, out
            assert b.rows("SELECT name FROM bulk WHERE id = 4;") == [["crlf"]]
            # A COPY statement cannot be combined with another statement: it is
            # refused before the copy-in handshake, so no data can be half-sent.
            out = b.query("COPY bulk (id, name) FROM STDIN; SELECT 1;", fail=True)
            assert out[-1] == (b"Z", b"I"), out
            assert b.rows("SELECT id FROM bulk WHERE id = 5;") == []
            b.close()
            print("PASS pgwire COPY: declared schema, commit, rollback on a bad row, CRLF")
            print("PASS pgwire password, result framing, multi-statements, session isolation, rollback")
            print("PASS pgwire index backfill and cross-portal indexed DML")

            key = "/invoices-2026/dir/a%20%2B%26.txt"
            assert signed(s3_port(2), "PUT", key, b"hello aws")[0] == 200
            # Default reads are local-applied, so a follower converges rather
            # than answering the new version instantly.
            s3_visible(s3_port(3), key, b"hello aws")
            assert signed(s3_port(3), "GET", key, extra={"range": "bytes=1-4"})[2] == b"ello"
            status, hs, body = signed(s3_port(3), "GET", key, extra={"range": "bytes=7-999"})
            assert status == 206 and body == b"ws", (status, hs, body)
            assert next(v for k, v in hs.items() if k.lower() == "content-range") == "bytes 7-8/9"
            assert signed(s3_port(3), "HEAD", key)[0] == 200
            # A quorum-confirmed read is served on OpenRaft: a follower forwards
            # it to the project-group leader, which reads one snapshot.
            consistent = signed(s3_port(3), "GET", key + "?consistent=true")
            assert consistent[0] == 200 and consistent[2] == b"hello aws", consistent
            missing = signed(s3_port(3), "GET", "/invoices-2026/consistent-missing?consistent=true")
            assert missing[0] == 404, missing
            # A successful conditional GET must return the version whose ETag
            # was matched, even while a different version is being published.
            race_key = "/invoices-2026/conditional-race"
            original, replacement = b"old version", b"new version has another size"
            status, hs, _ = signed(s3_port(2), "PUT", race_key, original)
            assert status == 200
            etag = next(v for k, v in hs.items() if k.lower() == "etag")
            s3_visible(s3_port(2), race_key, original)
            failures = []
            def overwrite():
                try:
                    for i in range(60):
                        result = signed(s3_port(2), "PUT", race_key, replacement if i % 2 == 0 else original)
                        assert result[0] == 200, result
                except BaseException as e:
                    failures.append(e)
            writer = threading.Thread(target=overwrite)
            writer.start()
            try:
                for _ in range(120):
                    status, _, body = signed(s3_port(2), "GET", race_key, extra={"if-match": etag})
                    assert status == 412 or (status == 200 and body == original), (status, body)
            finally:
                writer.join()
            assert not failures, failures
            print("PASS S3 conditional GET under concurrent overwrites")
            status, _, body = eventually(
                lambda: signed(s3_port(3), "GET", "/invoices-2026?list-type=2&prefix=dir%2F"),
                lambda r: r[0] == 200
                and ET.fromstring(r[2]).find("{*}Contents/{*}Key").text == "dir/a +&.txt",
                "S3 cross-node listing sees dir/a +&.txt")
            assert status == 200, body
            assert signed(s3_port(2), "GET", key, secret="wrong-password")[0] == 403
            invalid_range = signed(s3_port(2), "GET", key, extra={"range": "bytes=900-999"})
            assert invalid_range[0] == 416, invalid_range
            assert signed(s3_port(2), "DELETE", key)[0] == 204
            s3_absent(s3_port(2), key)
            for name in ["a", "b", "c"]:
                assert signed(s3_port(2), "PUT", "/invoices-2026/page/"+name, name.encode())[0] == 200
            _, _, first = eventually(
                lambda: signed(s3_port(2), "GET", "/invoices-2026?list-type=2&prefix=page%2F&max-keys=1"),
                lambda r: r[0] == 200
                and ET.fromstring(r[2]).find("{*}NextContinuationToken") is not None,
                "S3 paginated listing sees page/")
            token = ET.fromstring(first).find("{*}NextContinuationToken").text
            _, _, second = signed(s3_port(2), "GET", "/invoices-2026?list-type=2&prefix=page%2F&max-keys=1&continuation-token="+token)
            assert ET.fromstring(second).find("{*}Contents/{*}Key").text == "page/b"
            assert signed(s3_port(2), "PUT", "/invoices-2026/bad", b"bad", {"x-amz-checksum-crc32": "AAAAAA=="})[0] == 400
            assert signed(s3_port(2), "PUT", "/invoices-2026/bad-md5", b"bad", {"content-md5": "AAAAAAAAAAAAAAAAAAAAAA=="})[0] == 400
            # A quorum-confirmed read is served above on OpenRaft: the legacy
            # runtime refused `?consistent=true` because it cannot take such a
            # snapshot, so that refusal became a success assertion. Nothing here
            # falls back to a locally applied manifest.
            skip("reader lease",
                 "begin_read_lease is legacy-gated: the gate is `!st.app.meta."
                 "is_openraft()` at crates/yadbd/src/protocols/s3.rs:683, and the "
                 "lease itself is declared at crates/yadbd/src/main.rs:524, so "
                 "Multi-Raft has no fenced read lease to acquire, renew or expire; "
                 "consistent reads use the quorum snapshot instead")
            # Multipart upload: the parts are real durable allocations, and the
            # completed object must be indistinguishable from a single PUT.
            # An option the object store cannot honour is refused at initiate,
            # before any durable upload record exists, on the native wire too:
            # the listener only forwards a few headers, so the gateway cannot
            # see the client's metadata and would silently drop it.
            for options in ({"x-amz-meta-owner": "ops"},
                            {"x-amz-server-side-encryption": "AES256"}):
                status, _, body = signed(
                    s3_port(2), "POST", "/invoices-2026/multipart/refused?uploads",
                    b"", options)
                assert status == 501 and b"<Code>NotImplemented</Code>" in body, (status, body)
                assert signed(s3_port(2), "GET", "/invoices-2026/multipart/refused")[0] == 404
            mp_key = "/invoices-2026/multipart/object.bin"
            status, _, body = signed(s3_port(2), "POST", mp_key + "?uploads")
            assert status == 200, (status, body)
            upload_id = ET.fromstring(body).find("{*}UploadId").text
            assert upload_id.startswith("mpu-"), upload_id
            mp_parts = [b"a" * 4096, b"b" * 5000, b"c" * 7]
            mp_etags = []
            for number, part in enumerate(mp_parts, 1):
                status, hs, body = signed(
                    s3_port(2), "PUT",
                    f"{mp_key}?partNumber={number}&uploadId={upload_id}", part)
                assert status == 200, (status, body)
                etag = next(v for k, v in hs.items() if k.lower() == "etag")
                assert etag == '"' + hashlib.md5(part).hexdigest() + '"', etag
                mp_etags.append((number, etag.strip('"')))
            _, _, body = eventually(
                lambda: signed(s3_port(2), "GET", f"{mp_key}?uploadId={upload_id}"),
                lambda r: r[0] == 200 and len(ET.fromstring(r[2]).findall("{*}Part")) == 3,
                "S3 ListParts sees three parts")
            assert len(ET.fromstring(body).findall("{*}Part")) == 3, body
            # ListParts is paginated and says so; the marker resumes after the
            # last part shown. An out-of-range page size is refused, not clamped.
            status, _, body = signed(
                s3_port(2), "GET", f"{mp_key}?uploadId={upload_id}&max-parts=2")
            assert status == 200, (status, body)
            page = ET.fromstring(body)
            assert [p.find("{*}PartNumber").text for p in page.findall("{*}Part")] == ["1", "2"], body
            assert page.find("{*}IsTruncated").text == "true", body
            marker = page.find("{*}NextPartNumberMarker").text
            assert marker == "2", body
            status, _, body = signed(
                s3_port(2), "GET",
                f"{mp_key}?uploadId={upload_id}&max-parts=2&part-number-marker={marker}")
            assert status == 200, (status, body)
            page = ET.fromstring(body)
            assert [p.find("{*}PartNumber").text for p in page.findall("{*}Part")] == ["3"], body
            assert page.find("{*}IsTruncated").text == "false", body
            status, _, body = signed(
                s3_port(2), "GET", f"{mp_key}?uploadId={upload_id}&max-parts=0")
            assert status == 400 and b"<Code>InvalidArgument</Code>" in body, (status, body)
            # An explicit empty value is a client error, not an absent page size:
            # defaulting `max-parts=` to 1000 would answer a different request.
            for empty in ("max-parts=", "part-number-marker="):
                status, _, body = signed(
                    s3_port(2), "GET", f"{mp_key}?uploadId={upload_id}&{empty}")
                assert status == 400 and b"<Code>InvalidArgument</Code>" in body, (status, body)
            # A wrong ETag and an out-of-order list are refused with the S3 codes,
            # and neither consumes the upload.
            wrong = [(mp_etags[0][0], "0" * 32)] + mp_etags[1:]
            status, _, body = signed(
                s3_port(2), "POST", f"{mp_key}?uploadId={upload_id}",
                complete_multipart_xml(wrong))
            assert status == 400 and b"<Code>InvalidPart</Code>" in body, (status, body)
            status, _, body = signed(
                s3_port(2), "POST", f"{mp_key}?uploadId={upload_id}",
                complete_multipart_xml(list(reversed(mp_etags))))
            assert status == 400 and b"<Code>InvalidPartOrder</Code>" in body, (status, body)
            status, hs, body = signed(
                s3_port(2), "POST", f"{mp_key}?uploadId={upload_id}",
                complete_multipart_xml(mp_etags))
            assert status == 200, (status, body)
            expected = multipart_etag(mp_parts)
            assert ET.fromstring(body).find("{*}ETag").text == expected, body
            # Location names the object and must not leak the upload id: it is
            # the client's resume token for an upload that no longer exists.
            location = ET.fromstring(body).find("{*}Location")
            assert location is not None, body
            assert "uploadId" not in location.text, location.text
            joined = b"".join(mp_parts)
            _, hs, body = eventually(
                lambda: signed(s3_port(3), "GET", mp_key),
                lambda r: r[0] == 200 and r[2] == joined,
                "S3 completed multipart object visible on the other node")
            assert next(v for k, v in hs.items() if k.lower() == "etag") == expected
            status, hs, _ = signed(s3_port(3), "HEAD", mp_key)
            assert status == 200
            assert int(next(v for k, v in hs.items() if k.lower() == "content-length")) == len(joined)
            status, hs, body = signed(s3_port(3), "GET", mp_key, extra={"range": "bytes=4096-9095"})
            assert status == 206 and body == joined[4096:9096], (status, body)
            # The consumed upload cannot be completed twice.
            eventually(lambda: signed(s3_port(2), "GET", f"{mp_key}?uploadId={upload_id}"),
                       lambda r: r[0] == 404 and b"<Code>NoSuchUpload</Code>" in r[2],
                       "S3 consumed upload is gone")
            result = signed(s3_port(2), "POST", f"{mp_key}?uploadId={upload_id}",
                            complete_multipart_xml(mp_etags))
            assert result[0] == 404 and b"<Code>NoSuchUpload</Code>" in result[2], result
            # Abort removes the upload, and its parts are no longer usable.
            status, _, body = signed(s3_port(2), "POST", "/invoices-2026/multipart/aborted?uploads")
            assert status == 200, (status, body)
            abort_id = ET.fromstring(body).find("{*}UploadId").text
            aborted = "/invoices-2026/multipart/aborted"
            assert signed(
                s3_port(2), "PUT", f"{aborted}?partNumber=1&uploadId={abort_id}", b"x")[0] == 200
            assert signed(s3_port(2), "DELETE", f"{aborted}?uploadId={abort_id}")[0] == 204
            eventually(lambda: signed(s3_port(2), "GET", f"{aborted}?uploadId={abort_id}"),
                       lambda r: r[0] == 404 and b"<Code>NoSuchUpload</Code>" in r[2],
                       "S3 aborted upload is gone")
            result = signed(s3_port(2), "PUT", f"{aborted}?partNumber=2&uploadId={abort_id}", b"y")
            assert result[0] == 404 and b"<Code>NoSuchUpload</Code>" in result[2], result
            assert signed(s3_port(3), "GET", aborted)[0] == 404
            # The same flow through the other node. Each statement is forwarded
            # to whichever node leads the billing group, so the follower that
            # received it must converge on the same state.
            follow_key = "/invoices-2026/multipart/forwarded.bin"
            status, _, body = signed(s3_port(3), "POST", follow_key + "?uploads")
            assert status == 200, (status, body)
            follow_id = ET.fromstring(body).find("{*}UploadId").text
            follow_parts = [b"f" * 32, b"g" * 64]
            follow_etags = []
            for number, part in enumerate(follow_parts, 1):
                status, hs, body = signed(
                    s3_port(3), "PUT",
                    f"{follow_key}?partNumber={number}&uploadId={follow_id}", part)
                assert status == 200, (status, body)
                follow_etags.append(
                    (number, next(v for k, v in hs.items() if k.lower() == "etag").strip('"')))
            status, _, body = signed(
                s3_port(3), "POST", f"{follow_key}?uploadId={follow_id}",
                complete_multipart_xml(follow_etags))
            assert status == 200, (status, body)
            eventually(lambda: signed(s3_port(2), "GET", follow_key),
                       lambda r: r[0] == 200 and r[2] == b"".join(follow_parts),
                       "S3 forwarded multipart object visible on the other node")
            print("PASS S3 multipart: initiate, parts, list, complete, HEAD/Range, abort")
            print("PASS S3 SigV4, XML listing, escaped keys, PUT/GET/HEAD/Range/DELETE, bad signatures")

            concurrent_results = []
            concurrent_lock = threading.Lock()
            def concurrent_put(i):
                try:
                    # Every S3 write commits through the billing group's metadata
                    # log, and 24 simultaneous proposals contend for it: measured
                    # 0.27 s for one write, 0.33 s each at 2 in flight, but 38 s
                    # for all 24. The per-request timeout is a client-side
                    # budget, not part of the contract under test (the contract
                    # is that every proposal is acknowledged and readable), so it
                    # is raised here instead of racing a queue that is real.
                    result = signed(s3_port(2), "PUT", f"/invoices-2026/concurrent/{i}",
                                    f"value-{i}".encode(), timeout=180)
                except Exception as e:
                    result = (None, {}, repr(e))
                with concurrent_lock:
                    concurrent_results.append((i, result))
            writers = [threading.Thread(target=concurrent_put, args=(i,)) for i in range(24)]
            for writer in writers:
                writer.start()
            for writer in writers:
                writer.join()
            assert len(concurrent_results) == len(writers), concurrent_results
            failures = [(i, result) for i, result in concurrent_results if result[0] != 200]
            assert not failures, failures
            for i in range(24):
                eventually(lambda i=i: signed(s3_port(3), "GET", f"/invoices-2026/concurrent/{i}"),
                           lambda r, i=i: r[0] == 200 and r[2] == f"value-{i}".encode(),
                           f"S3 concurrent object {i} visible on the other node")
            print("PASS concurrent Raft-backed S3 proposals")

            if native_clients:
                native_client_checks(temp, pg_port(2), s3_port(2))

            Iscsi(iscsi_port(2), secret="wrong-password")
            discovery = Iscsi(iscsi_port(2), discovery=True)
            _, data = discovery.exchange(4, 0x80, b"SendTargets=All\0")
            assert data.count(b"TargetAddress=") == 3, data
            discovery.s.close()
            x, y = Iscsi(iscsi_port(2)), Iscsi(iscsi_port(3))
            cdb = bytes([0x12, 1, 0x83, 0, 255, 0])
            assert x.scsi(cdb, 255) == y.scsi(cdb, 255)
            # INQUIRY allocation length zero but iSCSI expects 36 bytes: the
            # SCSI response must report the entire transfer as underflow.
            x.itt += 1
            h = x.header(1, 0xc1)
            h[20:24] = (36).to_bytes(4, "big")
            h[32:38] = bytes([0x12, 0, 0, 0, 0, 0])
            x.send(h)
            x.cmd += 1
            r, data = x.recv()
            assert r[0] == 0x21 and r[3] == 0 and not data
            assert r[1] & 2 and int.from_bytes(r[44:48], "big") == 36, r
            # Respect the SCSI CDB allocation length independently from the
            # iSCSI Expected Data Transfer Length and report the difference.
            for cdb, allocated in [
                (bytes([0x03, 0, 0, 0, 4, 0]), 4),
                (bytes([0x1a, 0, 8, 0, 4, 0]), 4),
                (bytes([0x5a, 0, 8, 0, 0, 0, 0, 0, 4, 0]), 4),
                (bytes([0x9e, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0]), 4),
                (bytes([0xa0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0]), 4),
            ]:
                x.itt += 1
                h = x.header(1, 0xc1)
                h[20:24] = (32).to_bytes(4, "big")
                h[32:32+len(cdb)] = cdb
                x.send(h)
                x.cmd += 1
                r, data = x.recv()
                assert r[0] == 0x25 and r[1] & 1 and r[3] == 0, (cdb, r)
                assert len(data) == allocated, (cdb, len(data))
                assert r[1] & 2 and int.from_bytes(r[44:48], "big") == 32-allocated, (cdb, r)
            # INQUIRY: exact transfer, and a larger allocation/expected length
            # than the 36-byte standard response. Inspect the final iSCSI flags
            # and residual rather than merely checking the returned payload.
            for allocation, expected, length, residual in [(4, 4, 4, 0), (64, 64, 36, 28)]:
                x.itt += 1
                h = x.header(1, 0xc1)
                h[20:24] = expected.to_bytes(4, "big")
                h[32:38] = bytes([0x12, 0, 0, 0, allocation, 0])
                x.send(h)
                x.cmd += 1
                r, data = x.recv()
                assert r[0] == 0x25 and r[1] & 1 and r[3] == 0, r
                assert len(data) == length and data[2] == 6, (r, data)
                assert bool(r[1] & 2) == bool(residual), r
                assert not r[1] & 4 and int.from_bytes(r[44:48], "big") == residual, r
            x.write(b"A"*512)
            assert y.read() == b"A"*512
            x.write(b"B"*4096, immediate=False)
            assert y.read(blocks=8) == b"B"*4096
            unmap = struct.pack("!HHI Q I I", 22, 16, 0, 0, 8, 0)
            cdb = bytearray(10)
            cdb[0] = 0x42
            cdb[7:9] = len(unmap).to_bytes(2, "big")
            x.scsi(cdb, len(unmap), unmap)
            assert y.read(blocks=8) == bytes(4096)
            x.write(b"C"*512)
            x.scsi(bytes([0x35])+bytes(9))
            print("PASS iSCSI CHAP, discovery, stable VPD83, READ/WRITE, R2T/Data-Out, UNMAP")
            limited = Iscsi(iscsi_port(2), negotiation={"ImmediateData": "No", "MaxBurstLength": "4096", "FirstBurstLength": "4096"})
            assert limited.negotiated["ImmediateData"] == "No", limited.negotiated
            assert limited.negotiated["MaxBurstLength"] == "4096", limited.negotiated
            payload = bytes(range(256))*64
            limited.write(payload, lba=512, immediate=False)
            assert limited.read(lba=512, blocks=32) == payload
            limited.s.close()
            print("PASS iSCSI ImmediateData=No, negotiated burst limits, multiple R2Ts")
            if libiscsi_dir:
                iscsi_url = f"iscsi://backend%{SECRET}@127.0.0.1:{iscsi_port(2)}/iqn.2026-11.io.yadb:billing:vm-root/0"
                output = native([str(Path(libiscsi_dir)/"iscsi-inq"), "-d", "3", iscsi_url])
                assert "YADB" in output, output
                print(native([str(Path(libiscsi_dir)/"iscsi-readcapacity16"), iscsi_url]))
                print("PASS native libiscsi INQUIRY and READ CAPACITY(16)")
            if libiscsi_probe:
                probe_env = os.environ.copy()
                probe_env["YADB_TEST_SECRET"] = SECRET
                print(native([libiscsi_probe, f"127.0.0.1:{iscsi_port(2)}"], probe_env).strip())

            # Abruptly stop every process after acknowledged writes, retaining
            # the same storage directories. One SQL transaction remains open.
            assert signed(s3_port(2), "PUT", "/invoices-2026/restart", b"durable object")[0] == 200
            x.write(b"R"*512, lba=4096)
            uncommitted = Pg(pg_port(2))
            uncommitted.query("BEGIN; UPDATE wire SET balance=7 WHERE id=1;")
            for node in nodes:
                node.kill()
            for node in nodes:
                node.wait(timeout=10)
            uncommitted.s.close()
            b.s.close()
            x.s.close()
            y.s.close()
            nodes[:] = start_nodes(NODES)
            for i in NODES:
                wait(http_port(i))
            for i in NODES:
                deadline = time.monotonic()+30
                while "billing/ledger" not in json.loads(request(http_port(i), "GET", "/v1/status")[2])["databases"]:
                    assert time.monotonic() < deadline, "control cache not recovered"
                    time.sleep(.05)
            wait_all_leaders()
            if not install_snapshot_live:
                skip("cross-process InstallSnapshot",
                     "not run by default: reaching the daemon's shipped "
                     "snapshot trigger (Config::default, since_last:5000) needs "
                     f"a burst of {SNAPSHOT_BURST_ENTRIES} committed entries, "
                     f"measured at {SNAPSHOT_BURST_COST}, so this scenario is "
                     f"opt-in through --install-snapshot-live; {INSTALL_PROOF_ELSEWHERE}. "
                     "Without it a full-cluster restart only re-elects and "
                     "replays every node's local WAL")
            else:
                # Only one node is stopped. The restarted node must install the
                # leader's snapshot: its own log ends before the entries the
                # leader purged, so replaying that WAL cannot produce the state.
                #
                # Say where the install path is actually proven before spending
                # hours on it. The crate-level process test drives three child
                # processes over mTLS and calls `raft.trigger().snapshot()`
                # directly; this scenario can only reach the same trigger through
                # the shipped `Config::default()` policy, which is why it is
                # expensive, and it adds no evidence beyond the HTTP level.
                print(f"  live install attempt: {INSTALL_PROOF_ELSEWHERE}; this "
                      f"scenario spends a {SNAPSHOT_BURST_ENTRIES}-entry burst "
                      f"({SNAPSHOT_BURST_COST}) to reach the daemon's default "
                      "snapshot trigger", flush=True)
                leader = wait_group_leader(BILLING_GROUP)
                control_leader = wait_group_leader(CONTROL_GROUP)
                # Keep both the billing and control leaders alive: the burst
                # needs the billing leader to commit, and the control lease for
                # every request. Either other node follows both groups.
                candidates = [i for i in NODES if i not in (leader, control_leader)]
                stopped = (candidates or [i for i in NODES if i != leader])[0]
                assert stopped not in (leader, control_leader), (stopped, leader, control_leader)
                stopped_index = group_observation(stopped, BILLING_GROUP)["last_log_index"]
                assert isinstance(stopped_index, int), stopped_index
                node_log = temp / "logs" / f"node-{stopped}.log"
                nodes[stopped-1].kill()
                nodes[stopped-1].wait(timeout=10)
                burst_query(leader,
                            "CREATE TABLE snapshot_burst (id INT PRIMARY KEY, balance INT);")
                try:
                    statements, entries, burst_seconds = burst_commits(
                        leader, SNAPSHOT_BURST_ENTRIES)
                except BurstTimeout as timeout_error:
                    # Report the unfinished burst as a SKIP with its numbers, not
                    # as a pass and not as a suite failure: on this host it is the
                    # expected outcome, and it says something about cost only.
                    # Bring the stopped node back first, so the scenarios after
                    # this one see a full cluster and the run continues.
                    DEV.sync_replica_modes(temp)
                    nodes[stopped-1] = launch_node(temp, stopped, env)
                    wait(http_port(stopped))
                    skip("cross-process InstallSnapshot (attempted)",
                         f"the burst did not reach the daemon's default snapshot "
                         f"trigger in {timeout_error.seconds:.0f}s: it committed "
                         f"{timeout_error.entries} of {SNAPSHOT_BURST_ENTRIES} "
                         f"entries from {timeout_error.written} statements, and "
                         f"node {stopped} was restarted so the rest of the run "
                         f"could continue; no install was attempted, and no PASS "
                         f"is claimed. {INSTALL_PROOF_ELSEWHERE}")
                else:
                    target = eventually(
                        lambda: group_observation(leader, BILLING_GROUP),
                        lambda o: (observation_index(o["last_applied"]) or -1)
                                  >= o["last_log_index"],
                        f"leader {leader} applied every burst entry", timeout=120)
                    target_applied = observation_index(target["last_applied"])
                    assert target_applied > stopped_index, (target_applied, stopped_index)
                    leader_rows = burst_rows(leader)
                    assert leader_rows and int(leader_rows[0][0]) == statements, (
                        leader_rows, statements)
                    # Restart only this node, and instrument only this node. The
                    # install proof is a line in its own log; enabling openraft's
                    # INFO filter cluster-wide costs this debug build roughly a
                    # factor of ten in commit throughput, which the burst cannot
                    # pay. The scan starts at the current end of the file, so a
                    # line from an earlier scenario cannot satisfy it.
                    node_offset = node_log.stat().st_size
                    evidence_env = dict(env, RUST_LOG="openraft=info")
                    DEV.sync_replica_modes(temp)
                    nodes[stopped-1] = launch_node(temp, stopped, evidence_env)
                    restarted_pid = nodes[stopped-1].pid
                    wait(http_port(stopped))
                    deadline = time.monotonic()+300
                    while True:
                        try:
                            applied = observation_index(
                                group_observation(stopped, BILLING_GROUP)["last_applied"])
                        except (OSError, RuntimeError):
                            applied = None
                        if applied is not None and applied >= target_applied:
                            break
                        assert time.monotonic() < deadline, (
                            f"node {stopped} did not apply through index {target_applied}")
                        time.sleep(.05)
                    installs = log_evidence(
                        node_log, node_offset, "Done install complete snapshot",
                        lambda line: (snapshot_meta_index(line) or 0) > stopped_index,
                        f"node {stopped} completed a snapshot install")
                    # Both routes forward a data write to the billing leader, so
                    # the rows below prove the cluster still serves the burst; the
                    # proof that this node itself holds the state is its own
                    # applied index, the completed-install line above, and this
                    # local read, which `force_leader: false` serves from its own
                    # applied state.
                    restored_rows = burst_rows(stopped, local=True)
                    assert restored_rows == leader_rows, (restored_rows, leader_rows)
                    print(f"PASS cross-process InstallSnapshot: node {stopped} (pid "
                          f"{restarted_pid}) stopped at log index {stopped_index}; "
                          f"{statements} statements made {entries} entries in "
                          f"{burst_seconds:.1f}s; restarted alone, it applied through "
                          f"index {target_applied} only by installing a snapshot "
                          f"(installed meta index "
                          f"{max(snapshot_meta_index(line) for line in installs)}, "
                          f"past the index it stopped at) and its own local read "
                          f"returns all {leader_rows[0][0]} burst rows; this is "
                          f"HTTP-level coverage only, the install path itself is "
                          f"proven by the crate test "
                          f"three_process_mtls_failover_and_quorum_loss")
            restored = Pg(pg_port(2))
            assert restored.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            assert restored.rows("SELECT id FROM wire WHERE balance=90;") == [["1"]]
            assert restored.rows("SELECT id FROM wire WHERE balance=201;") == [["2"]]
            assert restored.rows("SELECT id FROM wire WHERE balance=200;") == []
            restored.close()
            s3_visible(s3_port(3), "/invoices-2026/restart", b"durable object")
            for port, path in vdisk_tombstones:
                internal = {"x-yadb-raft-token": RAFT_TOKEN}
                status, _, body = request(port, "PUT", path, b"late-after-restart", internal)
                assert status == 412, (status, body)
                assert request(port, "GET", path, headers=internal)[0] == 404
            disk = Iscsi(iscsi_port(3))
            assert disk.read(lba=4096) == b"R"*512
            disk.s.close()
            print("PASS full cluster SIGKILL/restart: committed SQL, S3, iSCSI and uncommitted SQL rollback")

            # Ambiguous writes. A client that loses the response decides the
            # outcome by reading, never by retrying the mutation.
            ambiguous_s3_key = "/invoices-2026/ambiguous-process-s3"
            blocked_s3_key = "/invoices-2026/ambiguous-blocked-s3"
            assert signed(s3_port(2), "GET", ambiguous_s3_key)[0] == 404
            assert signed(s3_port(2), "GET", blocked_s3_key)[0] == 404

            def cut_billing_leader():
                """Isolate the billing-group leader in both Raft directions.

                Peer-to-peer links stay intact. The isolated node cannot learn
                their new leader via Raft; if it demotes itself, it cannot
                forward to that leader over the separate public HTTP links.
                It may instead refuse before proposing anything.
                """
                leader = wait_group_leader(BILLING_GROUP)
                peers = [i for i in NODES if i != leader]
                for peer in peers:
                    proxies[BILLING_GROUP, leader, peer].blocked.set()
                    proxies[BILLING_GROUP, peer, leader].blocked.set()
                for peer in peers:
                    proxies[BILLING_GROUP, leader, peer].settle()
                    proxies[BILLING_GROUP, peer, leader].settle()
                observation = group_observation(leader, BILLING_GROUP)
                assert observation["leader_id"] in (None, leader), observation
                return leader

            def heal_billing_leader(leader):
                """Depose the cut leader, then restore every billing link.

                The peers must agree on a new leader *before* the links heal:
                an uncommitted entry in a deposed leader's log is truncated by
                the new leader, while an entry in a leader that is still in
                office would simply commit once a quorum returns.
                """
                peers = [i for i in NODES if i != leader]
                new = wait_group_leader_other(BILLING_GROUP, peers, leader)
                assert new != leader, (new, leader)
                for peer in peers:
                    proxies[BILLING_GROUP, leader, peer].blocked.clear()
                    proxies[BILLING_GROUP, peer, leader].blocked.clear()
                return wait_group_leader(BILLING_GROUP)

            # (a) May have committed. A proxy in front of the billing group
            # leader's S3 listener forwards the whole PUT, reads the daemon's
            # answer and closes without writing one response byte. The client
            # sees a transport error although the mutation may be durable.
            leader = wait_group_leader(BILLING_GROUP)
            drop = ResponseDropProxy(HTTP_PROXY_BASE + leader, s3_port(leader),
                                     "billing S3 PUT")
            threading.Thread(target=drop.serve_forever, daemon=True).start()
            try:
                try:
                    status, _, _ = signed(HTTP_PROXY_BASE + leader, "PUT",
                                          ambiguous_s3_key, b"ambiguous-s3")
                except (OSError, http.client.HTTPException) as error:
                    client_error = type(error).__name__
                else:
                    raise AssertionError(
                        f"ambiguous PUT was answered with HTTP {status}; a lost "
                        "response must surface as a transport error")
                assert drop.observed.wait(30), "response-drop proxy saw no request"
                assert drop.error is None, drop.error
                assert 200 <= drop.upstream_status < 300, drop.upstream_status
            finally:
                drop.shutdown()
                drop.server_close()
            # The daemon answered 2xx, so the mutation is durable: the object
            # must become the complete new value, never absent and never torn.
            # Reads are local-applied, so poll the (possibly new) group leader
            # until its apply loop has caught up with its own commit.
            reader = wait_group_leader(BILLING_GROUP)
            _, headers, body = eventually(
                lambda: signed(s3_port(reader), "GET", ambiguous_s3_key),
                lambda r: r[0] == 200 and r[2] == b"ambiguous-s3",
                "ambiguous S3 write is complete after a lost response")
            assert int(next(v for k, v in headers.items()
                            if k.lower() == "content-length")) == len(body), headers
            s3_outcome = (200, body)
            print(f"PASS ambiguous S3 write: client saw {client_error} after the "
                  f"daemon answered {drop.upstream_status}; the object is the "
                  f"complete new value ({len(body)} bytes)")

            # (b) Cannot have committed. The cut is in place before the mutation
            # is attempted. It is either refused before proposal or cannot
            # reach a quorum; it cannot be forwarded to the new leader. The
            # peers elect a higher-term leader before healing. No retry is made.
            leader = cut_billing_leader()
            try:
                blocked = signed(s3_port(leader), "PUT", blocked_s3_key,
                                 b"cannot-commit")
                assert not 200 <= blocked[0] < 300, blocked
                observation = group_observation(leader, BILLING_GROUP)
                assert observation["leader_id"] in (None, leader), observation
            finally:
                heal_billing_leader(leader)
            # A refused request or an entry that never reached a quorum must
            # leave the old value (absent) after the higher-term leader wins.
            for node in NODES:
                s3_absent(s3_port(node), blocked_s3_key)
            print(f"PASS ambiguous S3 write refused without a quorum: HTTP "
                  f"{blocked[0]} and the object stayed absent on all nodes")

            # The same guarantee for SQL: the INSERT is not retried, so a
            # non-2xx must mean no row rather than a row of another value.
            leader = cut_billing_leader()
            try:
                sql_fault = Pg(pg_port(leader))
                sql_fault.s.settimeout(60)
                try:
                    sql_fault.query("INSERT INTO wire VALUES (96,996);", fail=True)
                    observation = group_observation(leader, BILLING_GROUP)
                    assert observation["leader_id"] in (None, leader), observation
                finally:
                    sql_fault.close()
            finally:
                heal_billing_leader(leader)
            for node in NODES:
                check = Pg(pg_port(node))
                assert check.rows("SELECT balance FROM wire WHERE id=96;") == []
                check.close()
            print("PASS ambiguous SQL write refused without a quorum: the error "
                  "implied rollback and no replica published the row")

            # Once the groups have converged, neither accepted outcome may
            # change across a full process restart of the same storage files.
            for node in nodes:
                node.kill()
            for node in nodes:
                node.wait(timeout=10)
            nodes[:] = start_nodes(NODES)
            for i in NODES:
                wait(http_port(i))
                deadline = time.monotonic()+20
                while "billing/ledger" not in json.loads(request(http_port(i), "GET", "/v1/status")[2])["databases"]:
                    assert time.monotonic() < deadline, "control cache not recovered after ambiguous writes"
                    time.sleep(.05)
            wait_all_leaders()
            if s3_outcome[0] == 200:
                s3_visible(s3_port(2), ambiguous_s3_key, s3_outcome[1])
            else:
                s3_absent(s3_port(2), ambiguous_s3_key)
            for node in NODES:
                s3_absent(s3_port(node), blocked_s3_key)
            restart_sql = Pg(pg_port(2))
            assert restart_sql.rows("SELECT balance FROM wire WHERE id=96;") == []
            assert restart_sql.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            restart_sql.close()
            print("PASS ambiguous SQL/S3 outcomes preserved through full cluster restart")

            # Leave the current billing-group leader alive but remove both other
            # processes. Two of three processes are gone, so the control group
            # has no quorum either: the S3 request is refused at the control
            # lease, while the SQL session opened *before* the loss is refused
            # by the billing group itself. Nothing is acknowledged and no
            # outcome is assumed.
            leader = wait_group_leader(BILLING_GROUP)
            assert leader in NODES, leader
            isolated = Pg(pg_port(leader))
            isolated.s.settimeout(30)
            stopped = [i for i in NODES if i != leader]
            for i in stopped:
                nodes[i-1].kill()
            for i in stopped:
                nodes[i-1].wait(timeout=10)
            result = signed(s3_port(leader), "PUT", "/invoices-2026/no-quorum", b"unknown outcome")
            assert not 200 <= result[0] < 300, result
            isolated.query("INSERT INTO wire VALUES (99,999);", fail=True)
            isolated.close()

            # Restore only one follower first: two out of three form a Raft
            # quorum for every group, so metadata commits again. Every pool is
            # striped over all three failure domains, but a part whose nominal
            # slot sits on the dead node is written to a handoff slot on a live
            # node instead — YDB's handoff-disk rule. The ack therefore still
            # means every part is durable, so the write is accepted rather than
            # refused for a missing domain, and it is not counted as degraded.
            i = stopped[0]
            nodes[i-1] = start_nodes([i])[0]
            wait(http_port(i))
            live = [leader, i]
            wait_group_leader(CONTROL_GROUP, nodes=live)
            wait_group_leader(BILLING_GROUP, nodes=live)
            deadline = time.monotonic()+20
            while True:
                result = signed(s3_port(leader), "PUT", "/invoices-2026/quorum-restored", b"recovered")
                if result[0] == 200:
                    break
                assert time.monotonic() < deadline, result
                time.sleep(.1)
            health = json.loads(request(http_port(leader), "GET", "/v1/pools/health")[2])
            assert health["pools"], health
            assert all(p["degraded_puts"] == 0 for p in health["pools"]), health
            s3_visible(s3_port(leader), "/invoices-2026/restart", b"durable object")
            check = Pg(pg_port(leader))
            assert check.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            check.close()
            i = stopped[1]
            nodes[i-1] = start_nodes([i])[0]
            wait(http_port(i))
            # The blob written while that node was down lives partly on handoff
            # slots. Reads probe the nominal slot and then every spare, so it
            # must still read once the node is back.
            s3_visible(s3_port(leader), "/invoices-2026/quorum-restored", b"recovered",
                       timeout=20)
            print("PASS quorum loss: no SQL/S3 acknowledgement without a Raft quorum; a write with two of three nodes is accepted via handoff slots, stays undegraded and reads back")

            # Partition the billing group's leader from both peers in both
            # directions. Only that group's transport links are cut: the control
            # group and every direct client port stay reachable, which is what
            # makes this a per-group partition rather than a node outage.
            wait_all_leaders()
            old = wait_group_leader(BILLING_GROUP)
            majority = [i for i in NODES if i != old]
            stale_pg = Pg(pg_port(old))
            stale_pg.s.settimeout(30)
            for peer in majority:
                proxies[BILLING_GROUP, old, peer].blocked.set()
                proxies[BILLING_GROUP, peer, old].blocked.set()
            for peer in majority:
                proxies[BILLING_GROUP, old, peer].settle()
                proxies[BILLING_GROUP, peer, old].settle()
            # The bidirectional cut prevents `old` from learning the majority's
            # new leader. If it demotes itself it may refuse before proposing;
            # it cannot forward to that new leader. Neither mutation is retried.
            outcome = {}
            def attempt(name, call):
                try:
                    outcome[name] = call()
                except Exception as error:  # noqa: BLE001 - reported below
                    outcome[name] = error
            writers = [
                threading.Thread(target=attempt, args=(
                    "s3", lambda: signed(s3_port(old), "PUT",
                                         "/invoices-2026/partition-stale", b"minority"))),
                threading.Thread(target=attempt, args=(
                    "sql", lambda: stale_pg.query("INSERT INTO wire VALUES (98,998);", fail=True))),
            ]
            for thread in writers:
                thread.daemon = True
            for thread in writers:
                thread.start()
            for thread in writers:
                thread.join(120)
            assert all(not thread.is_alive() for thread in writers), "partition writers did not finish"
            stale_pg.close()
            # A definitive answer must not be a success; a transport error is
            # also a non-acknowledgement and is recorded rather than retried.
            if isinstance(outcome["s3"], tuple):
                assert not 200 <= outcome["s3"][0] < 300, outcome["s3"]
            else:
                assert isinstance(outcome["s3"], OSError), outcome["s3"]
            # Pg.query(fail=True) raises if there is an acknowledgement or any
            # other unexpected failure. Do not turn that exception into a pass.
            assert_sql_no_ack(outcome["sql"])
            observation = group_observation(old, BILLING_GROUP)
            assert observation["leader_id"] in (None, old), observation
            new = wait_group_leader_other(BILLING_GROUP, majority, old)
            # A Raft majority exists, so metadata commits. The third failure
            # domain is unreachable across the partition, but its parts move to
            # handoff slots on the reachable nodes, so the ack still means every
            # part is durable: the write is accepted on the majority side, while
            # the minority side above could not commit at all.
            accepted = signed(s3_port(new), "PUT", "/invoices-2026/partition-majority", b"majority")
            assert accepted[0] == 200, accepted
            majority_pg = Pg(pg_port(new))
            majority_pg.query("INSERT INTO wire VALUES (97,997);")
            majority_pg.close()
            assert all(p.poll() is None for p in nodes)
            for peer in majority:
                proxies[BILLING_GROUP, old, peer].blocked.clear()
                proxies[BILLING_GROUP, peer, old].blocked.clear()
            wait_group_leader(BILLING_GROUP)
            s3_visible(s3_port(old), "/invoices-2026/restart", b"durable object", timeout=20)
            # The write accepted on the majority side is durable, so the rejoined
            # node must serve it too: its nominal parts may still live on handoff
            # slots, which reads probe.
            s3_visible(s3_port(old), "/invoices-2026/partition-majority", b"majority", timeout=20)
            # The minority side's proposal never reached a quorum, so the healed
            # node must not resurrect it.
            s3_absent(s3_port(old), "/invoices-2026/partition-stale", timeout=20)
            reunited_pg = Pg(pg_port(old))
            assert reunited_pg.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            assert reunited_pg.rows("SELECT balance FROM wire WHERE id=97;") == [["997"]]
            assert reunited_pg.rows("SELECT balance FROM wire WHERE id=98;") == []
            reunited_pg.close()
            print("PASS live bidirectional partition: the minority side refuses without a Raft quorum; the majority side accepts through handoff slots and the data survives the heal")

            leader = wait_group_leader(BILLING_GROUP)
            survivor = next(i for i in NODES if i != leader)
            survivor_pair = tuple(i for i in NODES if i != leader)
            alive = Iscsi(iscsi_port(survivor))
            tx = Pg(pg_port(survivor))
            tx.query("BEGIN; UPDATE wire SET balance=1 WHERE id=1;")
            nodes[leader-1].kill()
            nodes[leader-1].wait(timeout=10)
            # The surviving pair must elect the billing group's new leader
            # before any write to that group; a read is local-applied and may
            # already succeed, but a write proposed without a leader is
            # refused, which is correct cluster behaviour and not placement.
            wait_group_leader_other(BILLING_GROUP, survivor_pair, leader)
            # Existing session survives storage leader loss if its gateway did not die;
            # a dead gateway requires initiator multipath to a different session.
            # Reads still work: the surviving replicas hold every committed
            # fragment. Writes also work: the missing domain's parts move to
            # handoff slots, so an ack still means every part is durable.
            deadline = time.monotonic()+20
            while True:
                try:
                    assert alive.read() == b"C"*512
                    break
                except AssertionError as e:
                    assert time.monotonic() < deadline, e
                    time.sleep(.1)
            # The third domain is gone, but the block's parts move to handoff
            # slots on the surviving nodes, so the write is still acknowledged
            # with every part durable and reads back immediately.
            alive.write(b"D"*512)
            assert alive.read() == b"D"*512
            written = signed(s3_port(survivor), "PUT", "/invoices-2026/after-failover", b"ok")
            assert written[0] == 200, written
            s3_visible(s3_port(survivor), "/invoices-2026/after-failover", b"ok")
            p = Pg(pg_port(survivor))
            assert p.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            # The leader's death changed the term, so this transaction cannot be
            # committed and must be restarted rather than silently applied.
            tx.query("COMMIT;", fail=True)
            tx.query("ROLLBACK;")
            assert tx.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            tx.close()
            # A fresh transaction reaches the same handoff placement, so it
            # commits with one domain offline and is visible immediately.
            fresh = Pg(pg_port(survivor))
            fresh.query("UPDATE wire SET balance=1 WHERE id=1;")
            assert fresh.rows("SELECT balance FROM wire WHERE id=1;") == [["1"]]
            fresh.close()
            p.close()
            # The node returns; what was written while it was away must still be
            # readable, and the key stays writable.
            nodes[leader-1] = start_nodes([leader])[0]
            wait(http_port(leader))
            s3_visible(s3_port(survivor), "/invoices-2026/after-failover", b"ok")
            print("PASS leader SIGKILL: surviving replicas serve reads; a write with one domain offline is acknowledged through handoff slots and survives the node's return")
        except Exception:
            print("Failure diagnostics: per-group OpenRaft status and transport proxy events")
            for i in NODES:
                try:
                    print(f"node {i}: {json.dumps(openraft_status(i))}")
                except (OSError, RuntimeError, http.client.HTTPException) as e:
                    print(f"node {i}: {e}")
            for (group, source, target), proxy in proxies.items():
                with proxy.events_lock:
                    print(f"group {group} {source}->{target}, "
                          f"blocked={proxy.blocked.is_set()}: {list(proxy.events)}")
            for path in sorted((temp / "logs").glob("*.log")):
                print(path.name, path.read_text())
            raise
        finally:
            # A failure before every node was launched leaves `None` entries;
            # masking the real error with an AttributeError hides it.
            for p in nodes:
                if p is not None and p.poll() is None:
                    p.terminate()
            for p in nodes:
                if p is not None:
                    p.wait(timeout=10)
            for proxy in proxies.values():
                proxy.shutdown()
                proxy.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-clients", action="store_true", help="also require psql and AWS CLI")
    parser.add_argument("--libiscsi-dir", help="directory containing libiscsi utilities")
    parser.add_argument("--libiscsi-probe", help="compiled tests/libiscsi_probe.c (temporary volume only)")
    parser.add_argument("--install-snapshot-live", action="store_true",
                        help="attempt the opt-in cross-process InstallSnapshot scenario: "
                             f"a burst of {SNAPSHOT_BURST_ENTRIES} committed entries, "
                             f"measured at {SNAPSHOT_BURST_COST}. Off by default because "
                             f"that burst takes hours on this host; "
                             f"{INSTALL_PROOF_ELSEWHERE}. An unfinished burst is "
                             f"reported as a SKIP with its numbers, never as a pass")
    args = parser.parse_args()
    run(native_clients=args.native_clients, libiscsi_dir=args.libiscsi_dir,
        libiscsi_probe=args.libiscsi_probe,
        install_snapshot_live=args.install_snapshot_live)
