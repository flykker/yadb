#!/usr/bin/env python3
"""Offline protocol integration tests using independent stdlib wire clients.

Run after cargo build --offline: python3 tests/protocols.py
Add --native-clients for psql/AWS CLI, --libiscsi-dir for libiscsi utilities,
and --libiscsi-probe for the compiled libiscsi_probe.c. Not a kernel multipath test.
"""
import datetime
import argparse
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
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
SECRET = "integration-secret-12345"
SUBJECT = "sa:billing-backend"


class PeerProxy(socketserver.ThreadingTCPServer):
    """One directed peer link; toggling it also closes in-flight connections."""
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, source, target):
        self.target = target
        self.blocked = threading.Event()
        super().__init__(("127.0.0.1", 19400 + source * 10 + target), PeerHandler)


class PeerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        if self.server.blocked.is_set():
            return
        try:
            with socket.create_connection(("127.0.0.1", 19000+self.server.target), timeout=2) as upstream:
                upstream.settimeout(2)
                self.request.settimeout(2)
                sockets = (self.request, upstream)
                while not self.server.blocked.is_set():
                    ready, _, _ = select.select(sockets, [], [], .05)
                    for sock in ready:
                        data = sock.recv(65536)
                        if not data:
                            return
                        (upstream if sock is self.request else self.request).sendall(data)
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


def request(port, method, path, body=b"", headers=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    c.request(method, path, body, headers or {})
    r = c.getresponse()
    status, hs, data = r.status, dict(r.getheaders()), r.read()
    c.close()
    return status, hs, data


def signed(port, method, path, body=b"", extra=None, secret=SECRET):
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = now[:8]
    headers = {"host": f"127.0.0.1:{port}", "x-amz-date": now,
               "x-amz-content-sha256": hashlib.sha256(body).hexdigest()}
    headers.update(extra or {})
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
    return request(port, method, path, body, headers)


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
        out = []
        for k, data in self.query(sql):
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
            out.append(row)
        return out

    def close(self):
        self.send(b"X", b"")
        self.s.close()


class Iscsi:
    def __init__(self, port, discovery=False, secret=SECRET, negotiation=None):
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


def wait(port):
    deadline = time.monotonic()+10
    while time.monotonic() < deadline:
        try:
            if request(port, "GET", "/healthz")[0] == 200:
                return
        except OSError:
            pass
        time.sleep(.05)
    raise RuntimeError("daemon not ready")


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
    print("PASS native psql 17 and AWS CLI 2 (Simple Query, transaction, S3 PUT/HEAD/GET/LIST)")


def run(native_clients=False, libiscsi_dir=None, libiscsi_probe=None):
    base = "/data/data/com.foxdebug.acodefree/cache/opencode"
    with tempfile.TemporaryDirectory(prefix="yadb-protocols-", dir=base) as temp:
        temp = Path(temp)
        creds = temp/"credentials.json"
        creds.write_text(json.dumps({"users": {"backend": {"subject": SUBJECT, "secret": SECRET}}}))
        os.chmod(creds, 0o600)
        nodes = []
        logs = []
        proxies = {}
        portals = ",".join(f"127.0.0.1:{19300+i}" for i in range(1, 4))
        def start_node(i):
            peers = ",".join(f"{j}=127.0.0.1:{19000+j if i == j else 19400+i*10+j}" for j in range(1, 4))
            return subprocess.Popen([str(ROOT/"target/debug/yadbd"), "--node-id", str(i),
                "--listen", f"127.0.0.1:{19000+i}", "--peers", peers, "--data-dir", str(temp/f"n{i}"),
                "--protocol-credentials", str(creds), "--pg-listen", f"127.0.0.1:{19100+i}",
                "--iscsi-listen", f"127.0.0.1:{19300+i}", "--iscsi-portals", portals,
                "--s3-listen", f"127.0.0.1:{19200+i}", "--s3-target", "billing/receipts"],
                stdout=logs[i-1], stderr=logs[i-1])
        try:
            for i in range(1, 4):
                for j in range(1, 4):
                    if i != j:
                        proxy = PeerProxy(i, j)
                        proxies[i, j] = proxy
                        threading.Thread(target=proxy.serve_forever, daemon=True).start()
            for i in range(1, 4):
                log = open(temp/f"node{i}.log", "wb")
                logs.append(log)
                nodes.append(start_node(i))
            for i in range(1, 4):
                wait(19000+i)
            body = json.dumps({"yaml": (ROOT/"examples/platform.yaml").read_text()}).encode()
            assert request(19002, "POST", "/v1/apply", body, {"Content-Type": "application/json"})[0] == 200
            for i in range(1, 4):
                deadline = time.monotonic()+10
                while "billing/ledger" not in json.loads(request(19000+i, "GET", "/v1/status")[2])["databases"]:
                    assert time.monotonic() < deadline, "control cache not replicated"
                    time.sleep(.05)
            a, b = Pg(19102), Pg(19103)
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
            a.close()
            print("PASS pgwire password, result framing, multi-statements, session isolation, rollback")

            key = "/invoices-2026/dir/a%20%2B%26.txt"
            assert signed(19202, "PUT", key, b"hello aws")[0] == 200
            assert signed(19203, "GET", key)[2] == b"hello aws"
            assert signed(19203, "GET", key, extra={"range": "bytes=1-4"})[2] == b"ello"
            status, hs, body = signed(19203, "GET", key, extra={"range": "bytes=7-999"})
            assert status == 206 and body == b"ws", (status, hs, body)
            assert next(v for k, v in hs.items() if k.lower() == "content-range") == "bytes 7-8/9"
            assert signed(19203, "HEAD", key)[0] == 200
            # A successful conditional GET must return the version whose ETag
            # was matched, even while a different version is being published.
            race_key = "/invoices-2026/conditional-race"
            original, replacement = b"old version", b"new version has another size"
            status, hs, _ = signed(19202, "PUT", race_key, original)
            assert status == 200
            etag = next(v for k, v in hs.items() if k.lower() == "etag")
            failures = []
            def overwrite():
                try:
                    for i in range(60):
                        result = signed(19202, "PUT", race_key, replacement if i % 2 == 0 else original)
                        assert result[0] == 200, result
                except BaseException as e:
                    failures.append(e)
            writer = threading.Thread(target=overwrite)
            writer.start()
            try:
                for _ in range(120):
                    status, _, body = signed(19202, "GET", race_key, extra={"if-match": etag})
                    assert status == 412 or (status == 200 and body == original), (status, body)
            finally:
                writer.join()
            assert not failures, failures
            print("PASS S3 conditional GET under concurrent overwrites")
            status, _, body = signed(19203, "GET", "/invoices-2026?list-type=2&prefix=dir%2F")
            assert status == 200 and ET.fromstring(body).find("{*}Contents/{*}Key").text == "dir/a +&.txt", body
            assert signed(19202, "GET", key, secret="wrong-password")[0] == 403
            invalid_range = signed(19202, "GET", key, extra={"range": "bytes=900-999"})
            assert invalid_range[0] == 416, invalid_range
            assert signed(19202, "DELETE", key)[0] == 204
            assert signed(19202, "GET", key)[0] == 404
            for name in ["a", "b", "c"]:
                assert signed(19202, "PUT", "/invoices-2026/page/"+name, name.encode())[0] == 200
            _, _, first = signed(19202, "GET", "/invoices-2026?list-type=2&prefix=page%2F&max-keys=1")
            token = ET.fromstring(first).find("{*}NextContinuationToken").text
            _, _, second = signed(19202, "GET", "/invoices-2026?list-type=2&prefix=page%2F&max-keys=1&continuation-token="+token)
            assert ET.fromstring(second).find("{*}Contents/{*}Key").text == "page/b"
            assert signed(19202, "PUT", "/invoices-2026/bad", b"bad", {"x-amz-checksum-crc32": "AAAAAA=="})[0] == 400
            assert signed(19202, "PUT", "/invoices-2026/bad-md5", b"bad", {"content-md5": "AAAAAAAAAAAAAAAAAAAAAA=="})[0] == 400
            assert signed(19202, "POST", "/invoices-2026/multipart?uploads")[0] == 501
            print("PASS S3 SigV4, XML listing, escaped keys, PUT/GET/HEAD/Range/DELETE, bad signatures")

            if native_clients:
                native_client_checks(temp, 19102, 19202)

            Iscsi(19302, secret="wrong-password")
            discovery = Iscsi(19302, discovery=True)
            _, data = discovery.exchange(4, 0x80, b"SendTargets=All\0")
            assert data.count(b"TargetAddress=") == 3, data
            discovery.s.close()
            x, y = Iscsi(19302), Iscsi(19303)
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
            limited = Iscsi(19302, negotiation={"ImmediateData": "No", "MaxBurstLength": "4096", "FirstBurstLength": "4096"})
            assert limited.negotiated["ImmediateData"] == "No", limited.negotiated
            assert limited.negotiated["MaxBurstLength"] == "4096", limited.negotiated
            payload = bytes(range(256))*64
            limited.write(payload, lba=512, immediate=False)
            assert limited.read(lba=512, blocks=32) == payload
            limited.s.close()
            print("PASS iSCSI ImmediateData=No, negotiated burst limits, multiple R2Ts")
            if libiscsi_dir:
                iscsi_url = f"iscsi://backend%{SECRET}@127.0.0.1:19302/iqn.2026-11.io.yadb:billing:vm-root/0"
                output = native([str(Path(libiscsi_dir)/"iscsi-inq"), "-d", "3", iscsi_url])
                assert "YADB" in output, output
                print(native([str(Path(libiscsi_dir)/"iscsi-readcapacity16"), iscsi_url]))
                print("PASS native libiscsi INQUIRY and READ CAPACITY(16)")
            if libiscsi_probe:
                probe_env = os.environ.copy()
                probe_env["YADB_TEST_SECRET"] = SECRET
                print(native([libiscsi_probe, "127.0.0.1:19302"], probe_env).strip())

            # Abruptly stop every process after acknowledged writes, retaining
            # the same storage directories. One SQL transaction remains open.
            assert signed(19202, "PUT", "/invoices-2026/restart", b"durable object")[0] == 200
            x.write(b"R"*512, lba=4096)
            uncommitted = Pg(19102)
            uncommitted.query("BEGIN; UPDATE wire SET balance=7 WHERE id=1;")
            for node in nodes:
                node.kill()
            for node in nodes:
                node.wait(timeout=10)
            uncommitted.s.close()
            b.s.close()
            x.s.close()
            y.s.close()
            nodes[:] = [start_node(i) for i in range(1, 4)]
            for i in range(1, 4):
                wait(19000+i)
            for i in range(1, 4):
                deadline = time.monotonic()+20
                while "billing/ledger" not in json.loads(request(19000+i, "GET", "/v1/status")[2])["databases"]:
                    assert time.monotonic() < deadline, "control cache not recovered"
                    time.sleep(.05)
            restored = Pg(19102)
            assert restored.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            restored.close()
            assert signed(19203, "GET", "/invoices-2026/restart")[2] == b"durable object"
            disk = Iscsi(19303)
            assert disk.read(lba=4096) == b"R"*512
            disk.s.close()
            print("PASS full cluster SIGKILL/restart: committed SQL, S3, iSCSI and uncommitted SQL rollback")

            # Leave the current leader alive but remove both voting peers. A
            # rejected or timed-out write may still commit after recovery, so do
            # not assume its final outcome; only assert it was not acknowledged.
            leader = json.loads(request(19002, "GET", "/raft/status")[2])["leader_id"]
            assert leader in (1, 2, 3), leader
            stopped = [i for i in range(1, 4) if i != leader]
            for i in stopped:
                nodes[i-1].kill()
            for i in stopped:
                nodes[i-1].wait(timeout=10)
            result = signed(19200+leader, "PUT", "/invoices-2026/no-quorum", b"unknown outcome")
            assert not 200 <= result[0] < 300, result
            isolated = Pg(19100+leader)
            isolated.s.settimeout(10)
            isolated.query("INSERT INTO wire VALUES (99,999);", fail=True)
            isolated.close()

            # Restore only one follower first: two out of three form a quorum.
            i = stopped[0]
            nodes[i-1] = start_node(i)
            wait(19000+i)
            deadline = time.monotonic()+20
            while True:
                result = signed(19200+leader, "PUT", "/invoices-2026/quorum-restored", b"recovered")
                if result[0] == 200:
                    break
                assert time.monotonic() < deadline, result
                time.sleep(.1)
            assert signed(19200+leader, "GET", "/invoices-2026/restart")[2] == b"durable object"
            assert signed(19200+leader, "GET", "/invoices-2026/quorum-restored")[2] == b"recovered"
            check = Pg(19100+leader)
            assert check.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            check.close()
            i = stopped[1]
            nodes[i-1] = start_node(i)
            wait(19000+i)
            print("PASS quorum loss: no SQL/S3 acknowledgement without peers; majority recovery")

            # Partition the live leader from both peers in both directions.
            # Direct client ports stay reachable on all three processes.
            deadline = time.monotonic()+20
            while True:
                statuses = [json.loads(request(19000+i, "GET", "/raft/status")[2]) for i in range(1, 4)]
                leaders = [s["node_id"] for s in statuses if s["role"] == "Leader"]
                if len(leaders) == 1 and all(s["leader_id"] == leaders[0] for s in statuses):
                    break
                assert time.monotonic() < deadline, statuses
                time.sleep(.1)
            old = leaders[0]
            majority = [i for i in range(1, 4) if i != old]
            for peer in majority:
                proxies[old, peer].blocked.set()
                proxies[peer, old].blocked.set()
            stale = signed(19200+old, "PUT", "/invoices-2026/partition-stale", b"minority")
            assert not 200 <= stale[0] < 300, stale
            stale_pg = Pg(19100+old)
            stale_pg.s.settimeout(10)
            stale_pg.query("INSERT INTO wire VALUES (98,998);", fail=True)
            stale_pg.close()
            deadline = time.monotonic()+20
            while True:
                statuses = [json.loads(request(19000+i, "GET", "/raft/status")[2]) for i in majority]
                leaders = [s["node_id"] for s in statuses if s["role"] == "Leader"]
                if len(leaders) == 1:
                    break
                assert time.monotonic() < deadline, statuses
                time.sleep(.1)
            new = leaders[0]
            result = signed(19200+new, "PUT", "/invoices-2026/partition-majority", b"majority")
            assert result[0] == 200, result
            majority_pg = Pg(19100+new)
            majority_pg.query("INSERT INTO wire VALUES (97,997);")
            majority_pg.close()
            assert all(p.poll() is None for p in nodes)
            for proxy in proxies.values():
                proxy.blocked.clear()
            deadline = time.monotonic()+20
            while True:
                statuses = [json.loads(request(19000+i, "GET", "/raft/status")[2]) for i in range(1, 4)]
                if all(s["leader_id"] == new for s in statuses) and statuses[old-1]["role"] == "Follower":
                    break
                assert time.monotonic() < deadline, statuses
                time.sleep(.1)
            assert signed(19200+old, "GET", "/invoices-2026/partition-majority")[2] == b"majority"
            assert signed(19200+old, "GET", "/invoices-2026/restart")[2] == b"durable object"
            reunited_pg = Pg(19100+old)
            assert reunited_pg.rows("SELECT balance FROM wire WHERE id=97;") == [["997"]]
            reunited_pg.close()
            print("PASS live bidirectional partition: minority rejected, majority elected, rejoined")

            leader = json.loads(request(19002, "GET", "/raft/status")[2])["leader_id"]
            survivor = next(i for i in range(1, 4) if i != leader)
            alive = Iscsi(19300+survivor)
            tx = Pg(19100+survivor)
            tx.query("BEGIN; UPDATE wire SET balance=1 WHERE id=1;")
            nodes[leader-1].kill()
            nodes[leader-1].wait(timeout=10)
            # Existing session survives storage leader loss if its gateway did not die;
            # a dead gateway requires initiator multipath to a different session.
            alive.write(b"D"*512)
            assert alive.read() == b"D"*512
            assert signed(19200+survivor, "PUT", "/invoices-2026/after-failover", b"ok")[0] == 200
            p = Pg(19100+survivor)
            assert p.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            tx.query("COMMIT;", fail=True)
            tx.query("ROLLBACK;")
            assert tx.rows("SELECT balance FROM wire WHERE id=1;") == [["90"]]
            tx.close()
            p.close()
            print("PASS leader SIGKILL: surviving iSCSI portal, pgwire and S3 remain usable")
        except Exception:
            for log in logs:
                log.flush()
            for path in temp.glob("*.log"):
                print(path.name, path.read_text())
            raise
        finally:
            for p in nodes:
                if p.poll() is None:
                    p.terminate()
            for p in nodes:
                p.wait(timeout=10)
            for log in logs:
                log.close()
            for proxy in proxies.values():
                proxy.shutdown()
                proxy.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-clients", action="store_true", help="also require psql and AWS CLI")
    parser.add_argument("--libiscsi-dir", help="directory containing libiscsi utilities")
    parser.add_argument("--libiscsi-probe", help="compiled tests/libiscsi_probe.c (temporary volume only)")
    args = parser.parse_args()
    run(native_clients=args.native_clients, libiscsi_dir=args.libiscsi_dir,
        libiscsi_probe=args.libiscsi_probe)
