//! iSCSI ERL=0, one connection/session, ordered commands; CHAP only, no digests.
//! Multipath recovery is performed by the initiator, not by migrating TCP sessions.
use super::*;
use crate::AppState;
use yadb_control::Role;

const MAX_IO: usize = 1024 * 1024;
const IQN: &str = "iqn.2026-11.io.yadb:";
struct Pdu {
    h: [u8; 48],
    data: Vec<u8>,
}
impl Pdu {
    fn read(c: &mut TcpStream) -> io::Result<Self> {
        let mut h = [0; 48];
        c.read_exact(&mut h)?;
        let n = ((h[5] as usize) << 16) | ((h[6] as usize) << 8) | h[7] as usize;
        if h[4] != 0 || n > MAX_IO {
            return Err(io::Error::other("unsupported AHS or oversized PDU"));
        }
        let mut data = vec![0; n];
        c.read_exact(&mut data)?;
        let mut pad = [0; 3];
        c.read_exact(&mut pad[..(4 - n % 4) % 4])?;
        Ok(Self { h, data })
    }
    fn new(op: u8, flags: u8, itt: &[u8]) -> Self {
        let mut h = [0; 48];
        h[0] = op;
        h[1] = flags;
        h[16..20].copy_from_slice(itt);
        Self {
            h,
            data: Vec::new(),
        }
    }
    fn send(&mut self, c: &mut TcpStream) -> io::Result<()> {
        let n = self.data.len();
        self.h[5] = (n >> 16) as u8;
        self.h[6] = (n >> 8) as u8;
        self.h[7] = n as u8;
        c.write_all(&self.h)?;
        c.write_all(&self.data)?;
        c.write_all(&[0; 3][..(4 - n % 4) % 4])
    }
}
fn u32at(b: &[u8], i: usize) -> u32 {
    u32::from_be_bytes(b[i..i + 4].try_into().unwrap())
}
fn u64at(b: &[u8], i: usize) -> u64 {
    u64::from_be_bytes(b[i..i + 8].try_into().unwrap())
}
fn fields(data: &[u8]) -> BTreeMap<String, String> {
    String::from_utf8_lossy(data)
        .split('\0')
        .filter_map(|s| {
            s.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect()
}
fn text(p: &mut Pdu, k: &str, v: &str) {
    p.data.extend_from_slice(format!("{k}={v}\0").as_bytes());
}

struct Session {
    stat: u32,
    cmd: u32,
    limit: usize,
    subject: String,
    target: String,
    discovery: bool,
    immediate: bool,
    max_burst: usize,
    first_burst: usize,
}
impl Session {
    fn sequence(&mut self, p: &mut Pdu, increment: bool) {
        p.h[24..28].copy_from_slice(&self.stat.to_be_bytes());
        if increment {
            self.stat = self.stat.wrapping_add(1);
        }
        p.h[28..32].copy_from_slice(&self.cmd.to_be_bytes());
        p.h[32..36].copy_from_slice(&self.cmd.to_be_bytes());
    }
}

pub fn connection(
    mut c: TcpStream,
    st: AppState,
    creds: Credentials,
    portals: Vec<String>,
) -> io::Result<()> {
    let challenge = random::<32>()?;
    let chap_id = random::<1>()?[0];
    let mut s = Session {
        stat: 0,
        cmd: 0,
        limit: 8192,
        subject: String::new(),
        target: String::new(),
        discovery: false,
        immediate: true,
        max_burst: MAX_IO,
        first_burst: 65536,
    };
    let mut offered = false;
    let mut authenticated = false;
    let mut full = false;
    for _ in 0..16 {
        let p = Pdu::read(&mut c)?;
        if p.h[0] & 0x3f != 3 {
            return Err(io::Error::other("login required"));
        }
        let kv = fields(&p.data);
        s.cmd = u32at(&p.h, 24);
        if let Some(v) = kv.get("TargetName") {
            s.target = v.clone();
        }
        if let Some(v) = kv.get("SessionType") {
            s.discovery = v == "Discovery";
        }
        if let Some(v) = kv.get("MaxRecvDataSegmentLength") {
            s.limit = v.parse::<usize>().unwrap_or(8192).clamp(512, MAX_IO);
        }
        let stage = (p.h[1] >> 2) & 3;
        let mut r = Pdu::new(0x23, stage << 2, &p.h[16..20]);
        r.h[8..14].copy_from_slice(&p.h[8..14]);
        r.h[14..16].copy_from_slice(&1u16.to_be_bytes());
        let mut failed = false;
        if stage == 0 {
            if let (Some(name), Some(response)) = (kv.get("CHAP_N"), kv.get("CHAP_R")) {
                let cred = creds.users.get(name);
                authenticated = cred.is_some_and(|v| {
                    let mut input = vec![chap_id];
                    input.extend_from_slice(v.secret.as_bytes());
                    input.extend_from_slice(&challenge);
                    response
                        .strip_prefix("0x")
                        .and_then(|v| unhex(v).ok())
                        .is_some_and(|got| equal(&got, &md5(&input)))
                });
                if let Some(cred) = cred.filter(|_| authenticated) {
                    s.subject = cred.subject.clone();
                }
                failed = !authenticated || kv.contains_key("CHAP_I"); // mutual CHAP not implemented
            } else if kv.contains_key("CHAP_A") && offered {
                if !kv["CHAP_A"].split(',').any(|s| s == "5") {
                    failed = true;
                }
                text(&mut r, "CHAP_A", "5");
                text(&mut r, "CHAP_I", &chap_id.to_string());
                text(&mut r, "CHAP_C", &format!("0x{}", hex(&challenge)));
            } else {
                if !kv
                    .get("AuthMethod")
                    .is_some_and(|v| v.split(',').any(|v| v == "CHAP"))
                {
                    failed = true;
                }
                offered = true;
                text(&mut r, "AuthMethod", "CHAP");
            }
            if authenticated && p.h[1] & 0x80 != 0 {
                r.h[1] = 0x81;
            }
        } else if stage == 1 && authenticated {
            // Boolean AND and numerical minimum negotiation (RFC 7143).
            // In particular, never turn an initiator's ImmediateData=No into Yes.
            if let Some(v) = kv.get("ImmediateData") {
                match v.as_str() {
                    "No" => s.immediate = false,
                    "Yes" => {}
                    _ => failed = true,
                }
            }
            for (key, limit) in [
                ("MaxBurstLength", &mut s.max_burst),
                ("FirstBurstLength", &mut s.first_burst),
            ] {
                if let Some(v) = kv.get(key) {
                    match v.parse::<usize>() {
                        Ok(n) if (512..=0xff_ffff).contains(&n) => *limit = (*limit).min(n),
                        _ => failed = true,
                    }
                }
            }
            s.first_burst = s.first_burst.min(s.max_burst);
            for key in ["HeaderDigest", "DataDigest"] {
                if kv
                    .get(key)
                    .is_some_and(|v| !v.split(',').any(|v| v == "None"))
                {
                    failed = true;
                }
            }
            for (k, v) in [
                ("HeaderDigest", "None"),
                ("DataDigest", "None"),
                ("MaxConnections", "1"),
                ("InitialR2T", "Yes"),
                ("MaxRecvDataSegmentLength", "1048576"),
                ("DefaultTime2Wait", "2"),
                ("DefaultTime2Retain", "0"),
                ("MaxOutstandingR2T", "1"),
                ("DataPDUInOrder", "Yes"),
                ("DataSequenceInOrder", "Yes"),
                ("ErrorRecoveryLevel", "0"),
                ("TargetPortalGroupTag", "1"),
            ] {
                text(&mut r, k, v);
            }
            text(
                &mut r,
                "ImmediateData",
                if s.immediate { "Yes" } else { "No" },
            );
            text(&mut r, "MaxBurstLength", &s.max_burst.to_string());
            text(&mut r, "FirstBurstLength", &s.first_burst.to_string());
            if !s.discovery && target(&st, &s, Role::ReadOnly).is_err() {
                failed = true;
            }
            if p.h[1] & 0x80 != 0 && p.h[1] & 3 == 3 {
                r.h[1] = 0x87;
                full = true;
            }
        } else {
            failed = true;
        }
        if failed {
            r.h[36] = 2;
            r.h[37] = 1;
        }
        s.sequence(&mut r, true);
        r.send(&mut c)?;
        if failed {
            return Ok(());
        }
        if full {
            break;
        }
    }
    if !full {
        return Err(io::Error::other("incomplete login"));
    }
    loop {
        let p = Pdu::read(&mut c)?;
        let op = p.h[0] & 0x3f;
        if p.h[0] & 0x40 == 0 {
            if u32at(&p.h, 24) != s.cmd {
                return Err(io::Error::other("out of order CmdSN"));
            }
            s.cmd = s.cmd.wrapping_add(1);
        }
        match op {
            0 => {
                if p.h[16..20] != [255; 4] {
                    let mut r = Pdu::new(0x20, 0x80, &p.h[16..20]);
                    r.h[20..24].fill(255);
                    r.data = p.data;
                    s.sequence(&mut r, true);
                    r.send(&mut c)?;
                }
            }
            4 => {
                let mut r = Pdu::new(0x24, 0x80, &p.h[16..20]);
                r.h[20..24].fill(255);
                let kv = fields(&p.data);
                if s.discovery && kv.contains_key("SendTargets") {
                    let cp = st.control.read().unwrap();
                    for name in cp.block_volumes.keys() {
                        let Some((proj, _)) = name.split_once('/') else {
                            continue;
                        };
                        if cp.authorize(proj, &s.subject, Role::ReadOnly).is_err() {
                            continue;
                        }
                        let iqn = format!("{IQN}{}", name.replace('/', ":"));
                        if kv["SendTargets"] != "All" && kv["SendTargets"] != iqn {
                            continue;
                        }
                        text(&mut r, "TargetName", &iqn);
                        for addr in &portals {
                            text(&mut r, "TargetAddress", &format!("{addr},1"));
                        }
                    }
                }
                if r.data.len() > s.limit {
                    return Err(io::Error::other("discovery result exceeds segment limit"));
                }
                s.sequence(&mut r, true);
                r.send(&mut c)?;
            }
            6 => {
                let mut r = Pdu::new(0x26, 0x80, &p.h[16..20]);
                s.sequence(&mut r, true);
                r.send(&mut c)?;
                return Ok(());
            }
            1 if !s.discovery => command(&mut c, &st, &mut s, p)?,
            2 => {
                let mut r = Pdu::new(0x22, 0x80, &p.h[16..20]);
                r.h[2] = 5;
                s.sequence(&mut r, true);
                r.send(&mut c)?;
            }
            _ => return Err(io::Error::other("unsupported iSCSI opcode")),
        }
    }
}

fn target(st: &AppState, s: &Session, role: Role) -> io::Result<(u32, String, String, u64, bool)> {
    let name = s
        .target
        .strip_prefix(IQN)
        .ok_or_else(|| io::Error::other("unknown target"))?;
    let (proj, volume) = name
        .split_once(':')
        .ok_or_else(|| io::Error::other("target must be project:volume"))?;
    let cp = st.control.read().unwrap();
    let pid = cp
        .authorize(proj, &s.subject, role)
        .map_err(io::Error::other)?;
    let spec = cp
        .block_volumes
        .get(&format!("{proj}/{volume}"))
        .ok_or_else(|| io::Error::other("unknown volume"))?;
    if spec.size_bytes < 512 || spec.size_bytes % 512 != 0 {
        return Err(io::Error::other("unaligned volume size"));
    }
    Ok((
        pid,
        proj.into(),
        volume.into(),
        spec.size_bytes,
        cp.authorize(proj, &s.subject, Role::ReadWrite).is_ok(),
    ))
}
fn sense(c: &mut TcpStream, s: &mut Session, p: &Pdu, key: u8, asc: u8) -> io::Result<()> {
    let mut r = Pdu::new(0x21, 0x80, &p.h[16..20]);
    r.h[3] = 2;
    r.data = vec![
        0, 18, 0x70, 0, key, 0, 0, 0, 0, 10, 0, 0, 0, 0, asc, 0, 0, 0, 0, 0,
    ];
    s.sequence(&mut r, true);
    r.send(c)
}
fn good(c: &mut TcpStream, s: &mut Session, p: &Pdu) -> io::Result<()> {
    let mut r = Pdu::new(0x21, 0x80, &p.h[16..20]);
    s.sequence(&mut r, true);
    r.send(c)
}
fn send_data(c: &mut TcpStream, s: &mut Session, p: &Pdu, mut data: Vec<u8>) -> io::Result<()> {
    let expected = u32at(&p.h, 20) as usize;
    data.truncate(expected);
    if data.is_empty() {
        let mut r = Pdu::new(0x21, 0x80, &p.h[16..20]);
        if expected != 0 {
            r.h[1] |= 2; // Underflow: no bytes of the expected transfer were sent.
            r.h[44..48].copy_from_slice(&(expected as u32).to_be_bytes());
        }
        s.sequence(&mut r, true);
        return r.send(c);
    }
    let total = data.len();
    let mut offset = 0;
    let mut seq = 0u32;
    while offset < total {
        let burst_end = (offset + s.max_burst).min(total);
        while offset < burst_end {
            let end = (offset + s.limit).min(burst_end);
            let chunk = &data[offset..end];
            let last = end == total;
            let flags = if last {
                0x81
            } else if end == burst_end {
                0x80
            } else {
                0
            };
            let mut r = Pdu::new(0x25, flags, &p.h[16..20]);
            r.h[20..24].fill(255);
            if last && total < expected {
                r.h[1] |= 2;
                r.h[44..48].copy_from_slice(&((expected - total) as u32).to_be_bytes());
            }
            r.h[36..40].copy_from_slice(&seq.to_be_bytes());
            r.h[40..44].copy_from_slice(&(offset as u32).to_be_bytes());
            s.sequence(&mut r, last);
            r.data = chunk.to_vec();
            r.send(c)?;
            offset = end;
            seq = seq.wrapping_add(1);
        }
    }
    Ok(())
}
fn receive_data(c: &mut TcpStream, s: &mut Session, p: &Pdu, len: usize) -> io::Result<Vec<u8>> {
    if len > MAX_IO
        || p.data.len() > len
        || p.data.len() > s.first_burst
        || (!s.immediate && !p.data.is_empty())
    {
        return Err(io::Error::other("transfer size invalid"));
    }
    let mut data = p.data.clone();
    let mut r2tsn = 0u32;
    while data.len() < len {
        let burst_end = (data.len() + s.max_burst).min(len);
        let mut r = Pdu::new(0x31, 0x80, &p.h[16..20]);
        r.h[8..16].copy_from_slice(&p.h[8..16]);
        r.h[20..24].copy_from_slice(&1u32.to_be_bytes());
        r.h[40..44].copy_from_slice(&(data.len() as u32).to_be_bytes());
        r.h[36..40].copy_from_slice(&r2tsn.to_be_bytes());
        r.h[44..48].copy_from_slice(&((burst_end - data.len()) as u32).to_be_bytes());
        s.sequence(&mut r, false);
        r.send(c)?;
        let mut seq = 0;
        while data.len() < burst_end {
            let d = Pdu::read(c)?;
            if d.h[0] & 0x3f != 5
                || d.h[16..20] != p.h[16..20]
                || u32at(&d.h, 20) != 1
                || u32at(&d.h, 36) != seq
                || u32at(&d.h, 40) as usize != data.len()
                || d.data.is_empty()
                || data.len() + d.data.len() > burst_end
            {
                return Err(io::Error::other("invalid Data-Out"));
            }
            data.extend(d.data);
            seq += 1;
            if (d.h[1] & 0x80 != 0) != (data.len() == burst_end) {
                return Err(io::Error::other("invalid final Data-Out"));
            }
        }
        r2tsn = r2tsn.wrapping_add(1);
    }
    Ok(data)
}
fn command(c: &mut TcpStream, st: &AppState, s: &mut Session, p: Pdu) -> io::Result<()> {
    if p.h[8..16] != [0; 8] {
        return sense(c, s, &p, 5, 0x25);
    }
    let op = p.h[32];
    let write = matches!(op, 0x2a | 0x8a | 0x42);
    let (pid, project, volume, size, rw) = match target(
        st,
        s,
        if write {
            Role::ReadWrite
        } else {
            Role::ReadOnly
        },
    ) {
        Ok(v) => v,
        Err(_) => return sense(c, s, &p, 7, 0x27),
    };
    let cdb = &p.h[32..48];
    let base = format!("/block/{}/{}", url(&project), url(&volume));
    match op {
        0x00 | 0x1b | 0x1e => good(c, s, &p),
        0x12 => {
            let data = if cdb[1] & 1 == 0 {
                let mut d = vec![0; 36];
                d[2] = 6;
                d[3] = 2;
                d[4] = 31;
                d[7] = 2;
                d[8..16].copy_from_slice(b"YADB    ");
                d[16..32].copy_from_slice(b"Distributed Disk");
                d[32..36].copy_from_slice(b"0001");
                d
            } else {
                let identity = format!("yadb:{pid}:{volume}");
                let id = sha(identity.as_bytes());
                let body = match cdb[2] {
                    0 => vec![0, 0x80, 0x83, 0xb0, 0xb2],
                    0x80 => id[..32].as_bytes().to_vec(),
                    0x83 => {
                        let name = format!("YADB    {}", &id[..32]);
                        let mut b = vec![2, 1, 0, name.len() as u8];
                        b.extend_from_slice(name.as_bytes());
                        b
                    }
                    0xb0 => {
                        let mut b = vec![0; 60];
                        b[2..4].copy_from_slice(&8u16.to_be_bytes());
                        b[4..8].copy_from_slice(&2048u32.to_be_bytes());
                        b[8..12].copy_from_slice(&8u32.to_be_bytes());
                        b[16..20].copy_from_slice(&2048u32.to_be_bytes());
                        b[20..24].copy_from_slice(&1u32.to_be_bytes());
                        b[24..28].copy_from_slice(&8u32.to_be_bytes());
                        b[28..32].copy_from_slice(&0x80000000u32.to_be_bytes());
                        b
                    }
                    0xb2 => vec![0, 0x80, 2, 0],
                    _ => return sense(c, s, &p, 5, 0x24),
                };
                let mut d = vec![0, cdb[2]];
                d.extend_from_slice(&(body.len() as u16).to_be_bytes());
                d.extend(body);
                d
            };
            let alloc = u16::from_be_bytes([cdb[3], cdb[4]]) as usize;
            send_data(c, s, &p, data.into_iter().take(alloc).collect())
        }
        0x25 => {
            let mut d = Vec::new();
            d.extend_from_slice(&((size / 512 - 1).min(u32::MAX as u64) as u32).to_be_bytes());
            d.extend_from_slice(&512u32.to_be_bytes());
            send_data(c, s, &p, d)
        }
        0x9e if cdb[1] & 31 == 0x10 => {
            let mut d = vec![0; 32];
            d[..8].copy_from_slice(&(size / 512 - 1).to_be_bytes());
            d[8..12].copy_from_slice(&512u32.to_be_bytes());
            d[13] = 3;
            d[14] = 0xc0;
            send_data(c, s, &p, d)
        }
        0xa0 => {
            let mut d = vec![0; 16];
            d[3] = 8;
            send_data(c, s, &p, d)
        }
        0x03 => send_data(
            c,
            s,
            &p,
            vec![0x70, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        0x1a | 0x5a => {
            let page = cdb[2] & 0x3f;
            if page != 8 && page != 0x3f {
                return sense(c, s, &p, 5, 0x24);
            }
            let mut d = if op == 0x1a {
                vec![0, 0, if rw { 0 } else { 0x80 }, 0]
            } else {
                vec![0, 0, 0, if rw { 0 } else { 0x80 }, 0, 0, 0, 0]
            };
            let mut cache = vec![0; 20];
            cache[0] = 8;
            cache[1] = 18; // WCE=0: writes are durable before GOOD
            d.extend(cache);
            let n = d.len();
            if op == 0x1a {
                d[0] = (n - 1) as u8;
            } else {
                d[..2].copy_from_slice(&((n - 2) as u16).to_be_bytes());
            }
            send_data(c, s, &p, d)
        }
        0x28 | 0x88 | 0x2a | 0x8a => {
            let (lba, blocks) = if op == 0x28 || op == 0x2a {
                (
                    u32at(cdb, 2) as u64,
                    u16::from_be_bytes([cdb[7], cdb[8]]) as u64,
                )
            } else {
                (u64at(cdb, 2), u32at(cdb, 10) as u64)
            };
            let Some(offset) = lba.checked_mul(512) else {
                return sense(c, s, &p, 5, 0x21);
            };
            let len = blocks * 512;
            if len > MAX_IO as u64 || offset.checked_add(len).is_none_or(|v| v > size) {
                return sense(c, s, &p, 5, 0x21);
            }
            if u32at(&p.h, 20) as u64 != len {
                return sense(c, s, &p, 5, 0x24);
            }
            if write {
                let data = receive_data(c, s, &p, len as usize)?;
                match http(
                    &st.listen_addr,
                    "PUT",
                    &format!("{base}?offset={offset}"),
                    &s.subject,
                    &data,
                ) {
                    Ok((200, _)) => good(c, s, &p),
                    _ => sense(c, s, &p, 2, 0x08),
                }
            } else {
                match http(
                    &st.listen_addr,
                    "GET",
                    &format!("{base}?offset={offset}&len={len}&consistent=true"),
                    &s.subject,
                    &[],
                ) {
                    Ok((200, data)) if data.len() == len as usize => send_data(c, s, &p, data),
                    _ => sense(c, s, &p, 2, 0x08),
                }
            }
        }
        0x35 | 0x91 => {
            match http(
                &st.listen_addr,
                "GET",
                &format!("{base}?offset=0&len=0&consistent=true"),
                &s.subject,
                &[],
            ) {
                Ok((200, _)) => good(c, s, &p),
                _ => sense(c, s, &p, 2, 0x08),
            }
        }
        0x42 => {
            let len = u16::from_be_bytes([cdb[7], cdb[8]]) as usize;
            let data = receive_data(c, s, &p, len)?;
            if len != 24 || data.len() < 24 || data[2..4] != 16u16.to_be_bytes() {
                return sense(c, s, &p, 5, 0x26);
            }
            let Some(offset) = u64at(&data, 8).checked_mul(512) else {
                return sense(c, s, &p, 5, 0x21);
            };
            let len = u32at(&data, 16) as u64 * 512;
            if offset % 4096 != 0 || len % 4096 != 0 {
                return sense(c, s, &p, 5, 0x26);
            }
            let body = serde_json::to_vec(&serde_json::json!({"offset":offset,"len":len})).unwrap();
            match http(
                &st.listen_addr,
                "POST",
                &format!("{base}/unmap"),
                &s.subject,
                &body,
            ) {
                Ok((200, _)) => good(c, s, &p),
                _ => sense(c, s, &p, 2, 0x08),
            }
        }
        _ => sense(c, s, &p, 5, 0x20),
    }
}

// RFC 1321 MD5 is required by CHAP algorithm 5 (not used for password storage).
pub(super) fn md5(input: &[u8]) -> [u8; 16] {
    let mut v = input.to_vec();
    let bits = (v.len() as u64) * 8;
    v.push(0x80);
    while v.len() % 64 != 56 {
        v.push(0);
    }
    v.extend_from_slice(&bits.to_le_bytes());
    let mut state = [0x67452301u32, 0xefcdab89, 0x98badcfe, 0x10325476];
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];
    for chunk in v.chunks_exact(64) {
        let mut m = [0u32; 16];
        for (i, b) in chunk.chunks_exact(4).enumerate() {
            m[i] = u32::from_le_bytes(b.try_into().unwrap());
        }
        let [a0, b0, c0, d0] = state;
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = if i < 16 {
                ((b & c) | (!b & d), i)
            } else if i < 32 {
                ((d & b) | (!d & c), (5 * i + 1) % 16)
            } else if i < 48 {
                (b ^ c ^ d, (3 * i + 5) % 16)
            } else {
                (c ^ (b | !d), (7 * i) % 16)
            };
            let next = b.wrapping_add(
                a.wrapping_add(f)
                    .wrapping_add(K[i])
                    .wrapping_add(m[g])
                    .rotate_left(S[i]),
            );
            a = d;
            d = c;
            c = b;
            b = next;
        }
        state = [
            a0.wrapping_add(a),
            b0.wrapping_add(b),
            c0.wrapping_add(c),
            d0.wrapping_add(d),
        ];
    }
    let mut out = [0; 16];
    for (i, n) in state.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&n.to_le_bytes());
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chap_vectors() {
        assert_eq!(hex(&md5(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(hex(&md5(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            hex(&md5(&vec![b'a'; 1000000])),
            "7707d6ae4e027c70eea2a935c2296f21"
        );
    }
}
