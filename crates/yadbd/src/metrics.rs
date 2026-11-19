//! Prometheus text exposition for the read-only health surface.
//!
//! The data is the same data `/v1/pools/health` serves: one row per pool, one
//! row per slot, the last collection pass, the last repair scan and one row per
//! metadata group. Nothing here takes a lease, writes metadata or contacts a
//! peer beyond the slot health probe the health endpoint already performs, so a
//! scrape cannot change what it observes.
//!
//! Rendering is a pure function over those rows, which is what the unit tests
//! below exercise. The handler only gathers the rows and hands them to it.

use std::fmt::Write as _;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use yadb_blob::{PoolHealthSnapshot, PoolHealthState};

use crate::gc::SweepOutcome;
use crate::{api_err, group_health_rows, AppState, ScrubSummary};

/// `GET /metrics` — the Prometheus scrape endpoint.
///
/// The slot health probe is the same call `/v1/pools/health` makes, including
/// the remote probe: reporting a remote slot from the last cached value would
/// publish a stale `online` gauge. It therefore runs on the blocking pool and a
/// scrape costs what one health read costs.
pub(crate) async fn handle_metrics(State(st): State<AppState>) -> Response {
    let st_clone = st.clone();
    let pools = match tokio::task::spawn_blocking(move || st_clone.collect_pool_health(true)).await {
        Ok(pools) => pools,
        Err(error) => {
            return api_err(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    };
    // Clone and release each lock before the first await: the group rows are
    // async and a `RwLockReadGuard` must not cross an await point.
    let last_scrub = st.last_scrub.read().unwrap().clone();
    let last_gc = st.last_gc.read().unwrap().clone();
    let groups = group_health_rows(&st).await;
    let body = render(
        st.meta.node_id(),
        if st.meta.is_openraft() {
            "openraft"
        } else {
            "single-node"
        },
        &pools,
        last_gc.as_ref(),
        last_scrub.as_ref(),
        &groups,
    );
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

/// Escapes a Prometheus label value: backslash, double quote and newline are the
/// only characters the format reserves.
fn label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Escapes a `# HELP` string, which reserves backslash and newline (not quote).
fn help(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\n', "\\n")
}

fn family(out: &mut String, name: &str, kind: &str, help_text: &str) {
    let _ = writeln!(out, "# HELP {name} {}", help(help_text));
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn sample<V: std::fmt::Display>(out: &mut String, name: &str, labels: &[(&str, String)], value: V) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let _ = write!(out, "{key}=\"{}\"", label(value));
        }
        out.push('}');
    }
    let _ = writeln!(out, " {value}");
}

fn pool_state(status: &PoolHealthState) -> &'static str {
    match status {
        PoolHealthState::Healthy => "healthy",
        PoolHealthState::Degraded => "degraded",
        PoolHealthState::Offline => "offline",
    }
}

/// Renders one scrape. Every family is emitted even when its input is empty or
/// absent, so a dashboard query does not silently stop matching after a restart:
/// `yadb_gc_last_pass_present 0` says "no pass has run", not "the metric is
/// gone".
pub(crate) fn render(
    node_id: u32,
    runtime: &str,
    pools: &[PoolHealthSnapshot],
    last_gc: Option<&SweepOutcome>,
    last_scrub: Option<&ScrubSummary>,
    groups: &[serde_json::Value],
) -> String {
    let mut out = String::with_capacity(8 * 1024);

    family(
        &mut out,
        "yadb_node_info",
        "gauge",
        "Always 1. Identifies the node and the metadata runtime it serves.",
    );
    sample(
        &mut out,
        "yadb_node_info",
        &[
            ("node_id", node_id.to_string()),
            ("metadata_runtime", runtime.to_string()),
        ],
        1,
    );

    family(
        &mut out,
        "yadb_pool_info",
        "gauge",
        "Always 1. The pool's scheme and its current health classification.",
    );
    family(
        &mut out,
        "yadb_pool_slots",
        "gauge",
        "Slots in the pool's canonical placement. Handoff slots are excluded.",
    );
    family(
        &mut out,
        "yadb_pool_handoff_slots",
        "gauge",
        "Spare slots that hold a part when its nominal slot does not answer.",
    );
    family(
        &mut out,
        "yadb_pool_online_slots",
        "gauge",
        "Canonical slots currently reachable.",
    );
    family(
        &mut out,
        "yadb_pool_min_read_slots",
        "gauge",
        "Slots a read needs to reconstruct a blob.",
    );
    family(
        &mut out,
        "yadb_pool_min_write_slots",
        "gauge",
        "Fragments a write must land before it is acknowledged.",
    );
    family(
        &mut out,
        "yadb_pool_repairs_total",
        "counter",
        "Fragments repaired on the pool's slots since this process started.",
    );
    family(
        &mut out,
        "yadb_pool_refused_puts_total",
        "counter",
        "Writes refused because the pool could not promise its configured redundancy.",
    );
    family(
        &mut out,
        "yadb_pool_degraded_puts_total",
        "counter",
        "Writes acknowledged below full redundancy on a pool with a reduced write quorum.",
    );
    for pool in pools {
        let name = &pool.pool_name;
        sample(
            &mut out,
            "yadb_pool_info",
            &[
                ("pool", name.clone()),
                ("scheme", pool.scheme.clone()),
                ("status", pool_state(&pool.status).to_string()),
            ],
            1,
        );
        sample(
            &mut out,
            "yadb_pool_slots",
            &[("pool", name.clone())],
            pool.total_slots,
        );
        sample(
            &mut out,
            "yadb_pool_handoff_slots",
            &[("pool", name.clone())],
            pool.handoff_slots,
        );
        sample(
            &mut out,
            "yadb_pool_online_slots",
            &[("pool", name.clone())],
            pool.online_slots,
        );
        sample(
            &mut out,
            "yadb_pool_min_read_slots",
            &[("pool", name.clone())],
            pool.min_read_slots,
        );
        sample(
            &mut out,
            "yadb_pool_min_write_slots",
            &[("pool", name.clone())],
            pool.min_write_slots,
        );
        sample(
            &mut out,
            "yadb_pool_repairs_total",
            &[("pool", name.clone())],
            pool.total_repairs,
        );
        sample(
            &mut out,
            "yadb_pool_refused_puts_total",
            &[("pool", name.clone())],
            pool.refused_puts,
        );
        sample(
            &mut out,
            "yadb_pool_degraded_puts_total",
            &[("pool", name.clone())],
            pool.degraded_puts,
        );
    }

    family(
        &mut out,
        "yadb_slot_online",
        "gauge",
        "1 when the slot answered its last health probe, 0 otherwise.",
    );
    family(
        &mut out,
        "yadb_slot_live_blobs",
        "gauge",
        "Live fragments the slot holds.",
    );
    family(
        &mut out,
        "yadb_slot_live_bytes",
        "gauge",
        "Bytes the slot's live fragments occupy.",
    );
    family(
        &mut out,
        "yadb_slot_segments",
        "gauge",
        "Segments the slot has open.",
    );
    family(
        &mut out,
        "yadb_slot_capacity_bytes",
        "gauge",
        "Bytes the slot reports as its capacity.",
    );
    family(
        &mut out,
        "yadb_slot_puts_ok_total",
        "counter",
        "Fragments the slot accepted.",
    );
    family(
        &mut out,
        "yadb_slot_puts_err_total",
        "counter",
        "Fragment writes the slot refused or failed.",
    );
    family(
        &mut out,
        "yadb_slot_gets_ok_total",
        "counter",
        "Fragment reads the slot served.",
    );
    family(
        &mut out,
        "yadb_slot_gets_err_total",
        "counter",
        "Fragment reads the slot failed.",
    );
    family(
        &mut out,
        "yadb_slot_repairs_ok_total",
        "counter",
        "Fragments the slot received from a repair pass.",
    );
    for pool in pools {
        for slot in &pool.slots {
            let labels = [
                ("pool", pool.pool_name.clone()),
                ("slot", slot.slot_index.to_string()),
                ("node_id", slot.node_id.to_string()),
                ("domain", slot.domain.clone()),
            ];
            sample(&mut out, "yadb_slot_online", &labels, u8::from(slot.online));
            sample(&mut out, "yadb_slot_live_blobs", &labels, slot.live_blobs);
            sample(&mut out, "yadb_slot_live_bytes", &labels, slot.live_bytes);
            sample(&mut out, "yadb_slot_segments", &labels, slot.segment_count);
            sample(
                &mut out,
                "yadb_slot_capacity_bytes",
                &labels,
                slot.total_disk_bytes,
            );
            sample(&mut out, "yadb_slot_puts_ok_total", &labels, slot.puts_ok);
            sample(&mut out, "yadb_slot_puts_err_total", &labels, slot.puts_err);
            sample(&mut out, "yadb_slot_gets_ok_total", &labels, slot.gets_ok);
            sample(&mut out, "yadb_slot_gets_err_total", &labels, slot.gets_err);
            sample(
                &mut out,
                "yadb_slot_repairs_ok_total",
                &labels,
                slot.repairs_ok,
            );
        }
    }

    family(
        &mut out,
        "yadb_gc_last_pass_present",
        "gauge",
        "1 when a collection pass has run since this process started.",
    );
    family(
        &mut out,
        "yadb_gc_blobs_live",
        "gauge",
        "Distinct live blobs the last pass's final mark found.",
    );
    family(
        &mut out,
        "yadb_gc_fragments_deleted",
        "gauge",
        "Physical fragments the last pass removed. A per-pass value, so it can go down: it is a gauge, not a counter.",
    );
    family(
        &mut out,
        "yadb_gc_bytes_reclaimed",
        "gauge",
        "Bytes the last pass's compaction returned to the filesystem. A per-pass value, so it can go down: it is a gauge, not a counter.",
    );
    family(
        &mut out,
        "yadb_gc_leases_live",
        "gauge",
        "Live reader/writer leases the last pass observed. Non-zero means it refused.",
    );
    family(
        &mut out,
        "yadb_gc_barriers_applied",
        "gauge",
        "Durable generation barriers the last pass wrote. A per-pass value, so it can go down: it is a gauge, not a counter.",
    );
    family(
        &mut out,
        "yadb_gc_refused",
        "gauge",
        "1 when the last pass deleted nothing, 0 when it ran.",
    );
    family(
        &mut out,
        "yadb_gc_groups_refused",
        "gauge",
        "Metadata groups the last pass refused.",
    );
    family(
        &mut out,
        "yadb_gc_pools_refused",
        "gauge",
        "Mounted pools the last pass refused.",
    );
    family(
        &mut out,
        "yadb_gc_errors",
        "gauge",
        "Per-operation failures the last pass reported after its refusal point.",
    );
    sample(
        &mut out,
        "yadb_gc_last_pass_present",
        &[],
        u8::from(last_gc.is_some()),
    );
    if let Some(gc) = last_gc {
        sample(&mut out, "yadb_gc_blobs_live", &[], gc.blobs_live);
        sample(
            &mut out,
            "yadb_gc_fragments_deleted",
            &[],
            gc.fragments_deleted,
        );
        sample(
            &mut out,
            "yadb_gc_bytes_reclaimed",
            &[],
            gc.bytes_reclaimed,
        );
        sample(&mut out, "yadb_gc_leases_live", &[], gc.leases_live);
        sample(
            &mut out,
            "yadb_gc_barriers_applied",
            &[],
            gc.barriers_applied,
        );
        sample(
            &mut out,
            "yadb_gc_refused",
            &[],
            u8::from(gc.refusal.is_some()),
        );
        sample(&mut out, "yadb_gc_groups_refused", &[], gc.groups_refused.len());
        sample(&mut out, "yadb_gc_pools_refused", &[], gc.pools_refused.len());
        sample(&mut out, "yadb_gc_errors", &[], gc.errors.len());
    }

    family(
        &mut out,
        "yadb_scrub_last_pass_present",
        "gauge",
        "1 when a repair scan has run since this process started.",
    );
    family(
        &mut out,
        "yadb_scrub_pools_checked",
        "gauge",
        "Pools the last repair scan examined.",
    );
    family(
        &mut out,
        "yadb_scrub_blobs_checked",
        "gauge",
        "Live blobs the last repair scan examined.",
    );
    family(
        &mut out,
        "yadb_scrub_blobs_intact",
        "gauge",
        "Blobs the last repair scan proved intact.",
    );
    family(
        &mut out,
        "yadb_scrub_blobs_repaired",
        "gauge",
        "Blobs the last repair scan repaired.",
    );
    family(
        &mut out,
        "yadb_scrub_fragments_repaired",
        "gauge",
        "Fragments the last repair scan rewrote.",
    );
    family(
        &mut out,
        "yadb_scrub_blobs_unrecoverable",
        "gauge",
        "Blobs the last repair scan could not prove intact.",
    );
    family(
        &mut out,
        "yadb_scrub_refused_puts_total",
        "counter",
        "Writes refused for redundancy, summed over pools.",
    );
    family(
        &mut out,
        "yadb_scrub_degraded_puts_total",
        "counter",
        "Writes acknowledged below full redundancy, summed over pools.",
    );
    family(
        &mut out,
        "yadb_scrub_orphan_blobs_collected",
        "gauge",
        "Orphan blobs the last pass collected.",
    );
    family(
        &mut out,
        "yadb_scrub_repair_error",
        "gauge",
        "1 when the last repair scan refused at least one group.",
    );
    family(
        &mut out,
        "yadb_scrub_orphan_collection_error",
        "gauge",
        "1 when the last orphan collection reported an error.",
    );
    sample(
        &mut out,
        "yadb_scrub_last_pass_present",
        &[],
        u8::from(last_scrub.is_some()),
    );
    if let Some(scrub) = last_scrub {
        sample(&mut out, "yadb_scrub_pools_checked", &[], scrub.pools_checked);
        sample(&mut out, "yadb_scrub_blobs_checked", &[], scrub.blobs_checked);
        sample(&mut out, "yadb_scrub_blobs_intact", &[], scrub.blobs_intact);
        sample(
            &mut out,
            "yadb_scrub_blobs_repaired",
            &[],
            scrub.blobs_repaired,
        );
        sample(
            &mut out,
            "yadb_scrub_fragments_repaired",
            &[],
            scrub.fragments_repaired,
        );
        sample(
            &mut out,
            "yadb_scrub_blobs_unrecoverable",
            &[],
            scrub.blobs_unrecoverable,
        );
        sample(
            &mut out,
            "yadb_scrub_refused_puts_total",
            &[],
            scrub.refused_puts,
        );
        sample(
            &mut out,
            "yadb_scrub_degraded_puts_total",
            &[],
            scrub.degraded_puts,
        );
        sample(
            &mut out,
            "yadb_scrub_orphan_blobs_collected",
            &[],
            scrub.orphan_blobs_collected,
        );
        sample(
            &mut out,
            "yadb_scrub_repair_error",
            &[],
            u8::from(scrub.repair_error.is_some()),
        );
        sample(
            &mut out,
            "yadb_scrub_orphan_collection_error",
            &[],
            u8::from(scrub.orphan_collection_error.is_some()),
        );
    }

    family(
        &mut out,
        "yadb_group_observation_error",
        "gauge",
        "1 when the metadata groups could not be enumerated at all.",
    );
    family(
        &mut out,
        "yadb_group_info",
        "gauge",
        "Always 1. One row per configured metadata group with its role.",
    );
    family(
        &mut out,
        "yadb_group_term",
        "gauge",
        "The group's current term as this node last observed it.",
    );
    family(
        &mut out,
        "yadb_group_commit_index",
        "gauge",
        "The group's commit index as this node last observed it.",
    );
    family(
        &mut out,
        "yadb_group_applied_index",
        "gauge",
        "The group's applied index as this node last observed it.",
    );
    family(
        &mut out,
        "yadb_group_reader_leases_live",
        "gauge",
        "1 when this node holds a live reader lease for the group.",
    );
    family(
        &mut out,
        "yadb_group_reader_lease_expiry_ms",
        "gauge",
        "Wall-clock expiry of this node's reader lease for the group; 0 when none.",
    );
    family(
        &mut out,
        "yadb_group_error",
        "gauge",
        "1 when the group row carries an observation error.",
    );
    for row in groups {
        let Some(group) = row["group"].as_u64() else {
            // The single error row has a null group: enumeration itself failed,
            // so there is no per-group row to attach the failure to.
            sample(&mut out, "yadb_group_observation_error", &[], 1);
            continue;
        };
        let labels = [("group", group.to_string())];
        sample(
            &mut out,
            "yadb_group_info",
            &[
                ("group", group.to_string()),
                (
                    "role",
                    row["role"].as_str().unwrap_or("unknown").to_string(),
                ),
                (
                    "node_id",
                    row["node_id"].as_u64().unwrap_or(0).to_string(),
                ),
            ],
            1,
        );
        sample(
            &mut out,
            "yadb_group_term",
            &labels,
            row["term"].as_u64().unwrap_or(0),
        );
        sample(
            &mut out,
            "yadb_group_commit_index",
            &labels,
            row["commit_index"].as_u64().unwrap_or(0),
        );
        sample(
            &mut out,
            "yadb_group_applied_index",
            &labels,
            row["applied_index"].as_u64().unwrap_or(0),
        );
        sample(
            &mut out,
            "yadb_group_reader_leases_live",
            &labels,
            row["reader_leases_live"].as_u64().unwrap_or(0),
        );
        sample(
            &mut out,
            "yadb_group_reader_lease_expiry_ms",
            &labels,
            row["reader_lease_nearest_expiry_ms"].as_u64().unwrap_or(0),
        );
        sample(
            &mut out,
            "yadb_group_error",
            &labels,
            u8::from(row["error"].is_string()),
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use yadb_blob::{SlotHealthSnapshot, VolumeStats};

    fn slot(index: usize, online: bool) -> SlotHealthSnapshot {
        SlotHealthSnapshot {
            slot_index: index,
            domain: "dom-a".to_string(),
            node_id: 1,
            addr: Some("127.0.0.1:19001".to_string()),
            is_local: true,
            online,
            live_blobs: 7 + index,
            live_bytes: 4096 * (index as u64 + 1),
            segment_count: 2,
            total_disk_bytes: 1 << 30,
            puts_ok: 11,
            puts_err: 1,
            gets_ok: 13,
            gets_err: 2,
            repairs_ok: 3,
        }
    }

    fn pool() -> PoolHealthSnapshot {
        PoolHealthSnapshot {
            pool_name: "data".to_string(),
            scheme: "mirror (3x)".to_string(),
            status: PoolHealthState::Degraded,
            total_slots: 2,
            handoff_slots: 1,
            online_slots: 1,
            min_read_slots: 1,
            min_write_slots: 2,
            total_repairs: 6,
            refused_puts: 4,
            degraded_puts: 2,
            slots: vec![slot(0, true), slot(1, false)],
        }
    }

    fn line<'a>(text: &'a str, needle: &str) -> &'a str {
        text.lines()
            .find(|line| line.starts_with(needle))
            .unwrap_or_else(|| panic!("no line starting with {needle:?} in:\n{text}"))
    }

    /// Every family the exposition declares, so the test cannot drift back to a
    /// hand-picked sample: a new `family(...)` call must be added here too, or
    /// the rendered `# HELP` set and this list disagree and the test fails.
    const FAMILIES: &[&str] = &[
        "yadb_node_info",
        "yadb_pool_info",
        "yadb_pool_slots",
        "yadb_pool_handoff_slots",
        "yadb_pool_online_slots",
        "yadb_pool_min_read_slots",
        "yadb_pool_min_write_slots",
        "yadb_pool_repairs_total",
        "yadb_pool_refused_puts_total",
        "yadb_pool_degraded_puts_total",
        "yadb_slot_online",
        "yadb_slot_live_blobs",
        "yadb_slot_live_bytes",
        "yadb_slot_segments",
        "yadb_slot_capacity_bytes",
        "yadb_slot_puts_ok_total",
        "yadb_slot_puts_err_total",
        "yadb_slot_gets_ok_total",
        "yadb_slot_gets_err_total",
        "yadb_slot_repairs_ok_total",
        "yadb_gc_last_pass_present",
        "yadb_gc_blobs_live",
        "yadb_gc_fragments_deleted",
        "yadb_gc_bytes_reclaimed",
        "yadb_gc_leases_live",
        "yadb_gc_barriers_applied",
        "yadb_gc_refused",
        "yadb_gc_groups_refused",
        "yadb_gc_pools_refused",
        "yadb_gc_errors",
        "yadb_scrub_last_pass_present",
        "yadb_scrub_pools_checked",
        "yadb_scrub_blobs_checked",
        "yadb_scrub_blobs_intact",
        "yadb_scrub_blobs_repaired",
        "yadb_scrub_fragments_repaired",
        "yadb_scrub_blobs_unrecoverable",
        "yadb_scrub_refused_puts_total",
        "yadb_scrub_degraded_puts_total",
        "yadb_scrub_orphan_blobs_collected",
        "yadb_scrub_repair_error",
        "yadb_scrub_orphan_collection_error",
        "yadb_group_observation_error",
        "yadb_group_info",
        "yadb_group_term",
        "yadb_group_commit_index",
        "yadb_group_applied_index",
        "yadb_group_reader_leases_live",
        "yadb_group_reader_lease_expiry_ms",
        "yadb_group_error",
    ];

    #[test]
    fn every_family_is_emitted_with_help_and_type() {
        let text = render(1, "openraft", &[pool()], None, None, &[]);
        for name in FAMILIES {
            assert!(
                text.contains(&format!("# HELP {name} ")),
                "missing HELP for {name}:\n{text}"
            );
            assert!(
                text.contains(&format!("# TYPE {name} ")),
                "missing TYPE for {name}:\n{text}"
            );
        }
        // The list above must be complete, not merely long: every declared
        // family in the output has to appear in it.
        let declared: Vec<&str> = text
            .lines()
            .filter_map(|line| line.strip_prefix("# HELP "))
            .filter_map(|rest| rest.split_whitespace().next())
            .collect();
        assert_eq!(
            declared.len(),
            FAMILIES.len(),
            "the exposition declares {} families but the list names {}:\n{text}",
            declared.len(),
            FAMILIES.len()
        );
        for name in &declared {
            assert!(
                FAMILIES.contains(name),
                "the exposition declares {name}, which the list omits"
            );
        }
        assert_eq!(
            text.matches("# TYPE ").count(),
            FAMILIES.len(),
            "every family must have exactly one TYPE line"
        );
    }

    #[test]
    fn pool_and_slot_samples_carry_the_health_values() {
        let text = render(7, "openraft", &[pool()], None, None, &[]);
        assert_eq!(
            line(&text, "yadb_node_info{"),
            "yadb_node_info{node_id=\"7\",metadata_runtime=\"openraft\"} 1"
        );
        assert_eq!(
            line(&text, "yadb_pool_info{"),
            "yadb_pool_info{pool=\"data\",scheme=\"mirror (3x)\",status=\"degraded\"} 1"
        );
        assert_eq!(line(&text, "yadb_pool_slots{"), "yadb_pool_slots{pool=\"data\"} 2");
        assert_eq!(
            line(&text, "yadb_pool_online_slots{"),
            "yadb_pool_online_slots{pool=\"data\"} 1"
        );
        assert_eq!(
            line(&text, "yadb_pool_refused_puts_total{"),
            "yadb_pool_refused_puts_total{pool=\"data\"} 4"
        );
        // The offline slot is a 0 gauge, not a missing sample: a slot that
        // disappears from the exposition would look like a decommissioned one.
        assert!(
            text.contains("yadb_slot_online{pool=\"data\",slot=\"1\",node_id=\"1\",domain=\"dom-a\"} 0"),
            "{text}"
        );
        assert!(
            text.contains("yadb_slot_live_bytes{pool=\"data\",slot=\"0\",node_id=\"1\",domain=\"dom-a\"} 4096"),
            "{text}"
        );
    }

    #[test]
    fn an_absent_gc_or_scrub_pass_reports_zero_present_and_no_samples() {
        let text = render(1, "single-node", &[], None, None, &[]);
        assert_eq!(line(&text, "yadb_gc_last_pass_present"), "yadb_gc_last_pass_present 0");
        assert_eq!(
            line(&text, "yadb_scrub_last_pass_present"),
            "yadb_scrub_last_pass_present 0"
        );
        assert!(
            !text.contains("\nyadb_gc_refused "),
            "a family with no pass must not publish a value:\n{text}"
        );
        assert!(
            !text.contains("\nyadb_scrub_blobs_checked "),
            "a family with no pass must not publish a value:\n{text}"
        );
    }

    #[test]
    fn a_refused_pass_is_visible_as_a_refusal_and_counts() {
        let gc = SweepOutcome {
            blobs_live: 9,
            fragments_deleted: 0,
            leases_live: 1,
            refusal: Some("a live lease refuses the pass".to_string()),
            groups_refused: vec!["group 2 refused".to_string()],
            ..SweepOutcome::default()
        };
        let text = render(1, "openraft", &[], Some(&gc), None, &[]);
        assert_eq!(line(&text, "yadb_gc_last_pass_present"), "yadb_gc_last_pass_present 1");
        assert_eq!(line(&text, "yadb_gc_refused"), "yadb_gc_refused 1");
        assert_eq!(line(&text, "yadb_gc_blobs_live"), "yadb_gc_blobs_live 9");
        assert_eq!(
            line(&text, "yadb_gc_fragments_deleted"),
            "yadb_gc_fragments_deleted 0"
        );
        assert_eq!(line(&text, "yadb_gc_groups_refused"), "yadb_gc_groups_refused 1");
        assert_eq!(line(&text, "yadb_gc_leases_live"), "yadb_gc_leases_live 1");
    }

    #[test]
    fn group_rows_become_per_group_samples_and_a_null_row_is_an_observation_error() {
        let rows = vec![
            serde_json::json!({
                "group": 2,
                "node_id": 1,
                "role": "Leader",
                "term": 5,
                "leader_id": 1,
                "commit_index": 120,
                "applied_index": 119,
                "reader_leases_live": 1,
                "reader_lease_nearest_expiry_ms": 123456789,
            }),
            serde_json::json!({
                "group": 3,
                "node_id": 1,
                "role": "Follower",
                "term": 4,
                "leader_id": 2,
                "commit_index": 88,
                "applied_index": 88,
                "reader_leases_live": 0,
                "reader_lease_nearest_expiry_ms": null,
                "error": "commit index unavailable",
            }),
        ];
        let text = render(1, "openraft", &[], None, None, &rows);
        assert_eq!(
            line(&text, "yadb_group_info{group=\"2\""),
            "yadb_group_info{group=\"2\",role=\"Leader\",node_id=\"1\"} 1"
        );
        assert_eq!(line(&text, "yadb_group_term{group=\"2\"}"), "yadb_group_term{group=\"2\"} 5");
        assert_eq!(
            line(&text, "yadb_group_reader_leases_live{group=\"2\"}"),
            "yadb_group_reader_leases_live{group=\"2\"} 1"
        );
        assert_eq!(
            line(&text, "yadb_group_reader_lease_expiry_ms{group=\"2\"}"),
            "yadb_group_reader_lease_expiry_ms{group=\"2\"} 123456789"
        );
        // No lease and a null expiry render as 0, and the row's error is a
        // gauge rather than a dropped row.
        assert_eq!(
            line(&text, "yadb_group_reader_lease_expiry_ms{group=\"3\"}"),
            "yadb_group_reader_lease_expiry_ms{group=\"3\"} 0"
        );
        assert_eq!(line(&text, "yadb_group_error{group=\"3\"}"), "yadb_group_error{group=\"3\"} 1");
        assert_eq!(line(&text, "yadb_group_error{group=\"2\"}"), "yadb_group_error{group=\"2\"} 0");
        assert!(!text.contains("\nyadb_group_observation_error 1"), "{text}");

        let failed = render(
            1,
            "openraft",
            &[],
            None,
            None,
            &[serde_json::json!({"group": null, "error": "cannot enumerate"})],
        );
        assert_eq!(
            line(&failed, "yadb_group_observation_error"),
            "yadb_group_observation_error 1"
        );
        assert!(!failed.contains("yadb_group_info{"), "{failed}");
    }

    #[test]
    fn label_values_escape_the_three_reserved_characters() {
        let mut pool = pool();
        pool.pool_name = "a\"b\\c\nd".to_string();
        let text = render(1, "openraft", &[pool], None, None, &[]);
        assert!(
            text.contains("pool=\"a\\\"b\\\\c\\nd\""),
            "label was not escaped:\n{text}"
        );
        // The escaping must not split the sample across lines: the raw newline
        // would otherwise start a new, unparseable exposition line.
        assert!(line(&text, "yadb_pool_slots{").ends_with("} 2"));
    }

    #[test]
    fn values_are_finite_decimals_not_scientific_notation() {
        let mut pool = pool();
        pool.slots[0].live_bytes = 1 << 40;
        let text = render(1, "openraft", &[pool], None, None, &[]);
        assert!(
            text.contains("} 1099511627776"),
            "large counters must stay decimal:\n{text}"
        );
    }

    #[test]
    fn rendering_is_deterministic_for_the_same_rows() {
        let first = render(1, "openraft", &[pool()], None, None, &[]);
        let second = render(1, "openraft", &[pool()], None, None, &[]);
        assert_eq!(first, second);
    }

    // Keeps the unused-import warning away when the fixture helpers above stop
    // naming `VolumeStats` directly.
    #[allow(dead_code)]
    fn _volume_stats_type_is_used(_: VolumeStats) {}
}
