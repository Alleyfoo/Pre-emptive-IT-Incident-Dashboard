"""
Pre-emptive IT Incident Dashboard — Kevät demo.
Entry point for Streamlit Cloud. Auto-generates synthetic fleet data on first load.
"""
from __future__ import annotations

import html
import json
import math
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.artifact_store import build_artifact_store
from runtime.run_pointer import get_latest_run_id
from tools.generate_ticket_scenarios import ScenarioConfig, ScenarioGenerator

DEMO_RUN_ID = "demo"
ARTIFACTS_ROOT = os.environ.get("ARTIFACTS_ROOT") or os.path.join(REPO_ROOT, "artifacts")


# ─────────────────────────────────────────────────────────
# Store helpers
# ─────────────────────────────────────────────────────────

def _store():
    return build_artifact_store(ARTIFACTS_ROOT)


def _load_json(store, key: str) -> Dict:
    if not store.exists(key):
        return {}
    return json.loads(store.read_text(key))


def _fleet_summary(store, run_id: str) -> Dict:
    return _load_json(store, f"{run_id}/fleet_summary.json")


def _timeline(store, run_id: str, host_id: str) -> Dict:
    return _load_json(store, f"{run_id}/hosts/{host_id}/timeline.json")


def _host_options(store, run_id: str, fleet: Dict) -> List[str]:
    opts = [h["host_id"] for h in fleet.get("top_hosts", [])]
    for key in store.list(f"{run_id}/hosts"):
        parts = key.split("/")
        if len(parts) >= 3:
            opts.append(parts[2])
    return sorted(list(set(opts)))


def _available_runs(store) -> List[str]:
    runs = store.list_runs()
    if runs:
        return [r for r in runs if r not in {"history"}]
    legacy: set = set()
    for key in store.list():
        if "/" in key:
            legacy.add(key.split("/")[0])
    return sorted(list(legacy))


# ─────────────────────────────────────────────────────────
# Demo data bootstrap
# The ScenarioGenerator writes {run_id}/snapshots/{host_id}.json
# (flat, no timestamp in filename). The standard _load_snapshots
# expects snapshot-YYYYMMDDTHHMMSSZ.json filenames, so we bypass
# it and call the pipeline functions directly.
# ─────────────────────────────────────────────────────────

def _run_demo_pipeline(run_id: str, store) -> None:
    from runtime.incident_flow import (
        build_fleet_summary,
        build_host_timelines,
        write_fleet_artifacts,
        write_host_artifacts,
    )
    from runtime.run_pointer import write_latest
    from runtime.schema_validate import validate_or_raise

    snapshots = []
    for key in store.list(f"{run_id}/snapshots"):
        if not key.endswith(".json"):
            continue
        try:
            data = json.loads(store.read_text(key))
        except Exception:
            continue
        snapshots.append({"key": key, "data": data})

    if not snapshots:
        return

    timelines = build_host_timelines(store, run_id, snapshots=snapshots)
    fleet = build_fleet_summary(run_id, timelines)
    host_meta = {h.get("host_id"): h for h in fleet.get("top_hosts", [])}
    write_host_artifacts(store, run_id, timelines, fleet_window=fleet.get("window"), host_meta=host_meta)
    write_fleet_artifacts(store, run_id, fleet)
    try:
        validate_or_raise(store, run_id)
    except Exception:
        pass
    write_latest(store, run_id)


def _bootstrap_demo_data(force: bool = False) -> None:
    store = _store()
    if not force and store.exists(f"{DEMO_RUN_ID}/fleet_summary.json"):
        return
    store.delete_prefix(DEMO_RUN_ID)
    config = ScenarioConfig(
        run_id=DEMO_RUN_ID,
        seed=42,
        n_hosts=20,
        days=1,
        scenario_tags=["driver_rollout_wave", "slow_burn", "single_host_hardware"],
    )
    ScenarioGenerator(config=config, artifacts_root=ARTIFACTS_ROOT).generate()
    _run_demo_pipeline(DEMO_RUN_ID, store)


# ─────────────────────────────────────────────────────────
# CSS injection
# ─────────────────────────────────────────────────────────

def inject_css() -> None:
    here = Path(__file__).parent
    base = (here / "kevat.css").read_text(encoding="utf-8")
    over = (here / "streamlit-overrides.css").read_text(encoding="utf-8")
    st.markdown(f"<style>{base}\n{over}</style>", unsafe_allow_html=True)
    st.markdown(
        """
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,500&family=Plus+Jakarta+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"
              rel="stylesheet">
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────
# Severity helpers
# ─────────────────────────────────────────────────────────

def _sev_class(s: int) -> str:
    if s >= 90:
        return "critical"
    if s >= 75:
        return "hot"
    if s >= 60:
        return "warm"
    return "cool"


def _sev_label(s: int) -> str:
    return {"cool": "Calm", "warm": "Sprouting", "hot": "Flowering", "critical": "Overgrown"}[_sev_class(s)]


# ─────────────────────────────────────────────────────────
# Sprout SVG (ported from components.jsx)
# ─────────────────────────────────────────────────────────

def sprout_svg(leaves: int, sev: int, bloom: bool = False, index: int = 0, w: int = 120, h: int = 240) -> str:
    height_frac = max(0.42, min(1.0, 0.42 + (sev / 120) * 0.58))
    stem_top = h * (1 - height_frac) + 6
    stem_base = h - 10
    cx = w / 2

    seed = (index * 73 + leaves * 11 + round(sev)) % 100
    wobble = 1 if seed % 2 == 0 else -1
    stem_curve = 6 + (seed % 6)

    mid_y = (stem_top + stem_base) / 2
    stem_path = (
        f"M {cx} {stem_base} "
        f"C {cx + stem_curve * wobble} {mid_y + 22}, "
        f"{cx - stem_curve * wobble} {mid_y - 22}, "
        f"{cx} {stem_top}"
    )

    leaf_parts: List[str] = []
    usable = max(1, min(8, leaves))
    leaf_palette = ["var(--leaf-500)", "var(--leaf-300)", "var(--leaf-200)"]
    for i in range(usable):
        t = 0.15 + (i / max(usable - 1, 1)) * 0.7
        y = stem_base + (stem_top - stem_base) * t
        side = -1 if i % 2 == 0 else 1
        leaf_size = 16 + (i / usable) * 3
        x = cx + math.sin(t * math.pi) * stem_curve * wobble * 0.6
        fill = leaf_palette[i % 3]
        angle = side * (32 + i * 3)
        leaf_parts.append(
            f'<g transform="translate({x:.1f} {y:.1f}) rotate({angle})">'
            f'<path d="M 0 0 Q {side * leaf_size * 0.55:.1f} {-leaf_size * 0.55:.1f} '
            f'{side * leaf_size:.1f} 0 Q {side * leaf_size * 0.55:.1f} {leaf_size * 0.55:.1f} 0 0 Z" '
            f'fill="{fill}" />'
            f'<line x1="0" y1="0" x2="{side * leaf_size:.1f}" y2="0" '
            f'stroke="rgba(255,255,255,0.22)" stroke-width="0.7" />'
            f'</g>'
        )

    sev_c = _sev_class(sev)
    bloom_accent = {
        "critical": "var(--sev-4)",
        "hot":      "var(--sev-3)",
        "warm":     "var(--sun)",
        "cool":     "var(--leaf-300)",
    }[sev_c]

    if bloom or sev_c in ("critical", "hot"):
        petals = "".join(
            f'<ellipse cx="0" cy="-11" rx="7" ry="10" transform="rotate({d})" '
            f'fill="var(--bloom)" stroke="var(--bloom-edge)" stroke-width="0.8" />'
            for d in (0, 72, 144, 216, 288)
        )
        pip = '<circle r="2" fill="var(--bloom)" opacity="{op}" />'.format(
            op=1 if sev_c == "critical" else 0.6
        )
        bloom_svg = (
            f'<g transform="translate({cx} {stem_top - 4})">'
            f'{petals}<circle r="5.5" fill="{bloom_accent}" />{pip}</g>'
        )
    elif sev_c == "warm":
        spokes = "".join(
            f'<line x1="0" y1="0" x2="0" y2="-9" transform="rotate({d})" '
            f'stroke="var(--pollen-600)" stroke-width="0.6" opacity="0.5" />'
            for d in (0, 45, 90, 135, 180, 225, 270, 315)
        )
        bloom_svg = (
            f'<g transform="translate({cx} {stem_top - 4})">'
            f'<circle r="9" fill="var(--pollen-200)" opacity="0.6" />'
            f'<circle r="7" fill="var(--sun)" />{spokes}</g>'
        )
    else:
        bloom_svg = (
            f'<g transform="translate({cx} {stem_top - 2})">'
            f'<ellipse cx="0" cy="-3" rx="4" ry="6" fill="var(--leaf-500)" />'
            f'<ellipse cx="0" cy="-2" rx="2.5" ry="4" fill="var(--leaf-300)" /></g>'
        )

    ground = f'<ellipse cx="{cx}" cy="{stem_base + 3}" rx="16" ry="2.4" fill="rgba(201,163,110,0.22)" />'

    return (
        f'<svg class="plant-svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" aria-hidden="true">'
        f'{ground}'
        f'<path d="{stem_path}" fill="none" stroke="var(--leaf-400)" stroke-width="2.4" stroke-linecap="round" />'
        f'{"".join(leaf_parts)}'
        f'{bloom_svg}'
        f'</svg>'
    )


# ─────────────────────────────────────────────────────────
# Fleet tab render functions
# ─────────────────────────────────────────────────────────

def render_hero(fleet: Dict, run_id: str) -> None:
    clusters = fleet.get("clusters", [])
    # affected_hosts in the fleet summary is a count (int), example_hosts is the list
    total_affected = sum(c.get("affected_hosts", 0) for c in clusters)
    # de-duplicate via example_hosts where available
    affected_set: set = set()
    for c in clusters:
        for h in c.get("example_hosts", []):
            affected_set.add(h)
    affected = len(affected_set) if affected_set else total_affected
    total = fleet.get("hosts_total", len(fleet.get("top_hosts", [])) or 20)
    new = sum(1 for c in clusters if c.get("status") == "new")
    risk = fleet.get("overall_risk_score", 0)
    st.markdown(
        f"""
        <div class="hero">
          <div class="hero-left">
            <p class="eyebrow" style="color: var(--leaf-500)">Today · run {html.escape(run_id)}</p>
            <h1>{affected} hosts are <span class="light">stirring.</span></h1>
            <p>{len(clusters)} clusters surfaced. Tend the flowering ones first — the rest will be calm by lunch.</p>
          </div>
          <div class="hero-right">
            <div class="kpi">
              <span class="eyebrow">Overall risk</span>
              <span class="num">{risk}</span>
            </div>
            <div class="kpi">
              <span class="eyebrow">Clusters</span>
              <span class="num">{len(clusters)}</span>
              <span class="delta warm">↗ {new} new today</span>
            </div>
            <div class="kpi">
              <span class="eyebrow">Affected</span>
              <span class="num">{affected}<small style="font-size:16px;color:var(--ink-faint)"> / {total}</small></span>
              <span class="sub">hosts in the field</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_meadow(clusters: List[Dict]) -> None:
    plants = []
    for i, c in enumerate(clusters[:6]):
        bloom = c.get("status") == "new"
        n_leaves = c.get("affected_hosts", 1)  # this is a count (int)
        sev = c.get("severity", 50)
        svg = sprout_svg(
            leaves=n_leaves,
            sev=sev,
            bloom=bloom,
            index=i,
            w=120,
            h=int(180 + (sev / 120) * 80),
        )
        # Use signature_key as name — trim to readable length
        raw_key = c.get("signature_key") or c.get("signature_hash", "")[:12]
        name = html.escape(raw_key[:40])
        meta = f"{n_leaves} hosts · sev {sev}"
        plants.append(
            f'<div class="plant">{svg}'
            f'<div class="plant-card"><span class="name">{name}</span>'
            f'<span class="meta">{meta}</span></div></div>'
        )
    st.markdown(
        f"""
        <section class="meadow-wrap">
          <div class="meadow-head">
            <div>
              <p class="eyebrow" style="color: var(--leaf-500)">The Meadow</p>
              <h2 style="font-family: var(--font-display); font-style: italic; font-weight: 500;">
                {len(clusters)} clusters in bloom today
              </h2>
            </div>
            <div class="legend">
              <span><i style="background: var(--leaf-300)"></i> bud · calm</span>
              <span><i style="background: var(--sun)"></i> pollen · watch</span>
              <span><i style="background: var(--sev-3)"></i> bloom · attend</span>
            </div>
          </div>
          <div class="meadow">{"".join(plants)}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


_ICON_PATHS = {
    "bsod":                "M13 3 L7 13 H12 L10 21 L17 11 H12 Z",
    "update_failure":      "M5 19 Q5 8 12 8 Q19 8 19 14 Q19 18 15 18 Q12 18 12 15",
    "disk_full":           "M5 11 H15 V18 Q15 19 14 19 H6 Q5 19 5 18 Z M15 13 L19 11 V15 M9 8 Q9 6 11 6 H13 Q15 6 15 8",
    "service_crash":       "M12 21 V14 M12 14 L9 11 M12 14 L15 11 M12 14 V8 L9 5",
    "network_instability": "M3 14 Q7 8 12 14 T21 14",
}


def _type_icon(t: str, color: str = "currentColor") -> str:
    d = _ICON_PATHS.get(t, "")
    if not d:
        return ""
    return (
        f'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" '
        f'stroke="{color}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">'
        f'<path d="{d}" /></svg>'
    )


def render_cluster_ledger(clusters: List[Dict]) -> None:
    rows = []
    for c in clusters:
        sev_class = _sev_class(c.get("severity", 0))
        bar_pct = min(100, (c.get("severity", 0) / 120) * 100)
        accent = {
            "cool":     "var(--leaf-300)",
            "warm":     "var(--pollen-400)",
            "hot":      "var(--sev-3)",
            "critical": "var(--sev-4)",
        }[sev_class]
        n_hosts = c.get("affected_hosts", 0)
        example = c.get("example_hosts", [])
        hosts_label = ", ".join(example[:3]) + ("…" if len(example) > 3 else "")
        status = c.get("status", "ongoing")
        status_label = "Sprouting today" if status == "new" else "Ongoing"
        status_dot_color = "var(--leaf-400)" if status == "new" else "var(--sun)"
        inc_type = c.get("type") or "bsod"
        sig_key = c.get("signature_key") or c.get("signature_hash", "")[:12]
        rows.append(f"""
          <div class="cluster-row">
            <span style="color: {accent}; display: flex;">{_type_icon(inc_type, accent)}</span>
            <div class="title">
              <strong>{html.escape(sig_key[:48])}</strong>
              <span class="sub">{html.escape(c.get("signature_hash", "")[:12])}</span>
            </div>
            <span class="hosts">
              <span style="font-family: var(--font-display); font-size: 20px; line-height: 1; color: var(--ink); font-weight: 500;">{n_hosts}</span>
              <span style="font-size: 11px; color: var(--ink-faint);">{html.escape(hosts_label)}</span>
            </span>
            <div class="severity-bar {sev_class}"><i style="width: {bar_pct:.0f}%"></i></div>
            <span class="status-pill {status}">
              <span style="width:6px;height:6px;border-radius:50%;background:{status_dot_color};"></span>
              {status_label}
            </span>
            <span style="color: var(--ink-faint); text-align: right; font-size: 18px;">›</span>
          </div>
        """)
    st.markdown(
        f"""
        <section class="card" style="margin-top: 24px;">
          <div class="card-head">
            <div>
              <p class="eyebrow">Cluster ledger</p>
              <h2 style="font-family: var(--font-display); font-style: italic; font-weight: 500;">What's growing</h2>
            </div>
          </div>
          <div class="cluster-list">
            <div class="cluster-row head">
              <span></span><span>Cluster</span><span>Hosts</span>
              <span>Severity</span><span>Status</span><span></span>
            </div>
            {"".join(rows)}
          </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_hosts_grid(hosts: List[Dict]) -> None:
    cards = []
    for h in hosts[:12]:
        sev_c = _sev_class(h.get("score", 0))
        score = h.get("score", 0)
        reasons = h.get("reasons", [])
        tags = "".join(
            f'<span class="type-tag {sev_c}">{html.escape(r[:30])}</span>'
            for r in reasons[:2]
        )
        user = html.escape(str(h.get("user_id", "")))
        cards.append(
            f'<div class="host-card">'
            f'  <div class="header">'
            f'    <div><div class="id">{html.escape(h.get("host_id",""))}</div>'
            f'    <div class="user">{user}</div></div>'
            f'    <div class="score">{score}<small>pts</small></div>'
            f'  </div>'
            f'  <div class="types">{tags}</div>'
            f'</div>'
        )
    st.markdown(
        f"""
        <section class="card" style="margin-top: 24px;">
          <div class="card-head">
            <div>
              <p class="eyebrow">Top impacted hosts</p>
              <h2 style="font-family: var(--font-display); font-style: italic; font-weight: 500;">Hosts needing attention</h2>
            </div>
          </div>
          <div class="hosts-grid">{"".join(cards)}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────
# Host tab
# ─────────────────────────────────────────────────────────

def render_host_view(store, run_id: str, host_id: str, fleet: Dict) -> None:
    timeline = _timeline(store, run_id, host_id)
    if not timeline:
        st.info(f"No timeline found for {host_id}.")
        return

    window = timeline.get("window") or {
        "start": timeline.get("window_start"),
        "end": timeline.get("window_end"),
    }
    host_meta = next((h for h in fleet.get("top_hosts", []) if h.get("host_id") == host_id), {})
    score = host_meta.get("score", 0)
    sev_c = _sev_class(score)
    action = host_meta.get("action", "—")
    reason = host_meta.get("action_reason", "")

    st.markdown(
        f"""
        <div class="host-detail" style="margin-bottom: 20px;">
          <h2>{html.escape(host_id)}</h2>
          <div class="id-mono">{window.get('start','')[:10]} → {window.get('end','')[:10]}</div>
          <dl>
            <dt>Action</dt><dd><strong>{html.escape(action)}</strong></dd>
            <dt>Score</dt><dd>{score} <small style="color:var(--ink-faint)">({_sev_label(score)})</small></dd>
            <dt>Reason</dt><dd>{html.escape(reason)}</dd>
          </dl>
        </div>
        """,
        unsafe_allow_html=True,
    )

    incidents = timeline.get("incidents", [])
    cluster_index = {c.get("signature_hash"): c for c in fleet.get("clusters", [])}

    if incidents:
        inc_cards = []
        for inc in incidents:
            sev = inc.get("severity", 0)
            sig_hash = inc.get("signature", {}).get("signature_hash", "")
            cluster = cluster_index.get(sig_hash, {})
            cluster_line = ""
            if cluster:
                cluster_line = (
                    f'<div style="font-size:12px;color:var(--ink-faint);margin-top:4px;">'
                    f'Fleet cluster: {html.escape(cluster.get("signature_key","")[:40])} '
                    f'({cluster.get("status","")})</div>'
                )
            ev_rows = "".join(
                f'<div class="ev-row">'
                f'<span>{html.escape(e.get("ts","")[:19])}</span>'
                f'<span>{html.escape(e.get("provider",""))} · {e.get("event_id","")} · {html.escape(e.get("message","")[:80])}</span>'
                f'</div>'
                for e in inc.get("evidence", [])[:3]
            )
            actions_html = "".join(
                f'<div class="action-row"><div class="bullet"></div>{html.escape(a)}</div>'
                for a in inc.get("recommended_actions", [])
            )
            inc_cards.append(
                f'<div class="incident">'
                f'  <div class="top">'
                f'    <div class="h">{html.escape(inc.get("title","") or inc.get("type",""))}'
                f'      <small>{inc.get("type","")}</small></div>'
                f'    <div class="sev">{sev}</div>'
                f'  </div>'
                f'  {cluster_line}'
                f'  <div class="body">Confidence: {inc.get("confidence","")}</div>'
                f'  <div class="actions-list">{actions_html}</div>'
                f'  <div class="evidence">{ev_rows}</div>'
                f'</div>'
            )
        st.markdown(
            f'<div class="incidents">{"".join(inc_cards)}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No incidents detected for this host.")

    events = timeline.get("events", [])
    if events:
        st.markdown(
            '<p class="eyebrow" style="margin-top:24px;color:var(--ink-faint)">Recent events (sample)</p>',
            unsafe_allow_html=True,
        )
        st.dataframe(pd.DataFrame(events).head(50), use_container_width=True)


# ─────────────────────────────────────────────────────────
# Validation tab
# ─────────────────────────────────────────────────────────

def render_validation(store, run_id: str) -> None:
    report_key = f"{run_id}/validation_report.md"
    if store.exists(report_key):
        st.markdown(store.read_text(report_key))
    else:
        st.info("No validation report found for this run.")


# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Kevät · Pre-emptive IT",
        page_icon="🌱",
        layout="wide",
    )
    inject_css()

    # Handle regeneration before rendering anything else
    if st.session_state.get("_regen"):
        del st.session_state["_regen"]
        with st.spinner("Growing new data…"):
            _bootstrap_demo_data(force=True)
        st.rerun()

    with st.spinner("Preparing demo data…"):
        _bootstrap_demo_data()

    with st.sidebar:
        st.markdown(
            '<p class="eyebrow" style="color:var(--leaf-500)">Pre-emptive IT</p>',
            unsafe_allow_html=True,
        )
        st.markdown(
            "**Kevät demo** — synthetic fleet of 20 Windows endpoints. "
            "Incidents detected before they become outages."
        )
        st.divider()
        if st.button("🌱 Regenerate demo data", use_container_width=True):
            st.session_state["_regen"] = True
            st.rerun()
        st.divider()
        st.caption("*Kevät tulee aina.* Spring always comes.")
        st.markdown(
            "[Source on GitHub](https://github.com/Alleyfoo/Pre-emptive-IT-Incident-Dashboard)"
        )

    store = _store()
    runs = _available_runs(store)
    if not runs:
        st.error("Demo data generation failed. Click 'Regenerate demo data' in the sidebar.")
        return

    suggested = get_latest_run_id(store) or runs[-1]
    run_id = st.selectbox(
        "Run",
        runs,
        index=runs.index(suggested) if suggested in runs else 0,
        label_visibility="collapsed",
    )

    fleet = _fleet_summary(store, run_id)
    if not fleet:
        st.warning(f"No fleet summary found for run `{run_id}`.")
        return

    tab_fleet, tab_host, tab_validation = st.tabs(
        ["🌿 Fleet — last 24 h", "🔍 Host timeline", "✅ Validation"]
    )

    with tab_fleet:
        render_hero(fleet, run_id)
        render_meadow(fleet.get("clusters", []))
        render_cluster_ledger(fleet.get("clusters", []))
        render_hosts_grid(fleet.get("top_hosts", []))

    with tab_host:
        host_options = _host_options(store, run_id, fleet)
        if not host_options:
            st.info("No hosts available for this run.")
        else:
            host_id = st.selectbox("Host", host_options, index=0)
            render_host_view(store, run_id, host_id, fleet)

    with tab_validation:
        render_validation(store, run_id)

    st.markdown(
        '<div class="footnote"><em>Kevät tulee aina.</em>'
        '<span>All data is synthetic · generated with seed 42</span></div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
