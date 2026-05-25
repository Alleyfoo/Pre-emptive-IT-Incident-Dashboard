"""
Pre-emptive IT Incident Dashboard — Kevät demo.
Entry point for Streamlit Cloud. Auto-generates synthetic fleet data on first load.
"""

from __future__ import annotations

import html
import json
import math
import os
import random
import sys
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timedelta, timezone

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.artifact_store import build_artifact_store
from runtime.run_pointer import get_latest_run_id
from tools.generate_ticket_scenarios import ScenarioConfig, ScenarioGenerator

DEMO_RUN_ID = "demo"
ARTIFACTS_ROOT = os.environ.get("ARTIFACTS_ROOT") or os.path.join(
    REPO_ROOT, "artifacts"
)


# ─────────────────────────────────────────────────────────
# Store helpers
# ─────────────────────────────────────────────────────────


def _store():
    return build_artifact_store(ARTIFACTS_ROOT)


def _load_json(store, key: str) -> Dict:
    if not store.exists(key):
        return {}
    return json.loads(store.read_text(key))


def _normalize_cluster(c: Dict) -> Dict:
    """Return a copy of ``c`` augmented to the canonical demo cluster shape.

    The project produces two cluster schemas; the analyzer (used by
    ``demo-test*``) emits ``affected_hosts`` as a host **list** plus an
    ``affected_host_count`` integer, ``incident_types`` as a list, ``signature``
    as the hash, and a nested ``basis`` object. The bootstrap demo emits
    ``affected_hosts`` as an int count, ``example_hosts`` as the host list,
    ``type`` as a string, and flat ``signature_hash`` / ``signature_key`` keys.

    All view code in this module assumes the bootstrap-demo shape, so we
    normalize at the load boundary. Original keys are preserved alongside the
    canonical ones.
    """
    out = dict(c)

    raw_hosts = c.get("affected_hosts")
    if isinstance(raw_hosts, list):
        out["example_hosts"] = raw_hosts
        out["affected_hosts"] = c.get("affected_host_count", len(raw_hosts))
    else:
        out["affected_hosts"] = int(raw_hosts or 0)
        out.setdefault("example_hosts", [])

    if "signature_hash" not in out and c.get("signature"):
        out["signature_hash"] = c["signature"]

    if "type" not in out:
        types = c.get("incident_types") or []
        if types:
            out["type"] = types[0]

    if not out.get("signature_key") and isinstance(c.get("basis"), dict):
        b = c["basis"]
        out["signature_key"] = (
            f"{b.get('provider', '')}:{b.get('event_id', '')}"
            f"|{b.get('message_template', '')}"
        )

    out.setdefault("status", "ongoing")
    return out


def _normalize_fleet(fleet: Dict) -> Dict:
    """Apply :func:`_normalize_cluster` to every cluster in ``fleet`` (in place)."""
    if not fleet:
        return fleet
    fleet["clusters"] = [_normalize_cluster(c) for c in fleet.get("clusters", [])]
    return fleet


def _fleet_summary(store, run_id: str) -> Dict:
    return _normalize_fleet(_load_json(store, f"{run_id}/fleet_summary.json"))


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
    write_host_artifacts(
        store, run_id, timelines, fleet_window=fleet.get("window"), host_meta=host_meta
    )
    write_fleet_artifacts(store, run_id, fleet)
    try:
        validate_or_raise(store, run_id)
    except Exception:
        pass
    try:
        from tools.validate import validate as run_validation

        run_validation(run_id=run_id, artifacts_root=ARTIFACTS_ROOT)
    except SystemExit:
        pass  # schema errors are logged in the report; don't crash the app
    except Exception:
        pass
    write_latest(store, run_id)


def _bootstrap_demo_data(force: bool = False) -> None:
    store = _store()
    if not force and store.exists(f"{DEMO_RUN_ID}/fleet_summary.json"):
        return
    seed = int(time.time()) % 100000 if force else 42
    st.session_state["_seed"] = seed
    store.delete_prefix(DEMO_RUN_ID)
    config = ScenarioConfig(
        run_id=DEMO_RUN_ID,
        seed=seed,
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
    bsheet = (here / "broadsheet.css").read_text(encoding="utf-8")
    cluster_nav_css = """
.kv-cluster-nav button {
    background: none !important;
    border: none !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    padding: 6px 0 !important;
    font-family: var(--font-body) !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    color: var(--ink) !important;
    text-align: left !important;
    width: 100% !important;
    cursor: pointer !important;
    transition: color 0.15s !important;
}
.kv-cluster-nav button:hover {
    color: var(--leaf-600, #3d6b4a) !important;
    background: none !important;
    border: none !important;
    box-shadow: none !important;
}
.kv-cluster-nav button[data-selected="true"] {
    color: var(--leaf-500) !important;
}
"""
    st.markdown(
        f"<style>{base}\n{over}\n{cluster_nav_css}\n{bsheet}</style>",
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,500&family=Plus+Jakarta+Sans:wght@400;500;600&family=Inter+Tight:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap"
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


def _is_workshop() -> bool:
    return "Workshop" in st.session_state.get("_view", "📰 Broadsheet")


def _sev_label(s: int) -> str:
    if _is_workshop():
        return {
            "cool": "Idle",
            "warm": "Warm",
            "hot": "Overheating",
            "critical": "Down",
        }[_sev_class(s)]
    return {
        "cool": "Calm",
        "warm": "Sprouting",
        "hot": "Flowering",
        "critical": "Overgrown",
    }[_sev_class(s)]


def _format_last_ts(ts: str) -> str:
    """Return a human-readable relative time string from an ISO timestamp."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - dt
        minutes = int(delta.total_seconds() / 60)
        if minutes < 2:
            return "just now"
        if minutes < 60:
            return f"{minutes}m ago"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h ago"
        return f"{hours // 24}d ago"
    except Exception:
        return ts[:16] if ts else "—"


def _parse_event_hour(ts: str, window_start: datetime) -> float:
    """Return fractional hours (0–24) of ts relative to window_start, clamped."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - window_start).total_seconds() / 3600.0
        return max(0.0, min(24.0, delta))
    except Exception:
        return 0.0


def _meadow_scene_svg(w: int = 900, h: int = 110, seed: int = 0) -> str:
    """Decorative dawn-horizon SVG strip — sun, hills, birch trees, wildflowers."""
    # Sky gradient
    sky = (
        f"<defs>"
        f'<linearGradient id="msky-{seed}" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="#dce9f5" />'
        f'<stop offset="100%" stop-color="#f8f3e8" /></linearGradient>'
        f'<linearGradient id="mhill-{seed}" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="#b5c9a3" />'
        f'<stop offset="100%" stop-color="#8aaa78" /></linearGradient>'
        f"</defs>"
        f'<rect width="{w}" height="{h}" fill="url(#msky-{seed})" />'
    )

    # Sun
    sun_x = 80 + (seed % 120)
    sun_y = h * 0.38
    sun = (
        f'<circle cx="{sun_x:.0f}" cy="{sun_y:.1f}" r="18" fill="#f5d77a" opacity="0.85" />'
        f'<circle cx="{sun_x:.0f}" cy="{sun_y:.1f}" r="13" fill="#f7e49a" />'
    )

    # Rolling hills
    hill_y = h * 0.55
    hills = (
        f'<ellipse cx="{w * 0.18:.0f}" cy="{hill_y + 20:.0f}" rx="{w * 0.22:.0f}" ry="55" fill="url(#mhill-{seed})" />'
        f'<ellipse cx="{w * 0.55:.0f}" cy="{hill_y + 30:.0f}" rx="{w * 0.32:.0f}" ry="60" fill="#9eba8c" />'
        f'<ellipse cx="{w * 0.88:.0f}" cy="{hill_y + 18:.0f}" rx="{w * 0.24:.0f}" ry="52" fill="#adc49a" />'
        f'<rect x="0" y="{hill_y + 42:.0f}" width="{w}" height="{h:.0f}" fill="#c5d6b3" />'
    )

    # Birch trees
    def _birch(tx: float, ty: float, th: float, tidx: int) -> str:
        tw = th * 0.06
        trunk = f'<rect x="{tx - tw/2:.1f}" y="{ty - th:.1f}" width="{tw:.1f}" height="{th:.1f}" rx="2" fill="#e8e0d0" />'
        marks = "".join(
            f'<rect x="{tx - tw/2 - 1:.1f}" y="{ty - th * (0.3 + i * 0.2):.1f}" '
            f'width="{tw + 2:.1f}" height="1.5" rx="0.5" fill="#b8a898" opacity="0.55" />'
            for i in range(3)
        )
        crown = (
            f'<ellipse cx="{tx:.1f}" cy="{ty - th - 10:.1f}" rx="{th * 0.16:.1f}" ry="{th * 0.22:.1f}" fill="#7eaa6a" opacity="0.88" />'
            f'<ellipse cx="{tx - th * 0.1:.1f}" cy="{ty - th - 6:.1f}" rx="{th * 0.12:.1f}" ry="{th * 0.16:.1f}" fill="#8db87a" opacity="0.75" />'
        )
        return trunk + marks + crown

    tree_seed = seed % 7
    trees_svg = "".join(
        _birch(w * fx, h * 0.85, 42 + (tree_seed * i % 14), i)
        for i, fx in enumerate([0.08, 0.22, 0.41, 0.63, 0.77, 0.92])
    )

    # Wildflowers
    def _flower(fx: float, fy: float, fc: str) -> str:
        return (
            f'<circle cx="{fx:.1f}" cy="{fy:.1f}" r="2.2" fill="{fc}" />'
            f'<line x1="{fx:.1f}" y1="{fy:.1f}" x2="{fx:.1f}" y2="{fy + 7:.1f}" '
            f'stroke="#5a8a4a" stroke-width="0.9" />'
        )

    colors = ["#e8b4c8", "#f5d77a", "#c8d8f0", "#d4e8c0", "#f0c8a0"]
    flowers_svg = "".join(
        _flower(
            w * (0.05 + (i * 0.13 + seed * 0.03) % 0.9),
            h * 0.88 + (i % 3) * 2,
            colors[i % len(colors)],
        )
        for i in range(14)
    )

    return (
        f'<svg width="100%" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid slice" '
        f'style="display:block;border-radius:12px 12px 0 0;overflow:hidden" aria-hidden="true">'
        f"{sky}{sun}{hills}{trees_svg}{flowers_svg}"
        f"</svg>"
    )


# ─────────────────────────────────────────────────────────
# Workshop scene HTML (port of scenes.jsx WorkshopStripe)
# ─────────────────────────────────────────────────────────


def _workshop_scene_html(h: int = 110) -> str:
    """Workbench scene: pegboard wall, desk edge, tool items."""
    s = "var(--ink-soft)"
    desk_y = int(h * 0.65)
    peg_h = int(h * 0.5)

    def _lamp():
        return (
            f'<svg viewBox="-12 -16 24 24" width="24" height="24">'
            f'<path d="M -8 -4 L 8 -4 L 5 -1 L -5 -1 Z" fill="var(--pollen-200)" stroke="{s}" stroke-width="0.6" />'
            f'<line x1="0" y1="-1" x2="0" y2="6" stroke="{s}" stroke-width="0.6" />'
            f'<ellipse cx="0" cy="6" rx="3" ry="0.8" fill="var(--earth-400)" />'
            f'<ellipse cx="0" cy="-4" rx="6" ry="2" fill="var(--sun)" opacity="0.4" /></svg>'
        )

    def _mug():
        return (
            f'<svg viewBox="-7 -10 14 14" width="22" height="22">'
            f'<rect x="-4" y="-6" width="8" height="7" rx="0.6" fill="var(--paper)" stroke="{s}" stroke-width="0.5" />'
            f'<ellipse cx="0" cy="-6" rx="4" ry="0.9" fill="#6b4a2a" />'
            f'<path d="M 4 -4.5 q 2 -0.3 2 1.5 q 0 1.5 -2 1.5" fill="none" stroke="{s}" stroke-width="0.5" />'
            f'<path d="M -1.5 -7.5 q 0.5 -1.2 0 -2.4" fill="none" stroke="var(--ink-faint)" stroke-width="0.4" opacity="0.6" />'
            f'<path d="M 1.5 -8 q 0.5 -1.2 0 -2.4" fill="none" stroke="var(--ink-faint)" stroke-width="0.4" opacity="0.5" /></svg>'
        )

    def _notebook():
        return (
            f'<svg viewBox="-7 -7 14 12" width="22" height="20">'
            f'<rect x="-6" y="-4" width="12" height="7" rx="0.5" fill="var(--earth-200)" stroke="var(--earth-400)" stroke-width="0.4" transform="rotate(-4)" />'
            f'<line x1="-4" y1="-1.6" x2="3" y2="-2" stroke="var(--ink-faint)" stroke-width="0.35" transform="rotate(-4)" />'
            f'<line x1="-4" y1="0.3" x2="2" y2="-0.1" stroke="var(--ink-faint)" stroke-width="0.35" transform="rotate(-4)" /></svg>'
        )

    def _paperclip():
        return (
            f'<svg viewBox="-5 -6 10 12" width="14" height="18" stroke="{s}" stroke-width="0.55" fill="none">'
            f'<path d="M -2 -4 L -2 3 q 0 1 1 1 q 1 0 1 -1 L 0 -3 q 0 -0.6 0.6 -0.6 q 0.6 0 0.6 0.6 L 1.2 3.5" transform="rotate(18)" /></svg>'
        )

    def _sticky():
        return (
            f'<svg viewBox="-6 -6 12 12" width="22" height="22">'
            f'<rect x="-5" y="-5" width="10" height="10" rx="0.3" fill="var(--pollen-200)" stroke="var(--earth-200)" stroke-width="0.3" transform="rotate(6)" />'
            f'<line x1="-3" y1="-2" x2="2.5" y2="-2" stroke="var(--earth-600)" stroke-width="0.3" opacity="0.6" transform="rotate(6)" />'
            f'<line x1="-3" y1="-0.3" x2="3" y2="-0.3" stroke="var(--earth-600)" stroke-width="0.3" opacity="0.6" transform="rotate(6)" />'
            f'<line x1="-3" y1="1.4" x2="2" y2="1.4" stroke="var(--earth-600)" stroke-width="0.3" opacity="0.6" transform="rotate(6)" /></svg>'
        )

    def _screws():
        dots = "".join(
            f'<g transform="translate({x} 0)">'
            f'<circle r="1.2" fill="var(--ink-soft)" />'
            f'<path d="M -0.7 0 L 0.7 0" stroke="var(--paper)" stroke-width="0.3" /></g>'
            for x in [-3.5, 0, 3.5]
        )
        return f'<svg viewBox="-6 -3 12 6" width="22" height="10">{dots}</svg>'

    def _tape():
        return (
            f'<svg viewBox="-6 -6 12 12" width="22" height="22">'
            f'<circle r="4.5" fill="var(--water-200)" stroke="{s}" stroke-width="0.4" />'
            f'<circle r="1.5" fill="var(--paper)" stroke="{s}" stroke-width="0.3" /></svg>'
        )

    def _plant():
        return (
            f'<svg viewBox="-7 -10 14 14" width="22" height="22">'
            f'<path d="M -3 0 L 3 0 L 2.4 3 L -2.4 3 Z" fill="var(--earth-600)" />'
            f'<line x1="0" y1="0" x2="0" y2="-7" stroke="var(--leaf-500)" stroke-width="0.4" />'
            f'<ellipse cx="-2" cy="-3" rx="1.8" ry="1" fill="var(--leaf-300)" transform="rotate(-20 -2 -3)" />'
            f'<ellipse cx="2.3" cy="-4.5" rx="1.8" ry="1" fill="var(--leaf-400)" transform="rotate(20 2.3 -4.5)" />'
            f'<ellipse cx="-1.5" cy="-6.5" rx="1.5" ry="0.9" fill="var(--leaf-300)" transform="rotate(-30 -1.5 -6.5)" />'
            f'<ellipse cx="0" cy="-7.5" rx="0.7" ry="1.2" fill="var(--leaf-500)" /></svg>'
        )

    def _pencil():
        return (
            f'<svg viewBox="-7 -2 14 4" width="22" height="8">'
            f'<rect x="-6" y="-1" width="9" height="2" fill="var(--pollen-400)" stroke="{s}" stroke-width="0.3" />'
            f'<polygon points="3,-1 5,0 3,1" fill="#d8c08a" stroke="{s}" stroke-width="0.3" />'
            f'<rect x="-6" y="-1" width="1.5" height="2" fill="var(--sev-3)" stroke="{s}" stroke-width="0.3" /></svg>'
        )

    def _screwdriver():
        return (
            f'<svg viewBox="-8 -3 16 6" width="28" height="10">'
            f'<rect x="-7" y="-1.2" width="6" height="2.4" rx="0.4" fill="var(--sev-3)" stroke="{s}" stroke-width="0.3" />'
            f'<rect x="-1" y="-0.4" width="6" height="0.8" fill="var(--ink-soft)" />'
            f'<polygon points="5,-0.4 6.5,0 5,0.4" fill="var(--ink-soft)" /></svg>'
        )

    def _pegholes():
        dots = "".join(
            f'<circle cx="{x}" cy="-6" r="0.5" fill="rgba(60,50,40,0.25)" />'
            for x in [-4, -1.5, 1, 3.5]
        )
        return f'<svg viewBox="-6 -8 12 4" width="18" height="6">{dots}</svg>'

    item_map = {
        "lamp": _lamp,
        "mug": _mug,
        "notebook": _notebook,
        "paperclip": _paperclip,
        "sticky": _sticky,
        "screws": _screws,
        "tape": _tape,
        "plant": _plant,
        "pencil": _pencil,
        "screwdriver": _screwdriver,
        "pegholes": _pegholes,
    }

    items = [
        (5, "lamp", False),
        (12, "pegholes", True),
        (19, "screwdriver", False),
        (24, "pegholes", True),
        (30, "mug", False),
        (39, "notebook", False),
        (47, "paperclip", False),
        (53, "sticky", False),
        (62, "pegholes", True),
        (68, "screws", False),
        (75, "tape", False),
        (83, "plant", False),
        (92, "pencil", False),
    ]

    # Pegboard dot grid
    peg_rows = int(peg_h / 6)
    peg_cols = 40
    peg_dots = "".join(
        f'<circle cx="{5 + i * 5}" cy="{6 + j * 6}" r="0.4" fill="rgba(60,50,40,0.4)" />'
        for i in range(peg_cols)
        for j in range(peg_rows)
    )
    pegboard_svg = (
        f'<svg width="100%" height="{peg_h}" viewBox="0 0 200 {peg_h}" preserveAspectRatio="none" '
        f'style="position:absolute;top:0;left:0;opacity:0.35" aria-hidden="true">{peg_dots}</svg>'
    )

    item_divs = ""
    for x_pct, el, is_peg in items:
        bottom_px = int(h * 0.55) if is_peg else int(h * 0.18)
        svg = item_map[el]()
        item_divs += (
            f'<div style="position:absolute;left:{x_pct}%;bottom:{bottom_px}px;transform:translateX(-50%)">'
            f"{svg}</div>"
        )

    coffee_ring_top = int(h * 0.68)

    return (
        f'<div style="position:relative;height:{h}px;overflow:hidden;border-radius:12px 12px 0 0;'
        f'background:linear-gradient(180deg,#f0e3c2 0%,#e5d4a8 65%,#d4be86 65%,#c9b075 100%)">'
        f"{pegboard_svg}"
        # desk edge lines
        f'<div style="position:absolute;left:0;right:0;top:{desk_y}px;height:1px;background:var(--earth-600);opacity:0.4"></div>'
        f'<div style="position:absolute;left:0;right:0;top:{desk_y + 2}px;height:1px;background:var(--earth-400);opacity:0.3"></div>'
        # lamp glow
        f'<div style="position:absolute;left:1%;top:0;width:16%;height:100%;'
        f'background:radial-gradient(ellipse at 30% 50%,rgba(241,185,74,0.35),transparent 60%)"></div>'
        # coffee ring
        f'<div style="position:absolute;left:31%;top:{coffee_ring_top}px;width:24px;height:6px;'
        f'border-radius:50%;border:1px solid rgba(110,76,40,0.25);opacity:0.6"></div>'
        f"{item_divs}"
        f"</div>"
    )


# ─────────────────────────────────────────────────────────
# Sprout SVG (ported from components.jsx)
# ─────────────────────────────────────────────────────────


def sprout_svg(
    leaves: int,
    sev: int,
    bloom: bool = False,
    index: int = 0,
    w: int = 120,
    h: int = 240,
) -> str:
    """Parametric SVG plant — faithful port of components.jsx.
    Health inverts with severity: low sev = tall lush plant, high sev = broken stem.
    """
    sev_c = _sev_class(sev)
    health = max(0.0, min(1.0, 1.0 - sev / 120.0))
    height_frac = 0.50 + 0.50 * health
    stem_top_y = h * (1 - height_frac) + 6
    stem_base_y = h - 10
    cx = w / 2

    seed = (index * 73 + leaves * 11 + round(sev)) % 100
    ws = 1 if seed % 2 == 0 else -1  # wobble sign
    sc = 6 + (seed % 6)  # stem curve

    broken = sev_c == "critical"
    drooping = sev_c in ("hot", "critical")
    wilted = sev_c in ("warm", "hot", "critical")
    mid_y = (stem_top_y + stem_base_y) / 2

    crown_x, crown_y = cx, stem_top_y
    snap_mark = ""

    if broken:
        snap_y = stem_base_y + (stem_top_y - stem_base_y) * 0.55
        snap_x = cx + ws * 4
        dangle_x = snap_x + ws * 28
        dangle_y = snap_y + 26
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx + sc*ws:.1f} {(stem_base_y+snap_y)/2+4:.1f}, "
            f"{cx + 2*ws:.1f} {snap_y+6:.1f}, {snap_x:.1f} {snap_y:.1f} "
            f"M {snap_x+ws*3:.1f} {snap_y+1:.1f} "
            f"L {snap_x+ws*10:.1f} {snap_y+4:.1f} "
            f"Q {snap_x+ws*22:.1f} {snap_y+10:.1f} {dangle_x:.1f} {dangle_y:.1f}"
        )
        crown_x, crown_y = dangle_x, dangle_y
        snap_mark = (
            f'<g stroke="#6b5a40" stroke-width="0.9" fill="none" stroke-linecap="round">'
            f'<path d="M {snap_x-3:.1f} {snap_y-1:.1f} l 2 -2 l -1 -2 l 3 -1" />'
            f'<path d="M {snap_x+3:.1f} {snap_y+1:.1f} l -1 2 l 2 1" /></g>'
        )
    elif drooping:
        tip_x = cx + ws * 16
        tip_y = stem_top_y + 22
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx+sc*ws:.1f} {mid_y+22:.1f}, {cx-4*ws:.1f} {mid_y-8:.1f}, "
            f"{cx+4*ws:.1f} {stem_top_y+4:.1f} "
            f"Q {cx+14*ws:.1f} {stem_top_y+8:.1f} {tip_x:.1f} {tip_y:.1f}"
        )
        crown_x, crown_y = tip_x, tip_y
    elif wilted:
        tip_x = cx + ws * 6
        tip_y = stem_top_y + 4
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx+sc*ws:.1f} {mid_y+18:.1f}, {cx-sc*ws:.1f} {mid_y-14:.1f}, "
            f"{tip_x:.1f} {tip_y:.1f}"
        )
        crown_x, crown_y = tip_x, tip_y
    else:
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx+sc*ws:.1f} {mid_y+22:.1f}, {cx-sc*ws:.1f} {mid_y-22:.1f}, {cx:.1f} {stem_top_y:.1f}"
        )

    stem_color = "#8a6a3f" if broken else "#9aa37c" if drooping else "var(--leaf-400)"

    green_pal = ["var(--leaf-500)", "var(--leaf-300)", "var(--leaf-200)"]
    yellow_pal = ["var(--leaf-400)", "#c9b76b", "#d8c97d"]
    brown_pal = ["#9aa37c", "#a3956b", "#8a6a3f"]
    palette = brown_pal if (broken or drooping) else yellow_pal if wilted else green_pal

    usable = max(3, min(8, leaves + 1))
    fallen_count = (
        min(usable - 1, 3)
        if broken
        else min(usable - 1, 2) if drooping else 1 if wilted else 0
    )
    attached = max(2, usable - fallen_count)

    leaf_parts: List[str] = []
    for i in range(attached):
        t = 0.12 + (i / max(attached - 1, 1)) * 0.62
        t_eff = t * 0.55 if broken else t
        y = stem_base_y + (stem_top_y - stem_base_y) * t_eff
        side = -1 if i % 2 == 0 else 1
        lsz = 14 + (i / usable) * 4
        lx = cx + math.sin(t * math.pi) * sc * ws * 0.6
        fill = palette[i % 3]
        angle = (
            side * (32 + i * 3)
            + side * (18 if wilted else 0)
            + side * (14 if drooping else 0)
        )
        curl_y = lsz * 0.7 if wilted else -lsz * 0.55
        leaf_parts.append(
            f'<g transform="translate({lx:.1f} {y:.1f}) rotate({angle})">'
            f'<path d="M 0 0 Q {side*lsz*0.55:.1f} {curl_y:.1f} {side*lsz:.1f} 0 '
            f'Q {side*lsz*0.55:.1f} {lsz*0.55:.1f} 0 0 Z" fill="{fill}" />'
            f'<line x1="0" y1="0" x2="{side*lsz:.1f}" y2="0" stroke="rgba(255,255,255,0.22)" stroke-width="0.7" />'
            f"</g>"
        )

    fallen_parts: List[str] = []
    for i in range(fallen_count):
        fx = cx + (i - fallen_count / 2) * 14 + (seed % 7) - 3
        fy = stem_base_y + 3
        rot = ((seed * (i + 1)) % 90) - 45
        fallen_parts.append(
            f'<g transform="translate({fx:.1f} {fy:.1f}) rotate({rot})">'
            f'<path d="M 0 0 Q 5 -3 10 0 Q 5 3 0 0 Z" fill="{palette[(i+1)%3]}" opacity="0.85" /></g>'
        )

    # Crown — severity-driven, with optional forced bloom for new clusters
    if sev_c == "cool":
        if bloom or leaves >= 3:
            back = "".join(
                f'<ellipse cx="0" cy="-7" rx="4.5" ry="6.5" transform="rotate({d})" fill="var(--bloom-edge)" />'
                for d in [36, 108, 180, 252, 324]
            )
            front = "".join(
                f'<ellipse cx="0" cy="-8" rx="5" ry="7.5" transform="rotate({d})" fill="var(--bloom)" stroke="var(--earth-200)" stroke-width="0.7" />'
                for d in [0, 72, 144, 216, 288]
            )
            crown_el = (
                f'<g transform="translate({crown_x:.1f} {crown_y-4:.1f})" filter="url(#spr-sh-{index})">'
                f'{back}{front}<circle r="3.2" fill="var(--sun)" /></g>'
            )
        else:
            crown_el = (
                f'<g transform="translate({crown_x:.1f} {crown_y-2:.1f})">'
                f'<ellipse cx="0" cy="-3" rx="3.5" ry="5.5" fill="var(--leaf-500)" />'
                f'<ellipse cx="0" cy="-2" rx="2" ry="3.5" fill="var(--leaf-300)" /></g>'
            )
    elif sev_c == "warm":
        petals = "".join(
            f'<ellipse cx="0" cy="-5" rx="4" ry="5.5" transform="rotate({d})" fill="var(--bloom-edge)" stroke="#d8c97d" stroke-width="0.6" />'
            for d in [200, 270, 340]
        )
        crown_el = (
            f'<g transform="translate({crown_x:.1f} {crown_y-2:.1f})">'
            f'{petals}<circle r="2.8" fill="#c79f2c" /></g>'
        )
    elif sev_c == "hot":
        crown_el = (
            f'<g transform="translate({crown_x:.1f} {crown_y:.1f})">'
            f'<ellipse cx="0" cy="2" rx="6" ry="3" fill="#9aa37c" transform="rotate(20)" />'
            f'<ellipse cx="2" cy="4" rx="5" ry="2.6" fill="#a3956b" transform="rotate(35)" /></g>'
        )
    else:  # critical
        crown_el = (
            f'<g transform="translate({crown_x:.1f} {crown_y:.1f})">'
            f'<path d="M -4 -2 q 4 -3 8 0 q 0 4 -4 4 q -4 0 -4 -4 Z" fill="#8a6a3f" />'
            f'<path d="M -3 -1 q 3 -2 6 0 q 0 3 -3 3 q -3 0 -3 -3 Z" fill="#6b5a40" /></g>'
        )

    ground_fill = (
        "rgba(138,106,63,0.35)"
        if broken
        else "rgba(201,163,110,0.28)" if drooping else "rgba(201,163,110,0.22)"
    )
    ground = f'<ellipse cx="{cx:.1f}" cy="{stem_base_y+3:.1f}" rx="18" ry="2.6" fill="{ground_fill}" />'

    cracks = ""
    if broken:
        cracks = (
            f'<g stroke="#8a6a3f" stroke-width="0.6" opacity="0.6" fill="none">'
            f'<path d="M {cx-12:.1f} {stem_base_y+5:.1f} L {cx-6:.1f} {stem_base_y+3:.1f} L {cx:.1f} {stem_base_y+5:.1f}" />'
            f'<path d="M {cx+4:.1f} {stem_base_y+6:.1f} L {cx+10:.1f} {stem_base_y+4:.1f}" /></g>'
        )

    return (
        f'<svg class="plant-svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" aria-hidden="true">'
        f"<defs>"
        f'<filter id="spr-sh-{index}" x="-50%" y="-50%" width="200%" height="200%">'
        f'<feGaussianBlur in="SourceAlpha" stdDeviation="1.2" />'
        f'<feOffset dx="0" dy="1.5" />'
        f'<feComponentTransfer><feFuncA type="linear" slope="0.28" /></feComponentTransfer>'
        f'<feMerge><feMergeNode /><feMergeNode in="SourceGraphic" /></feMerge>'
        f"</filter>"
        f"</defs>"
        f"{ground}{cracks}"
        f'<path d="{stem_path}" fill="none" stroke="{stem_color}" stroke-width="2.4" stroke-linecap="round" />'
        f"{snap_mark}"
        f'{"".join(leaf_parts)}'
        f'{"".join(fallen_parts)}'
        f"{crown_el}"
        f"</svg>"
    )


# ─────────────────────────────────────────────────────────
# Computer SVG (port of computers.jsx)
# ─────────────────────────────────────────────────────────


def computer_svg(
    host_id: str = "HOST-001",
    sev: int = 60,
    index: int = 0,
    w: int = 108,
    h: int = 130,
    label: bool = False,
) -> str:
    sev_c = _sev_class(sev)
    seed = (index * 31 + len(host_id) * 7) % 100
    cx = w / 2
    screen_w = w * 0.72
    screen_h = h * 0.52
    screen_x = (w - screen_w) / 2
    screen_y = 14

    body_fill = {
        "critical": "#c7b59b",
        "hot": "#d5c4a4",
        "warm": "#dccfa9",
        "cool": "#dde7d0",
    }[sev_c]
    screen_fill = {
        "critical": "#7a5a3a",
        "hot": "#9a7956",
        "warm": "#cfc28a",
        "cool": "#cfe2c2",
    }[sev_c]
    accent = {
        "critical": "var(--sev-4)",
        "hot": "var(--sev-3)",
        "warm": "var(--pollen-600)",
        "cool": "var(--leaf-500)",
    }[sev_c]
    led_color = {
        "critical": "var(--sev-4)",
        "hot": "var(--sev-3)",
        "warm": "var(--sun)",
        "cool": "var(--leaf-300)",
    }[sev_c]

    # Tilt — monitor tilts with severity, anchored at stand bottom
    tilt = {
        "critical": -10 if seed % 2 == 0 else 10,
        "hot": -4 if seed % 2 == 0 else 4,
        "warm": -1.5 if seed % 2 == 0 else 1.5,
        "cool": 0,
    }[sev_c]
    y_shift = 6 if sev_c == "critical" else 0

    eye_shift = (seed % 5) - 2
    face_y = screen_y + screen_h * 0.5
    eye_y = face_y - 4
    mouth_y = face_y + 12

    # Face per mood
    if sev_c == "cool":
        face = (
            f'<circle cx="{cx - 14 + eye_shift:.1f}" cy="{eye_y:.1f}" r="2.6" fill="var(--ink)" />'
            f'<circle cx="{cx + 14 + eye_shift:.1f}" cy="{eye_y:.1f}" r="2.6" fill="var(--ink)" />'
            f'<circle cx="{cx - 14 + eye_shift - 0.8:.1f}" cy="{eye_y - 0.8:.1f}" r="0.7" fill="#fff" />'
            f'<circle cx="{cx + 14 + eye_shift - 0.8:.1f}" cy="{eye_y - 0.8:.1f}" r="0.7" fill="#fff" />'
            f'<circle cx="{cx - 22:.1f}" cy="{eye_y + 6:.1f}" r="1.6" fill="#e69aa1" opacity="0.6" />'
            f'<circle cx="{cx + 22:.1f}" cy="{eye_y + 6:.1f}" r="1.6" fill="#e69aa1" opacity="0.6" />'
            f'<path d="M {cx - 8:.1f} {mouth_y:.1f} Q {cx:.1f} {mouth_y + 5:.1f} {cx + 8:.1f} {mouth_y:.1f}" '
            f'fill="none" stroke="var(--ink)" stroke-width="1.6" stroke-linecap="round" />'
        )
    elif sev_c == "warm":
        face = (
            f'<g stroke="var(--ink)" stroke-width="1.6" stroke-linecap="round" fill="none">'
            f'<path d="M {cx - 18:.1f} {eye_y + 1:.1f} q 4 -3 8 0" />'
            f'<path d="M {cx + 10:.1f} {eye_y + 1:.1f} q 4 -3 8 0" />'
            f'<ellipse cx="{cx:.1f}" cy="{mouth_y:.1f}" rx="2.8" ry="2.2" fill="var(--ink)" stroke="none" />'
            f'<text x="{cx + 26:.1f}" y="{face_y - 8:.1f}" font-family="Newsreader, serif" font-size="13" font-style="italic" '
            f'fill="var(--pollen-600)" font-weight="500" stroke="none">z</text>'
            f"</g>"
        )
    elif sev_c == "hot":
        face = (
            f'<circle cx="{cx - 14 + eye_shift:.1f}" cy="{eye_y:.1f}" r="3" fill="var(--ink)" />'
            f'<circle cx="{cx + 14 + eye_shift:.1f}" cy="{eye_y:.1f}" r="3" fill="var(--ink)" />'
            f'<circle cx="{cx - 14 + eye_shift - 1:.1f}" cy="{eye_y - 1:.1f}" r="0.8" fill="#fff" />'
            f'<circle cx="{cx + 14 + eye_shift - 1:.1f}" cy="{eye_y - 1:.1f}" r="0.8" fill="#fff" />'
            f'<path d="M {cx - 7:.1f} {mouth_y + 2:.1f} Q {cx:.1f} {mouth_y - 4:.1f} {cx + 7:.1f} {mouth_y + 2:.1f}" '
            f'fill="none" stroke="var(--ink)" stroke-width="1.6" stroke-linecap="round" />'
            f'<path d="M {cx + 22:.1f} {eye_y - 4:.1f} q -3 6 0 9 q 3 -3 0 -9 Z" fill="var(--water-400)" />'
        )
    else:  # critical — × × eyes, yellow tones, error code
        face = (
            f'<g stroke="#f1d36e" stroke-width="2" stroke-linecap="round">'
            f'<line x1="{cx - 18:.1f}" y1="{eye_y - 3:.1f}" x2="{cx - 10:.1f}" y2="{eye_y + 3:.1f}" />'
            f'<line x1="{cx - 18:.1f}" y1="{eye_y + 3:.1f}" x2="{cx - 10:.1f}" y2="{eye_y - 3:.1f}" />'
            f'<line x1="{cx + 10:.1f}" y1="{eye_y - 3:.1f}" x2="{cx + 18:.1f}" y2="{eye_y + 3:.1f}" />'
            f'<line x1="{cx + 10:.1f}" y1="{eye_y + 3:.1f}" x2="{cx + 18:.1f}" y2="{eye_y - 3:.1f}" />'
            f"</g>"
            f'<line x1="{cx - 6:.1f}" y1="{mouth_y + 1:.1f}" x2="{cx + 6:.1f}" y2="{mouth_y + 1:.1f}" '
            f'stroke="#f1d36e" stroke-width="1.6" stroke-linecap="round" />'
            f'<text x="{cx:.1f}" y="{mouth_y + 12:.1f}" text-anchor="middle" '
            f'font-family="JetBrains Mono, monospace" font-size="6" fill="#f1d36e" letter-spacing="0.1em">0x7E</text>'
        )

    # Screen damage overlays
    if sev_c == "warm":
        screen_damage = (
            f'<g stroke="rgba(0,0,0,0.45)" stroke-width="0.7" fill="none">'
            f'<path d="M {screen_x + screen_w - 12:.1f} {screen_y + 2:.1f} l 4 5 l -3 3" /></g>'
        )
    elif sev_c == "hot":
        screen_damage = (
            f'<g stroke="rgba(0,0,0,0.55)" stroke-width="0.9" fill="none" stroke-linejoin="miter">'
            f'<path d="M {screen_x + 8:.1f} {screen_y + 3:.1f} L {screen_x + 18:.1f} {screen_y + 10:.1f} '
            f"L {screen_x + 12:.1f} {screen_y + 14:.1f} L {screen_x + 24:.1f} {screen_y + 22:.1f} "
            f'L {screen_x + 18:.1f} {screen_y + 26:.1f} L {screen_x + 30:.1f} {screen_y + screen_h - 4:.1f}" />'
            f'<path d="M {screen_x + 18:.1f} {screen_y + 10:.1f} l 4 2" />'
            f'<path d="M {screen_x + 24:.1f} {screen_y + 22:.1f} l 5 -1" /></g>'
        )
    elif sev_c == "critical":
        screen_damage = (
            f'<g stroke="rgba(0,0,0,0.65)" stroke-width="0.9" fill="none">'
            f'<path d="M {screen_x + 6:.1f} {screen_y + 2:.1f} '
            f"L {screen_x + screen_w * 0.45:.1f} {screen_y + screen_h * 0.4:.1f} "
            f'L {screen_x + screen_w - 8:.1f} {screen_y + 6:.1f}" />'
            f'<path d="M {screen_x + screen_w * 0.45:.1f} {screen_y + screen_h * 0.4:.1f} '
            f'L {screen_x + 12:.1f} {screen_y + screen_h - 4:.1f}" />'
            f'<path d="M {screen_x + screen_w * 0.45:.1f} {screen_y + screen_h * 0.4:.1f} '
            f'L {screen_x + screen_w - 6:.1f} {screen_y + screen_h - 8:.1f}" />'
            f'<path d="M {screen_x + screen_w * 0.45:.1f} {screen_y + screen_h * 0.4:.1f} '
            f'L {screen_x + screen_w * 0.65:.1f} {screen_y + 4:.1f}" /></g>'
            f'<g stroke="rgba(241,211,110,0.55)" stroke-width="1.2">'
            f'<line x1="{screen_x + 6:.1f}" y1="{screen_y + screen_h - 14:.1f}" '
            f'x2="{screen_x + screen_w - 14:.1f}" y2="{screen_y + screen_h - 14:.1f}" />'
            f'<line x1="{screen_x + 14:.1f}" y1="{screen_y + screen_h - 8:.1f}" '
            f'x2="{screen_x + screen_w - 6:.1f}" y2="{screen_y + screen_h - 8:.1f}" /></g>'
        )
    else:
        screen_damage = ""

    # Smoke wisps from screen top
    if sev_c in ("hot", "critical"):
        n_wisps = 3 if sev_c == "critical" else 2
        opacity = 0.55 if sev_c == "critical" else 0.4
        wisps = []
        for i in range(n_wisps):
            sx = screen_x + screen_w * (0.2 + i * 0.3)
            sy = screen_y - 4
            sw = 2.2 - i * 0.3
            wisps.append(
                f'<path d="M {sx:.1f} {sy:.1f} c -3 -4, 3 -8, 0 -12 c -3 -4, 3 -8, 0 -12" '
                f'fill="none" stroke="#7a7268" stroke-width="{sw:.1f}" stroke-linecap="round" '
                f'transform="translate(0 {i * -2})" />'
            )
        smoke = f'<g opacity="{opacity}">{"".join(wisps)}</g>'
    else:
        smoke = ""

    # Sparks for critical
    sparks = (
        (
            f'<g fill="#f1d36e">'
            f'<circle cx="{cx + 26:.1f}" cy="{screen_y - 2:.1f}" r="1.2" />'
            f'<circle cx="{cx + 30:.1f}" cy="{screen_y - 6:.1f}" r="0.8" opacity="0.7" />'
            f'<circle cx="{cx - 28:.1f}" cy="{screen_y - 4:.1f}" r="1" opacity="0.8" /></g>'
        )
        if sev_c == "critical"
        else ""
    )

    # Stand cracks for critical
    stand_cracks = (
        (
            f'<g stroke="#6b5a40" stroke-width="0.6" opacity="0.6">'
            f'<line x1="{cx - 12:.1f}" y1="{h - 16:.1f}" x2="{cx + 2:.1f}" y2="{h - 14:.1f}" />'
            f'<line x1="{cx + 4:.1f}" y1="{h - 14:.1f}" x2="{cx + 14:.1f}" y2="{h - 16:.1f}" /></g>'
        )
        if sev_c == "critical"
        else ""
    )

    # LED blink — faster for critical
    led_dur = "0.8s" if sev_c == "critical" else "1.6s"
    led_blink = (
        f'<animate attributeName="opacity" values="1;0.3;1" dur="{led_dur}" repeatCount="indefinite" />'
        if sev_c != "cool"
        else ""
    )

    # Optional host-id label
    label_el = (
        (
            f'<text x="{cx:.1f}" y="{h - 28:.1f}" text-anchor="middle" '
            f'font-family="var(--font-mono)" font-size="9" letter-spacing="0.06em" '
            f'fill="var(--ink-soft)" font-weight="600">{html.escape(host_id)}</text>'
        )
        if label
        else ""
    )

    safe_id = html.escape(host_id).replace(" ", "_").replace("-", "_")
    scan_id = f"scan_{index}_{safe_id}"
    shadow_id = f"shadow_{index}_{safe_id}"

    return (
        f'<svg class="computer-svg sym-{sev_c}" width="{w}" height="{h}" viewBox="0 0 {w} {h}" aria-hidden="true">'
        f"<defs>"
        f'<pattern id="{scan_id}" width="2" height="2" patternUnits="userSpaceOnUse">'
        f'<rect width="2" height="2" fill="{screen_fill}" />'
        f'<line x1="0" y1="0.5" x2="2" y2="0.5" stroke="rgba(0,0,0,0.05)" stroke-width="0.4" /></pattern>'
        f'<filter id="{shadow_id}" x="-30%" y="-30%" width="160%" height="160%">'
        f'<feGaussianBlur in="SourceAlpha" stdDeviation="2" />'
        f'<feOffset dx="0" dy="2" />'
        f'<feComponentTransfer><feFuncA type="linear" slope="0.18" /></feComponentTransfer>'
        f'<feMerge><feMergeNode /><feMergeNode in="SourceGraphic" /></feMerge></filter>'
        f"</defs>"
        # ground shadow
        f'<ellipse cx="{cx:.1f}" cy="{h - 6}" rx="{w * 0.32:.1f}" ry="3" fill="rgba(60,60,60,0.10)" />'
        # stand — fixed, does not tilt
        f'<rect x="{cx - 18:.1f}" y="{h - 22}" width="36" height="10" rx="2" '
        f'fill="{body_fill}" stroke="{accent}" stroke-opacity="0.25" stroke-width="1" />'
        f'<rect x="{cx - 28:.1f}" y="{h - 12}" width="56" height="6" rx="1.5" '
        f'fill="{body_fill}" stroke="{accent}" stroke-opacity="0.25" stroke-width="1" />'
        f"{stand_cracks}"
        # monitor body + screen, tilted
        f'<g transform="translate(0 {y_shift}) rotate({tilt:.1f} {cx:.1f} {h - 22})">'
        f"{smoke}{sparks}"
        f'<g filter="url(#{shadow_id})">'
        f'<rect x="{screen_x - 8:.1f}" y="{screen_y - 8}" width="{screen_w + 16:.1f}" height="{screen_h + 22:.1f}" '
        f'rx="10" fill="{body_fill}" stroke="rgba(0,0,0,0.12)" stroke-width="0.6" /></g>'
        f'<rect x="{screen_x:.1f}" y="{screen_y}" width="{screen_w:.1f}" height="{screen_h:.1f}" '
        f'rx="6" fill="url(#{scan_id})" stroke="rgba(0,0,0,0.18)" stroke-width="0.6" />'
        f"{face}"
        f"{screen_damage}"
        f'<circle cx="{screen_x + screen_w - 5:.1f}" cy="{screen_y + screen_h + 7:.1f}" r="1.4" fill="{led_color}">'
        f"{led_blink}</circle>"
        f"</g>"
        f"{label_el}"
        f"</svg>"
    )


def render_workshop(clusters: List[Dict]) -> None:
    cluster_cards = []
    for i, c in enumerate(clusters[:6]):
        sev = c.get("severity", 50)
        n_hosts = c.get("affected_hosts", 0)
        example = c.get("example_hosts", [])
        show_hosts = example[:5]
        overflow = n_hosts - len(show_hosts) if n_hosts > len(show_hosts) else 0

        computers_html = "".join(
            computer_svg(hid, sev, index=i * 10 + j, w=108, h=130)
            for j, hid in enumerate(show_hosts)
        )
        if overflow > 0:
            computers_html += (
                f'<div class="comp-more">'
                f'<span class="num">+{overflow}</span>'
                f'<span class="lbl">more</span>'
                f"</div>"
            )

        status = c.get("status", "ongoing")
        status_dot = "var(--leaf-400)" if status == "new" else "var(--sun)"
        status_label = "New issue" if status == "new" else "Running"
        sig_key = html.escape(
            (c.get("signature_key") or c.get("signature_hash", ""))[:40]
        )

        cluster_cards.append(
            f'<div class="comp-cluster">'
            f'  <div class="comp-cluster-row">{computers_html}</div>'
            f'  <div class="comp-cluster-meta">'
            f"    <div>"
            f'      <span class="name">{sig_key}</span>'
            f'      <span class="meta">{n_hosts} hosts · sev {sev}</span>'
            f"    </div>"
            f'    <span class="status-pill {status}">'
            f'      <span style="width:6px;height:6px;border-radius:50%;background:{status_dot};"></span>'
            f"      {status_label}"
            f"    </span>"
            f"  </div>"
            f"</div>"
        )

    scene = _workshop_scene_html(h=110)
    st.markdown(
        f"""
        <section class="workshop-wrap">
          {scene}
          <div class="workshop-head">
            <div>
              <p class="eyebrow" style="color: var(--leaf-500)">The Workshop</p>
              <h2>{len(clusters)} clusters — how are the machines feeling?</h2>
            </div>
            <div class="legend">
              <span>😊 calm</span>
              <span>😴 sleepy · watch</span>
              <span>😰 worried · attend</span>
              <span>🤒 unwell · urgent</span>
            </div>
          </div>
          <div class="workshop-grid">{"".join(cluster_cards)}</div>
        </section>
        """,
        unsafe_allow_html=True,
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
    workshop = _is_workshop()
    if workshop:
        headline = f'{affected} machines <span class="light">flagged.</span>'
        subline = f"{len(clusters)} issues detected. Start with the hottest — the rest should cool down by end of day."
        hosts_sub = "machines on the network"
    else:
        headline = f'{affected} hosts are <span class="light">stirring.</span>'
        subline = f"{len(clusters)} clusters surfaced. Tend the flowering ones first — the rest will be calm by lunch."
        hosts_sub = "hosts in the field"
    st.markdown(
        f"""
        <div class="hero">
          <div class="hero-left">
            <p class="eyebrow" style="color: var(--leaf-500)">Today · run {html.escape(run_id)}</p>
            <h1>{headline}</h1>
            <p>{subline}</p>
          </div>
          <div class="hero-right">
            <div class="kpi">
              <span class="eyebrow">Overall risk</span>
              <span class="num">{risk}</span>
            </div>
            <div class="kpi">
              <span class="eyebrow">{"Issues" if workshop else "Clusters"}</span>
              <span class="num">{len(clusters)}</span>
              <span class="delta warm">↗ {new} new today</span>
            </div>
            <div class="kpi">
              <span class="eyebrow">Affected</span>
              <span class="num">{affected}<small style="font-size:16px;color:var(--ink-faint)"> / {total}</small></span>
              <span class="sub">{hosts_sub}</span>
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
    scene = _meadow_scene_svg(w=900, h=110, seed=sum(ord(c) for c in "meadow"))
    st.markdown(
        f"""
        <section class="meadow-wrap">
          {scene}
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
    "bsod": "M13 3 L7 13 H12 L10 21 L17 11 H12 Z",
    "update_failure": "M5 19 Q5 8 12 8 Q19 8 19 14 Q19 18 15 18 Q12 18 12 15",
    "disk_full": "M5 11 H15 V18 Q15 19 14 19 H6 Q5 19 5 18 Z M15 13 L19 11 V15 M9 8 Q9 6 11 6 H13 Q15 6 15 8",
    "service_crash": "M12 21 V14 M12 14 L9 11 M12 14 L15 11 M12 14 V8 L9 5",
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


def _is_broadsheet() -> bool:
    return "Broadsheet" in st.session_state.get("_view", "")


# ─────────────────────────────────────────────────────────
# Broadsheet view — modern, no-metaphor dashboard
# (port of design/pre-emptive (3)/kevat-dashboard/broadsheet.html)
# ─────────────────────────────────────────────────────────

# Broadsheet uses different colour roles than the Meadow/Workshop palette;
# map an incident-type string to a CSS var defined in broadsheet.css.
_BS_TYPE_COLOR = {
    "bsod": "var(--bs-crit)",
    "update_failure": "var(--bs-warn)",
    "disk_full": "var(--bs-info)",
    "service_crash": "var(--bs-ink-faint)",
    "service_crash_loop": "var(--bs-ink-faint)",
    "network_instability": "var(--bs-accent)",
}
_BS_TYPE_LABEL = {
    "bsod": "BSOD",
    "update_failure": "Update failure",
    "disk_full": "Disk full",
    "service_crash": "Service crash",
    "service_crash_loop": "Service crash",
    "network_instability": "Network",
}


def _bs_color_for_type(t: str) -> str:
    return _BS_TYPE_COLOR.get(t, "var(--bs-ink-faint)")


def _bs_sev_meter_class(sev: int) -> str:
    if sev >= 90:
        return "crit"
    if sev >= 60:
        return "warn"
    return ""


def _bs_fmt_hhmm(ts: str) -> str:
    """Render an ISO timestamp as HH:MM (UTC). Fallback to last 5 chars."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%H:%M")
    except Exception:
        return (ts or "—")[-5:] if ts else "—"


def _bs_risk_sparkline(current: int, width: int = 200, height: int = 28) -> str:
    """Deterministic 14-day risk trend trailing into the current value."""
    import random

    rng = random.Random(int(current) * 7919 + 13)
    points = []
    # Ramp from ~current-40 up to current with jitter
    base = max(10, current - 35)
    n = 15
    for i in range(n):
        t = i / (n - 1)
        v = base + (current - base) * t + rng.uniform(-4, 4)
        v = max(0, min(120, v))
        x = (i / (n - 1)) * (width - 4) + 2
        # Invert y: higher value → lower y
        y = height - 2 - (v / 120) * (height - 6)
        points.append(f"{x:.1f},{y:.1f}")
    last_x = (width - 4) + 2 - 0  # last x ≈ width-2
    last_y = height - 2 - (current / 120) * (height - 6)
    poly = " ".join(points)
    color = (
        "var(--bs-crit)"
        if current >= 75
        else "var(--bs-warn)" if current >= 60 else "var(--bs-accent)"
    )
    return (
        f'<svg class="spark" viewBox="0 0 {width} {height}" preserveAspectRatio="none">'
        f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="1.4" />'
        f'<circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="2" fill="{color}" />'
        f"</svg>"
    )


def _bs_cluster_sparkline(sev: int, type_: str, seed: int) -> str:
    """Deterministic per-cluster 16-bar 'last 24h' sparkline."""
    import random

    rng = random.Random(seed * 11 + sev)
    color = _bs_color_for_type(type_)
    bars = []
    for i in range(15):
        h = rng.uniform(2, 18)
        # Push peak toward the right end for "new"/"hot" clusters
        if sev >= 75 and i >= 10:
            h = max(h, rng.uniform(10, 20))
        bars.append(f'<rect x="{i * 6}" y="{22 - h:.1f}" width="3" height="{h:.1f}"/>')
    return (
        f'<svg viewBox="0 0 90 22" preserveAspectRatio="none">'
        f'<g fill="{color}">{"".join(bars)}</g></svg>'
    )


def _bs_risk_chart(current: int) -> str:
    """Larger area + line chart showing a deterministic 14-day trend."""
    import random

    rng = random.Random(int(current) * 1013 + 41)
    w, h = 600, 180
    n = 15
    base = max(10, current - 35)
    pts: List[tuple] = []
    for i in range(n):
        t = i / (n - 1)
        v = base + (current - base) * t + rng.uniform(-5, 5)
        v = max(20, min(120, v))
        x = 40 + t * (w - 40)
        # y maps value→pixel: top (y=30) = 120, bottom (y=150) = 30
        y = 150 - ((v - 30) / 90) * 120
        pts.append((x, y))
    # Force last point to match current
    last_v = max(20, min(120, current))
    last_y = 150 - ((last_v - 30) / 90) * 120
    pts[-1] = (w, last_y)
    polyline = " ".join(f"{x:.0f},{y:.0f}" for x, y in pts)
    area = (
        f"M {pts[0][0]:.0f} {pts[0][1]:.0f} "
        + " ".join(f"L {x:.0f} {y:.0f}" for x, y in pts[1:])
        + f" L {w} 180 L {pts[0][0]:.0f} 180 Z"
    )
    # Threshold line at value 75 → y = 150 - ((75-30)/90)*120 = 150 - 60 = 90
    return f"""
    <svg viewBox="0 0 {w} {h}" preserveAspectRatio="none" style="height: 180px">
      <g stroke="var(--bs-divider)" stroke-width="1">
        <line x1="40" y1="30"  x2="{w}" y2="30" />
        <line x1="40" y1="70"  x2="{w}" y2="70" />
        <line x1="40" y1="110" x2="{w}" y2="110" />
        <line x1="40" y1="150" x2="{w}" y2="150" />
      </g>
      <g font-family="JetBrains Mono" font-size="9" fill="var(--bs-ink-faint)">
        <text x="32" y="33"  text-anchor="end">120</text>
        <text x="32" y="73"  text-anchor="end">90</text>
        <text x="32" y="113" text-anchor="end">60</text>
        <text x="32" y="153" text-anchor="end">30</text>
      </g>
      <line x1="40" y1="90" x2="{w}" y2="90"
            stroke="var(--bs-ink-faint)" stroke-width="1" stroke-dasharray="3 4" />
      <text x="{w}" y="86" text-anchor="end" font-family="JetBrains Mono"
            font-size="9" fill="var(--bs-ink-faint)">75 threshold</text>
      <defs>
        <linearGradient id="bs-area" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%"   stop-color="var(--bs-accent)" stop-opacity="0.18"/>
          <stop offset="100%" stop-color="var(--bs-accent)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <path d="{area}" fill="url(#bs-area)" />
      <polyline points="{polyline}" fill="none" stroke="var(--bs-accent)"
                stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      <circle cx="{w}" cy="{last_y:.0f}" r="3.5" fill="var(--bs-accent)" stroke="#fff" stroke-width="2" />
      <g font-family="JetBrains Mono" font-size="9" fill="var(--bs-ink-faint)">
        <text x="40"  y="172">−14d</text>
        <text x="200" y="172">−10d</text>
        <text x="360" y="172">−6d</text>
        <text x="520" y="172">−2d</text>
        <text x="{w}" y="172" text-anchor="end">today</text>
      </g>
    </svg>
    """


def render_broadsheet(fleet: Dict, run_id: str, store) -> None:
    # Cluster shape is normalized at the data-loading boundary by
    # ``_normalize_fleet`` — every view can rely on the canonical demo schema.
    clusters: List[Dict] = fleet.get("clusters", [])
    top_hosts: List[Dict] = fleet.get("top_hosts", [])
    risk = int(fleet.get("overall_risk_score", 0))
    n_clusters = len(clusters)
    new_count = sum(1 for c in clusters if c.get("status") == "new")
    # Affected hosts — prefer dedup across example_hosts, else the count sum.
    affected_set = {h for c in clusters for h in c.get("example_hosts", [])}
    affected = len(affected_set) or sum(c.get("affected_hosts", 0) for c in clusters)
    # Use the fleet's reported size; fall back to derived counts only.
    total_hosts = fleet.get(
        "hosts_total",
        fleet.get(
            "host_count",
            len(affected_set) or len(top_hosts) or 0,
        ),
    )

    # Window / finished time
    window = fleet.get("window", {})
    finished_ts = fleet.get("generated_at") or window.get("end", "")
    finished_hhmm = _bs_fmt_hhmm(finished_ts) if finished_ts else "—"

    # Status dot colour follows worst severity in fleet
    worst = max((c.get("severity", 0) for c in clusters), default=0)
    status_dot_cls = "crit" if worst >= 90 else ("ok" if worst < 60 else "")

    # Type counts for the filter chips and donut
    type_counts: Dict[str, int] = {}
    for c in clusters:
        t = c.get("type") or "unknown"
        type_counts[t] = type_counts.get(t, 0) + 1

    # ─── Filter chips ──────────────────────────────────────
    chip_html = [
        f'<span class="chip active">All types <span class="count">{n_clusters}</span></span>'
    ]
    for t, n in sorted(type_counts.items(), key=lambda x: -x[1]):
        chip_html.append(
            f'<span class="chip"><span class="dot" style="background: {_bs_color_for_type(t)}"></span>'
            f'{html.escape(_BS_TYPE_LABEL.get(t, t))} <span class="count">{n}</span></span>'
        )
    chip_html.append('<span style="flex: 1"></span>')
    chip_html.append('<span class="chip">Status: any</span>')
    chip_html.append('<span class="chip">Window: 24h</span>')

    # ─── KPI tiles ─────────────────────────────────────────
    pipeline_ok = True
    schema_errors_count = 0
    val_key = f"{run_id}/validation_summary.json"
    val_summary: Dict = {}
    if store.exists(val_key):
        try:
            val_summary = json.loads(store.read_text(val_key))
            schema_errors_count = len(val_summary.get("schema_errors", []))
            pipeline_ok = schema_errors_count == 0
        except Exception:
            pass

    pipeline_bars = "".join(
        f'<span style="flex:1; background: {"var(--bs-ok)" if pipeline_ok else "var(--bs-crit)"}; '
        f'border-radius: 2px; height: {6 + (i % 5) * 4}px"></span>'
        for i in range(min(15, total_hosts or 15))
    )

    kpis_html = f"""
    <div class="kpis">
      <div class="kpi">
        <span class="lbl">Overall risk</span>
        <div class="row">
          <span class="val">{risk}</span>
          <span class="delta {'up' if risk >= 75 else 'dn'}">{'↗' if risk >= 75 else '↘'} {abs(risk - 60)}</span>
        </div>
        {_bs_risk_sparkline(risk)}
        <span class="help">14-day trend · lower is better</span>
      </div>
      <div class="kpi">
        <span class="lbl">Active clusters</span>
        <div class="row">
          <span class="val">{n_clusters}</span>
          <span class="delta {'up' if new_count else 'dn'}">{'↗' if new_count else '·'} {new_count} new</span>
        </div>
        <svg class="spark" viewBox="0 0 200 28" preserveAspectRatio="none">
          <g fill="var(--bs-ink-2)">
            {"".join(
                f'<rect x="{i*14+2}" y="{28-(4+((i*7+risk)%18))}" width="10" height="{4+((i*7+risk)%18)}" />'
                for i in range(14)
            )}
            <rect x="184" y="{max(2, 28-min(25, n_clusters*4+8))}" width="10"
                  height="{min(25, n_clusters*4+8)}" fill="var(--bs-crit)" />
          </g>
        </svg>
        <span class="help">{new_count} new in this run</span>
      </div>
      <div class="kpi">
        <span class="lbl">Affected hosts</span>
        <div class="row">
          <span class="val">{affected}</span>
          <span class="unit">/ {total_hosts}</span>
          <span class="delta up">↗ {affected}</span>
        </div>
        <svg class="spark" viewBox="0 0 200 28" preserveAspectRatio="none">
          <polyline points="0,22 16,18 32,20 48,14 64,16 80,11 96,14 112,9 128,11 144,6 160,8 176,5 192,3"
                    fill="none" stroke="var(--bs-crit)" stroke-width="1.4" />
          <circle cx="192" cy="3" r="2" fill="var(--bs-crit)" />
        </svg>
        <span class="help">{int(100 * affected / max(1, total_hosts))}% of fleet has ≥1 incident</span>
      </div>
      <div class="kpi">
        <span class="lbl">Pipeline</span>
        <div class="row">
          <span class="val {'ok' if pipeline_ok else ''}" style="color: {'var(--bs-ok)' if pipeline_ok else 'var(--bs-crit)'}">
            {total_hosts - schema_errors_count}/{total_hosts}
          </span>
          <span class="delta dn">schema</span>
        </div>
        <div style="display:flex; gap:4px; height: 28px; align-items: flex-end; margin-top: 6px">
          {pipeline_bars}
        </div>
        <span class="help">{schema_errors_count} schema error{'s' if schema_errors_count != 1 else ''} · redaction balanced</span>
      </div>
    </div>
    """

    # ─── Cluster table rows ────────────────────────────────
    cluster_rows = []
    for i, c in enumerate(clusters):
        sev = int(c.get("severity", 0))
        meter_cls = _bs_sev_meter_class(sev)
        bar_pct = min(100, int((sev / 120) * 100))
        t = c.get("type") or "unknown"
        type_color = _bs_color_for_type(t)
        n_affected = c.get("affected_hosts", len(c.get("example_hosts", [])))
        sig_key = c.get("signature_key", "")
        sig_hash = c.get("signature_hash", "")
        # Split signature_key into name + sub
        if ":" in sig_key and "|" in sig_key:
            head, rest = sig_key.split("|", 1)
            cl_name = html.escape(rest[:60])
            source = html.escape(head)
        else:
            cl_name = html.escape(sig_key[:60] or sig_hash[:12])
            source = ""
        status = c.get("status", "ongoing")
        status_label = status
        cluster_rows.append(
            f"""
          <tr>
            <td>
              <div class="cl-name">{cl_name}</div>
              <div class="cl-sub">{html.escape(sig_hash[:12])}{(" · " + source) if source else ""}</div>
            </td>
            <td><span class="type-tag"><span class="dot" style="background: {type_color}"></span>{html.escape(t)}</span></td>
            <td class="num-col mono">{n_affected}</td>
            <td>
              <span class="status-pill {status}">
                <span style="width:6px;height:6px;border-radius:50%;background:{'var(--bs-accent)' if status == 'new' else 'var(--bs-warn)'}"></span>
                {html.escape(status_label)}
              </span>
            </td>
            <td class="num-col">
              <div class="sev">
                <span class="num">{sev}</span>
                <div class="meter {meter_cls}"><i style="width: {bar_pct}%"></i></div>
              </div>
            </td>
            <td class="num-col">{_bs_cluster_sparkline(sev, t, i)}</td>
          </tr>
        """
        )

    # ─── Top hosts rows ────────────────────────────────────
    host_rows = []
    for h in top_hosts[:8]:
        sev = int(h.get("score", 0))
        meter_cls = _bs_sev_meter_class(sev)
        bar_pct = min(100, int((sev / 120) * 100))
        # Pick the first incident type from reasons like "bsod (sev 90)"
        reasons = h.get("reasons", [])
        first = reasons[0] if reasons else ""
        t = first.split(" ")[0] if first else "unknown"
        type_color = _bs_color_for_type(t)
        last_ts = h.get("last_event_ts") or window.get("end", "")
        host_rows.append(
            f"""
          <tr>
            <td><span class="host-id">{html.escape(h.get("host_id", ""))}</span></td>
            <td><span class="type-tag"><span class="dot" style="background: {type_color}"></span>{html.escape(t)}</span></td>
            <td class="num-col">
              <div class="sev"><span class="num">{sev}</span>
                <div class="meter {meter_cls}"><i style="width: {bar_pct}%"></i></div>
              </div>
            </td>
            <td class="num-col mono" style="color: var(--bs-ink-soft)">{_bs_fmt_hhmm(last_ts)}</td>
          </tr>
        """
        )

    # ─── Donut chart (by type) ─────────────────────────────
    total_inc = sum(c.get("affected_hosts", 1) for c in clusters) or 1
    # Each type's share of total incidents
    type_share: Dict[str, int] = {}
    for c in clusters:
        t = c.get("type") or "unknown"
        type_share[t] = type_share.get(t, 0) + c.get("affected_hosts", 1)
    sorted_types = sorted(type_share.items(), key=lambda x: -x[1])
    circumference = 270.0  # 2π·43
    donut_segments = []
    legend_rows = []
    offset = 0.0
    for t, count in sorted_types:
        frac = count / total_inc
        dash = circumference * frac
        gap = circumference - dash
        color = _bs_color_for_type(t)
        donut_segments.append(
            f'<circle cx="50" cy="50" r="43" stroke="{color}" '
            f'stroke-dasharray="{dash:.1f} {gap:.1f}" stroke-dashoffset="{-offset:.1f}" />'
        )
        offset += dash
        legend_rows.append(
            f"""
          <div class="row">
            <span class="dot" style="background: {color}"></span>
            <span class="name">{html.escape(_BS_TYPE_LABEL.get(t, t))}</span>
            <span class="v">{count}</span>
            <span class="pct">{int(100 * frac)}%</span>
          </div>
        """
        )

    donut_html = f"""
    <div class="card">
      <div class="card-head">
        <div class="left">
          <h2>By type</h2>
          <span class="sub">{total_inc} incidents · {len(sorted_types)} types</span>
        </div>
      </div>
      <div class="donut-wrap">
        <svg viewBox="0 0 100 100" aria-hidden="true">
          <g fill="none" stroke-width="14" transform="rotate(-90 50 50)">
            {"".join(donut_segments)}
          </g>
          <text x="50" y="50" class="donut-center">{total_inc}</text>
          <text x="50" y="62" class="donut-center-lbl">incidents</text>
        </svg>
        <div class="dist-legend">{"".join(legend_rows)}</div>
      </div>
    </div>
    """

    # ─── Recommended actions (from top_hosts.action_reason / action) ──
    action_items = []
    seen_reasons: set = set()
    order = 0
    for h in top_hosts:
        action = h.get("action", "monitor")
        reason = h.get("action_reason", "")
        key = (action, reason)
        if key in seen_reasons:
            continue
        seen_reasons.add(key)
        order += 1
        if order > 5:
            break
        sev_score = h.get("score", 0)
        cls = (
            "crit"
            if sev_score >= 90
            else ("warn" if sev_score >= 75 else "info" if sev_score >= 60 else "")
        )
        host_id = h.get("host_id", "")
        action_items.append(
            f"""
          <div class="feed-item {cls}">
            <span class="dot"></span>
            <span class="ts">{order:02d}</span>
            <div>
              <div class="desc"><span class="em">{html.escape(action.capitalize())}</span>
                — {html.escape(host_id)}{' · ' + html.escape(reason) if reason else ''}</div>
            </div>
            <span class="right">·</span>
          </div>
        """
        )
    if not action_items:
        action_items.append(
            '<div class="feed-item"><span class="dot"></span><span class="ts">—</span>'
            '<div><div class="desc">No recommended actions for this run.</div></div>'
            '<span class="right">·</span></div>'
        )

    actions_html = f"""
    <div class="card">
      <div class="card-head">
        <div class="left">
          <h2>Recommended actions</h2>
          <span class="sub">{len(action_items)} items · ordered by impact</span>
        </div>
        <div class="right"><span class="ghost">Open in runbook</span></div>
      </div>
      <div class="card-body" style="padding: 0">{"".join(action_items)}</div>
    </div>
    """

    # ─── Activity feed (from clusters' last_seen) ──────────
    feed_items = []
    sorted_clusters = sorted(
        clusters, key=lambda c: c.get("last_seen", ""), reverse=True
    )
    for c in sorted_clusters[:6]:
        sev = c.get("severity", 0)
        cls = (
            "crit"
            if sev >= 90
            else ("warn" if sev >= 75 else "info" if sev >= 60 else "")
        )
        ts = _bs_fmt_hhmm(c.get("last_seen", ""))
        hosts = c.get("example_hosts", [])
        host_str = ", ".join(hosts[:2])
        sig_key = c.get("signature_key", "")
        # Take the right side of the | for the human-readable description
        desc = sig_key.split("|", 1)[1] if "|" in sig_key else sig_key
        delta = f"+{sev - 60}" if sev > 60 else "+0"
        feed_items.append(
            f"""
          <div class="feed-item {cls}">
            <span class="dot"></span>
            <span class="ts">{ts}</span>
            <div class="desc">{html.escape(desc[:80])} on <span class="em">{html.escape(host_str)}</span>.</div>
            <span class="right">{delta} sev</span>
          </div>
        """
        )

    feed_html = f"""
    <div class="card">
      <div class="card-head">
        <div class="left">
          <h2>Recent activity</h2>
          <span class="sub">Last {len(feed_items)} events · {html.escape(run_id)}</span>
        </div>
        <div class="right"><span class="ghost">View all</span></div>
      </div>
      <div class="feed">{"".join(feed_items)}</div>
    </div>
    """

    # ─── Validation strip ──────────────────────────────────
    precision = val_summary.get("incident_type_precision")
    recall = val_summary.get("incident_type_recall")
    ranking = val_summary.get("ranking_score")
    cluster_ok = val_summary.get("cluster_detected", False)
    schema_pass_cls = "ok" if pipeline_ok else "crit"

    def _vfmt(v) -> str:
        return f"{v:.0%}" if isinstance(v, (int, float)) else "—"

    def _vcls(v, threshold: float) -> str:
        if isinstance(v, (int, float)):
            return "ok" if v >= threshold else "warn"
        return ""

    val_strip_html = f"""
    <div class="val-strip">
      <div class="val-cell">
        <div class="lbl">Schema pass</div>
        <div class="v {schema_pass_cls}">{total_hosts - schema_errors_count} / {total_hosts}</div>
        <div class="sub">Snapshots, incidents, tickets</div>
      </div>
      <div class="val-cell">
        <div class="lbl">Cluster recall</div>
        <div class="v {'ok' if cluster_ok else 'warn'}">{'pass' if cluster_ok else 'fail'}</div>
        <div class="sub">Ground-truth outages detected</div>
      </div>
      <div class="val-cell">
        <div class="lbl">Type precision</div>
        <div class="v {_vcls(precision, 0.8)}">{_vfmt(precision)}</div>
        <div class="sub">Incident-type match</div>
      </div>
      <div class="val-cell">
        <div class="lbl">Ranking</div>
        <div class="v {_vcls(ranking, 0.6)}">{_vfmt(ranking)}</div>
        <div class="sub">Top-host hit rate</div>
      </div>
      <div class="val-cell">
        <div class="lbl">Run finished</div>
        <div class="v">{finished_hhmm}</div>
        <div class="sub">{html.escape(run_id)} · ok</div>
      </div>
    </div>
    """

    # ─── Assemble ──────────────────────────────────────────
    _bs_html = f"""
        <div class="broadsheet">
          <div class="page-head">
            <div class="title">
              <h1>
                <span class="status-dot {status_dot_cls}"></span>
                Fleet · last 24h
              </h1>
              <div class="meta">
                <span>{total_hosts} hosts surveyed</span>
                <span class="sep">·</span>
                <span>{n_clusters} clusters · {affected} affected</span>
                <span class="sep">·</span>
                <span class="mono">{html.escape(run_id)} · finished {finished_hhmm}</span>
              </div>
            </div>
          </div>

          <div class="filter-row">{"".join(chip_html)}</div>

          {kpis_html}

          <div class="cols">
            <div class="stack">
              <div class="card">
                <div class="card-head">
                  <div class="left">
                    <h2>Risk over time</h2>
                    <span class="sub">Last 14 days · daily aggregate</span>
                  </div>
                  <div class="right">
                    <span class="ghost">14d</span>
                    <span class="ghost" style="color: var(--bs-ink)">30d</span>
                    <span class="ghost">90d</span>
                  </div>
                </div>
                <div class="chart-wrap">
                  <div class="chart-legend">
                    <span><i style="background: var(--bs-accent)"></i> Risk score</span>
                    <span><i style="background: var(--bs-ink-faint)"></i> Threshold (75)</span>
                  </div>
                  {_bs_risk_chart(risk)}
                </div>
              </div>

              <div class="card">
                <div class="card-head">
                  <div class="left">
                    <h2>Active clusters</h2>
                    <span class="sub">{n_clusters} detected · {new_count} new today</span>
                  </div>
                  <div class="right">
                    <span class="ghost">Sort: severity</span>
                  </div>
                </div>
                <div class="card-body tight">
                  <table class="tbl">
                    <thead>
                      <tr>
                        <th>Cluster</th><th>Type</th><th class="num-col">Hosts</th>
                        <th>Status</th><th class="num-col">Severity</th><th class="num-col">Last 24h</th>
                      </tr>
                    </thead>
                    <tbody>{"".join(cluster_rows)}</tbody>
                  </table>
                </div>
              </div>

              <div class="card">
                <div class="card-head">
                  <div class="left">
                    <h2>Top hosts</h2>
                    <span class="sub">Sorted by severity · top {len(host_rows)} of {len(top_hosts)}</span>
                  </div>
                </div>
                <div class="card-body tight">
                  <table class="tbl">
                    <thead>
                      <tr>
                        <th>Host</th><th>Incident</th>
                        <th class="num-col">Sev</th><th class="num-col">Last event</th>
                      </tr>
                    </thead>
                    <tbody>{"".join(host_rows)}</tbody>
                  </table>
                </div>
              </div>
            </div>

            <div class="stack">
              {donut_html}
              {actions_html}
              {feed_html}
            </div>
          </div>

          {val_strip_html}
        </div>
        """
    # Strip leading whitespace from every line so Streamlit's Markdown parser
    # does not treat indented HTML blocks as fenced code (which causes the
    # raw <div ...> to spill into the page).
    _bs_html = "\n".join(line.lstrip() for line in _bs_html.splitlines())
    st.markdown(_bs_html, unsafe_allow_html=True)


def render_cluster_ledger(clusters: List[Dict]) -> None:
    workshop = _is_workshop()
    eyebrow = "Issue log" if workshop else "Cluster ledger"
    heading = "What&#39;s running hot" if workshop else "What&#39;s growing"
    col_item = "Issue" if workshop else "Cluster"
    col_hosts = "Machines" if workshop else "Hosts"
    col_sev = "Heat" if workshop else "Severity"
    col_status = "State" if workshop else "Status"

    selected_hash = st.session_state.get("selected_cluster")

    st.markdown(
        f"""
        <section class="card" style="margin-top: 24px;">
          <div class="card-head">
            <div>
              <p class="eyebrow">{eyebrow}</p>
              <h2 style="font-family: var(--font-display); font-style: italic; font-weight: 500;">{heading}</h2>
            </div>
          </div>
          <div class="cluster-list">
            <div class="cluster-row head" style="display:grid;grid-template-columns:28px 1fr 80px 120px 110px;">
              <span></span><span>{col_item}</span><span>{col_hosts}</span>
              <span>{col_sev}</span><span>{col_status}</span>
            </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="kv-cluster-nav">', unsafe_allow_html=True)

    for c in clusters:
        sig_hash = c.get("signature_hash", "")
        sig_key = c.get("signature_key") or sig_hash[:12]
        sev_class = _sev_class(c.get("severity", 0))
        bar_pct = min(100, (c.get("severity", 0) / 120) * 100)
        accent = {
            "cool": "var(--leaf-300)",
            "warm": "var(--pollen-400)",
            "hot": "var(--sev-3)",
            "critical": "var(--sev-4)",
        }[sev_class]
        n_hosts = c.get("affected_hosts", 0)
        example = c.get("example_hosts", [])
        hosts_label = ", ".join(example[:3]) + ("…" if len(example) > 3 else "")
        status = c.get("status", "ongoing")
        status_label = (
            ("New issue" if workshop else "Sprouting today")
            if status == "new"
            else ("Running" if workshop else "Ongoing")
        )
        status_dot_color = "var(--leaf-400)" if status == "new" else "var(--sun)"
        inc_type = c.get("type") or "bsod"
        is_selected = sig_hash == selected_hash

        col_i, col_t, col_h, col_b, col_s = st.columns([0.4, 4, 1.2, 2, 2])
        with col_i:
            st.markdown(
                f'<div style="padding-top:10px;color:{accent}">{_type_icon(inc_type, accent)}</div>',
                unsafe_allow_html=True,
            )
        with col_t:
            label = ("◉  " if is_selected else "") + sig_key[:48]
            if st.button(label, key=f"cl_{sig_hash}", use_container_width=True):
                if is_selected:
                    st.session_state.pop("selected_cluster", None)
                else:
                    st.session_state["selected_cluster"] = sig_hash
                st.rerun()
            st.markdown(
                f'<div style="font-size:11px;color:var(--ink-faint);margin-top:-6px;padding-left:1px">'
                f"{html.escape(sig_hash[:12])}</div>",
                unsafe_allow_html=True,
            )
        with col_h:
            st.markdown(
                f'<div style="padding-top:8px;font-size:18px;font-weight:500;color:var(--ink);line-height:1.2">'
                f"{n_hosts}<br>"
                f'<span style="font-size:11px;color:var(--ink-faint)">{html.escape(hosts_label)}</span></div>',
                unsafe_allow_html=True,
            )
        with col_b:
            st.markdown(
                f'<div style="padding-top:16px">'
                f'<div class="severity-bar {sev_class}"><i style="width:{bar_pct:.0f}%"></i></div></div>',
                unsafe_allow_html=True,
            )
        with col_s:
            st.markdown(
                f'<div style="padding-top:10px">'
                f'<span class="status-pill {status}">'
                f'<span style="width:6px;height:6px;border-radius:50%;background:{status_dot_color};'
                f'display:inline-block;margin-right:4px;vertical-align:middle;"></span>'
                f"{status_label}</span></div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            '<div style="border-top:1px solid var(--leaf-50,#eef2e8);margin:0 0 2px 0"></div>',
            unsafe_allow_html=True,
        )

    st.markdown("</div></div></section>", unsafe_allow_html=True)


def render_hosts_grid(hosts: List[Dict], selected_cluster: Dict | None = None) -> None:
    # Filter to cluster's example_hosts when a cluster is selected
    filter_ids: set | None = None
    filter_label = ""
    if selected_cluster:
        example = selected_cluster.get("example_hosts", [])
        if example:
            filter_ids = set(example)
            sig_key = (
                selected_cluster.get("signature_key")
                or selected_cluster.get("signature_hash", "")[:12]
            )
            filter_label = html.escape(sig_key[:48])

    if filter_ids is not None:
        display_hosts = [h for h in hosts if h.get("host_id") in filter_ids]
    else:
        display_hosts = hosts

    cards = []
    for h in display_hosts[:12]:
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
            f"  </div>"
            f'  <div class="types">{tags}</div>'
            f"</div>"
        )

    eyebrow = "Top impacted hosts"
    if filter_label:
        eyebrow = f'Hosts · <span style="color:var(--leaf-500)">{filter_label}</span>'
    heading = (
        "Hosts needing attention"
        if not filter_ids
        else f"{len(display_hosts)} host{'s' if len(display_hosts) != 1 else ''} in this cluster"
    )

    if filter_ids is not None:
        col_head, col_clear = st.columns([6, 1])
        with col_clear:
            if st.button("Show all", key="clear_cluster_filter"):
                st.session_state.pop("selected_cluster", None)
                st.rerun()

    st.markdown(
        f"""
        <section class="card" style="margin-top: 24px;">
          <div class="card-head">
            <div>
              <p class="eyebrow">{eyebrow}</p>
              <h2 style="font-family: var(--font-display); font-style: italic; font-weight: 500;">{heading}</h2>
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
    if _is_broadsheet():
        _render_host_broadsheet(timeline, host_id, fleet, run_id)
    elif _is_workshop():
        _render_host_workshop(timeline, host_id, fleet)
    else:
        _render_host_meadow(timeline, host_id, fleet)


def _render_host_meadow(timeline: Dict, host_id: str, fleet: Dict) -> None:
    window = timeline.get("window") or {
        "start": timeline.get("window_start"),
        "end": timeline.get("window_end"),
    }
    host_meta = next(
        (h for h in fleet.get("top_hosts", []) if h.get("host_id") == host_id), {}
    )
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
                f"</div>"
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
                f"  </div>"
                f"  {cluster_line}"
                f'  <div class="body">Confidence: {inc.get("confidence","")}</div>'
                f'  <div class="actions-list">{actions_html}</div>'
                f'  <div class="evidence">{ev_rows}</div>'
                f"</div>"
            )
        st.markdown(
            f'<div class="incidents">{"".join(inc_cards)}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No incidents detected for this host.")

    events = timeline.get("events", [])
    win = timeline.get("window") or {}
    win_start_str = win.get("start") or timeline.get("window_start") or ""
    try:
        win_start_dt = datetime.fromisoformat(win_start_str.replace("Z", "+00:00"))
        if win_start_dt.tzinfo is None:
            win_start_dt = win_start_dt.replace(tzinfo=timezone.utc)
    except Exception:
        win_start_dt = datetime.now(timezone.utc) - timedelta(hours=24)

    if events:
        # Branch-and-bud SVG timeline
        strip_w = 800
        branch_svg_h = 180
        bud_rows: List[str] = []
        for ev in events[:40]:
            ts = ev.get("ts", "")
            level = ev.get("level", "")
            msg = html.escape(ev.get("message", "")[:50])
            hour = _parse_event_hour(ts, win_start_dt)
            x = (hour / 24.0) * strip_w
            is_err = level in ("Error", "Critical", "Warning")
            y = 55 if is_err else 130
            if is_err:
                petals = "".join(
                    f'<ellipse cx="0" cy="-9" rx="5.5" ry="8" transform="rotate({d})" '
                    f'fill="var(--bloom)" stroke="var(--bloom-edge)" stroke-width="0.8" />'
                    for d in [0, 72, 144, 216, 288]
                )
                bud_rows.append(
                    f'<g transform="translate({x:.1f} {y})">'
                    f"{petals}"
                    f'<circle r="4" fill="var(--sev-3)" />'
                    f'<circle r="1.4" fill="var(--bloom)" />'
                    f"<title>{msg}</title>"
                    f"</g>"
                )
            else:
                bud_rows.append(
                    f'<circle cx="{x:.1f}" cy="{y}" r="5" fill="var(--leaf-400)" opacity="0.8">'
                    f"<title>{msg}</title></circle>"
                    f'<circle cx="{x:.1f}" cy="{y}" r="9" fill="var(--leaf-300)" opacity="0.15" />'
                )

        leaf_positions = [0.06, 0.13, 0.22, 0.34, 0.46, 0.58, 0.68, 0.79, 0.88, 0.95]
        leaf_colors = [
            "var(--leaf-300)",
            "var(--leaf-400)",
            "var(--leaf-200)",
            "var(--leaf-300)",
            "var(--leaf-400)",
            "var(--leaf-300)",
            "var(--leaf-200)",
            "var(--leaf-400)",
            "var(--leaf-300)",
            "var(--leaf-400)",
        ]
        leaf_sides = [-1, 1, -1, 1, -1, 1, -1, 1, -1, 1]
        leaf_sizes = [14, 16, 13, 15, 17, 14, 15, 16, 13, 15]
        branch_leaves = ""
        for lp, lc, ls, lsz in zip(leaf_positions, leaf_colors, leaf_sides, leaf_sizes):
            lx = lp * strip_w
            ly = 105 + math.sin(lp * 6) * 2
            ly_off = ls * 14
            lang = ls * (38 + leaf_positions.index(lp) % 3 * 6)
            branch_leaves += (
                f"<g>"
                f'<line x1="{lx:.1f}" y1="{ly:.1f}" x2="{lx + ls*4:.1f}" y2="{ly + ly_off:.1f}" '
                f'stroke="var(--earth-600)" stroke-width="0.8" opacity="0.5" />'
                f'<g transform="translate({lx + ls*4:.1f} {ly + ly_off:.1f}) rotate({lang})">'
                f'<path d="M 0 0 Q {ls*lsz*0.55:.1f} {-lsz*0.55:.1f} {ls*lsz:.1f} 0 '
                f'Q {ls*lsz*0.55:.1f} {lsz*0.55:.1f} 0 0 Z" fill="{lc}" />'
                f'<line x1="0" y1="0" x2="{ls*lsz:.1f}" y2="0" stroke="rgba(255,255,255,0.25)" stroke-width="0.6" />'
                f"</g></g>"
            )

        hours_row = "".join(
            f"<span>{str(hr).zfill(2)}:00</span>" for hr in [0, 4, 8, 12, 16, 20, 24]
        )
        branch_svg = (
            f'<svg viewBox="0 0 {strip_w} {branch_svg_h}" preserveAspectRatio="none" '
            f'width="100%" height="{branch_svg_h}" style="position:absolute;inset:0;top:10px" aria-hidden="true">'
            f"<defs>"
            f'<linearGradient id="branch-grad-{host_id.replace("-","_")}" x1="0" x2="1" y1="0" y2="0">'
            f'<stop offset="0%" stop-color="var(--earth-200)" />'
            f'<stop offset="50%" stop-color="var(--earth-400)" />'
            f'<stop offset="100%" stop-color="var(--earth-200)" /></linearGradient>'
            f"</defs>"
            f'<path d="M 0 110 Q {strip_w*0.25:.0f} 90, {strip_w*0.5:.0f} 108 T {strip_w} 100" '
            f'fill="none" stroke="url(#branch-grad-{host_id.replace("-","_")})" '
            f'stroke-width="4" stroke-linecap="round" />'
            f"{branch_leaves}"
            f'{"".join(bud_rows)}'
            f"</svg>"
        )
        st.markdown(
            f'<p class="eyebrow" style="margin-top:24px;color:var(--ink-faint)">24h event timeline · {len(events)} events</p>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="branch-wrap" style="position:relative;height:{branch_svg_h + 20}px;overflow:hidden;border-radius:10px;background:var(--linen);">'
            f"{branch_svg}"
            f'<div class="hours" style="position:absolute;bottom:6px;left:0;right:0;display:flex;justify-content:space-between;padding:0 8px;font-size:10px;color:var(--ink-faint);font-family:var(--font-mono);">'
            f"{hours_row}</div></div>",
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────
# Host view — Workshop variant (CRT-style event strip)
# ─────────────────────────────────────────────────────────


def _render_host_workshop(timeline: Dict, host_id: str, fleet: Dict) -> None:
    window = timeline.get("window") or {
        "start": timeline.get("window_start"),
        "end": timeline.get("window_end"),
    }
    host_meta = next(
        (h for h in fleet.get("top_hosts", []) if h.get("host_id") == host_id), {}
    )
    score = host_meta.get("score", 0)
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
                f"</div>"
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
                f"  </div>"
                f"  {cluster_line}"
                f'  <div class="body">Confidence: {inc.get("confidence","")}</div>'
                f'  <div class="actions-list">{actions_html}</div>'
                f'  <div class="evidence">{ev_rows}</div>'
                f"</div>"
            )
        st.markdown(
            f'<div class="incidents">{"".join(inc_cards)}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No incidents detected for this host.")

    events = timeline.get("events", [])
    win = timeline.get("window") or {}
    win_start_str = win.get("start") or timeline.get("window_start") or ""
    try:
        win_start_dt = datetime.fromisoformat(win_start_str.replace("Z", "+00:00"))
        if win_start_dt.tzinfo is None:
            win_start_dt = win_start_dt.replace(tzinfo=timezone.utc)
    except Exception:
        win_start_dt = datetime.now(timezone.utc) - timedelta(hours=24)

    if not events:
        return

    # CRT-style 24h strip: dark phosphor monitor with vertical ticks per event.
    strip_w = 800
    strip_h = 110
    grid = "".join(
        f'<line x1="{(h / 24.0) * strip_w:.1f}" y1="14" x2="{(h / 24.0) * strip_w:.1f}" y2="{strip_h - 18}" '
        f'stroke="rgba(140, 220, 170, 0.10)" stroke-width="1" />'
        for h in range(25)
    )
    baseline_y = strip_h / 2
    ticks: List[str] = []
    for ev in events[:80]:
        ts = ev.get("ts", "")
        level = ev.get("level", "")
        msg = html.escape(ev.get("message", "")[:80])
        hour = _parse_event_hour(ts, win_start_dt)
        x = (hour / 24.0) * strip_w
        if level in ("Error", "Critical"):
            color = "var(--sev-4, #d2575d)"
            tick_h = 22
            glow = 6
        elif level == "Warning":
            color = "var(--sev-3, #d9a23a)"
            tick_h = 16
            glow = 4
        else:
            color = "#7be0a8"
            tick_h = 8
            glow = 2
        ticks.append(
            f'<g><line x1="{x:.1f}" y1="{baseline_y - tick_h:.1f}" x2="{x:.1f}" y2="{baseline_y + tick_h:.1f}" '
            f'stroke="{color}" stroke-width="2" stroke-linecap="round" '
            f'style="filter: drop-shadow(0 0 {glow}px {color})"><title>{msg}</title></line></g>'
        )
    hour_labels = "".join(
        f"<span>{str(hr).zfill(2)}:00</span>" for hr in [0, 4, 8, 12, 16, 20, 24]
    )
    crt_svg = (
        f'<svg viewBox="0 0 {strip_w} {strip_h}" preserveAspectRatio="none" '
        f'width="100%" height="{strip_h}">'
        f"{grid}"
        f'<line x1="0" y1="{baseline_y}" x2="{strip_w}" y2="{baseline_y}" '
        f'stroke="#7be0a8" stroke-width="1.2" opacity="0.6" '
        f'style="filter: drop-shadow(0 0 3px #7be0a8)" />'
        f'{"".join(ticks)}'
        f"</svg>"
    )
    st.markdown(
        f'<p class="eyebrow" style="margin-top:24px;color:var(--ink-faint)">'
        f"24h event timeline · {len(events)} events</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="position:relative;height:{strip_h + 22}px;border-radius:10px;'
        f"background:#0c1410;border:1px solid #142018;padding:0;overflow:hidden;"
        f'background-image:repeating-linear-gradient(0deg, rgba(123,224,168,0.04) 0 1px, transparent 1px 3px);">'
        f"{crt_svg}"
        f'<div style="position:absolute;bottom:4px;left:0;right:0;display:flex;'
        f"justify-content:space-between;padding:0 8px;font-size:10px;color:#7be0a8;"
        f'font-family:var(--font-mono);opacity:0.7;letter-spacing:0.04em;">'
        f"{hour_labels}</div></div>",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────
# Host view — Broadsheet variant (editorial, no metaphor)
# ─────────────────────────────────────────────────────────


def _render_host_broadsheet(
    timeline: Dict, host_id: str, fleet: Dict, run_id: str
) -> None:
    window = timeline.get("window") or {
        "start": timeline.get("window_start"),
        "end": timeline.get("window_end"),
    }
    host_meta = next(
        (h for h in fleet.get("top_hosts", []) if h.get("host_id") == host_id), {}
    )
    score = int(host_meta.get("score", 0) or 0)
    action = host_meta.get("action", "—")
    reason = host_meta.get("action_reason", "")
    user_id = host_meta.get("user_id", "")

    incidents = timeline.get("incidents", [])
    cluster_index = {c.get("signature_hash"): c for c in fleet.get("clusters", [])}
    events = timeline.get("events", [])

    # Window start datetime for event positioning.
    win_start_str = window.get("start") or timeline.get("window_start") or ""
    try:
        win_start_dt = datetime.fromisoformat(win_start_str.replace("Z", "+00:00"))
        if win_start_dt.tzinfo is None:
            win_start_dt = win_start_dt.replace(tzinfo=timezone.utc)
    except Exception:
        win_start_dt = datetime.now(timezone.utc) - timedelta(hours=24)

    last_event_ts = ""
    if events:
        last_event_ts = events[-1].get("ts", "") or ""

    # Status dot reflects host score severity.
    if score >= 90:
        status_dot_cls = "crit"
    elif score >= 60:
        status_dot_cls = ""
    else:
        status_dot_cls = "ok"

    # ─── Score sparkline (deterministic, lands on actual score) ───────
    score_spark = _bs_risk_sparkline(score)

    def _sev_meter(sev: int) -> str:
        cls = _bs_sev_meter_class(sev)
        pct = min(100, int((sev / 120) * 100))
        return (
            f'<div class="sev"><span class="num">{sev}</span>'
            f'<div class="meter {cls}"><i style="width: {pct}%"></i></div></div>'
        )

    # ─── KPI tiles ───────────────────────────────────────────────────
    kpis_html = f"""
    <div class="kpis">
      <div class="kpi">
        <span class="lbl">Host score</span>
        <div class="row">
          <span class="val">{score}</span>
          <span class="delta {'up' if score >= 75 else 'dn'}">
            {'↗' if score >= 75 else '↘'} {abs(score - 60)}
          </span>
        </div>
        {score_spark}
        <span class="help">0 = idle · 120 = critical</span>
      </div>
      <div class="kpi">
        <span class="lbl">Action</span>
        <div class="row">
          <span class="val" style="font-size: 22px; letter-spacing: -0.01em">
            {html.escape(action.capitalize())}
          </span>
        </div>
        <span class="help">{html.escape(reason or '—')}</span>
      </div>
      <div class="kpi">
        <span class="lbl">Incidents · events</span>
        <div class="row">
          <span class="val">{len(incidents)}</span>
          <span class="unit">/ {len(events)} ev</span>
        </div>
        <span class="help">last event {_bs_fmt_hhmm(last_event_ts)}</span>
      </div>
    </div>
    """

    # ─── Incidents ───────────────────────────────────────────────────
    inc_cards: List[str] = []
    for inc in incidents:
        sev = int(inc.get("severity", 0) or 0)
        sig_hash = inc.get("signature", {}).get("signature_hash", "")
        cluster = cluster_index.get(sig_hash, {})
        type_str = inc.get("type", "") or "unknown"
        type_color = _bs_color_for_type(type_str)

        cluster_meta = ""
        if cluster:
            cluster_meta = (
                f'<span class="sub">'
                f'cluster {html.escape(cluster.get("signature_key", "")[:50])} · '
                f'{html.escape(cluster.get("status", ""))}</span>'
            )

        actions_rows = "".join(
            f'<div class="feed-item">'
            f'<span class="dot"></span>'
            f'<span class="ts">{idx + 1:02d}</span>'
            f'<div class="desc">{html.escape(a)}</div>'
            f'<span class="right">·</span>'
            f"</div>"
            for idx, a in enumerate(inc.get("recommended_actions", []))
        )

        ev_rows = "".join(
            f"""
            <tr>
              <td class="mono num-col" style="white-space:nowrap; color: var(--bs-ink-soft)">
                {html.escape(e.get("ts", "")[:19])}
              </td>
              <td><span class="type-tag">{html.escape(e.get("provider", ""))}</span>
                <span class="mono" style="color: var(--bs-ink-faint)">·
                {html.escape(str(e.get("event_id", "")))}</span></td>
              <td>{html.escape(e.get("message", "")[:120])}</td>
            </tr>
            """
            for e in inc.get("evidence", [])[:5]
        )

        inc_cards.append(
            f"""
            <div class="card" style="margin-bottom: 12px">
              <div class="card-head">
                <div class="left">
                  <h2>{html.escape(inc.get("title", "") or type_str)}</h2>
                  <span class="sub">
                    <span class="type-tag">
                      <span class="dot" style="background: {type_color}"></span>
                      {html.escape(type_str)}
                    </span>
                    · confidence {html.escape(str(inc.get("confidence", "—")))}
                  </span>
                  {cluster_meta}
                </div>
                <div class="right">{_sev_meter(sev)}</div>
              </div>
              <div class="card-body" style="padding: 0">
                {actions_rows or
                 '<div class="feed-item"><span class="dot"></span>'
                 '<span class="ts">—</span><div class="desc">'
                 'No recommended actions.</div><span class="right">·</span></div>'}
                {f'<table class="tbl"><thead><tr>'
                 f'<th class="num-col">When</th><th>Source</th><th>Message</th>'
                 f'</tr></thead><tbody>{ev_rows}</tbody></table>' if ev_rows else ''}
              </div>
            </div>
            """
        )

    incidents_html = (
        f"""
        <div class="card-head" style="border-bottom: none; padding-bottom: 0">
          <div class="left">
            <h2>Incidents</h2>
            <span class="sub">{len(incidents)} detected this window</span>
          </div>
        </div>
        {''.join(inc_cards)}
        """
        if incidents
        else (
            '<div class="card"><div class="card-body">'
            '<span class="sub">No incidents detected for this host.</span>'
            "</div></div>"
        )
    )

    # ─── 24h timeline strip ─────────────────────────────────────────
    strip_w = 800
    strip_h = 110
    pad_l = 32
    err_y = 30
    info_y = strip_h - 36

    grid_lines = "".join(
        f'<line x1="{pad_l + (h / 24.0) * (strip_w - pad_l):.1f}" y1="14" '
        f'x2="{pad_l + (h / 24.0) * (strip_w - pad_l):.1f}" y2="{strip_h - 18}" '
        f'stroke="var(--bs-divider)" stroke-width="1" />'
        for h in range(25)
    )
    dots: List[str] = []
    for ev in events[:80]:
        ts = ev.get("ts", "")
        level = ev.get("level", "")
        msg = html.escape(ev.get("message", "")[:120])
        hour = _parse_event_hour(ts, win_start_dt)
        x = pad_l + (hour / 24.0) * (strip_w - pad_l)
        if level in ("Error", "Critical"):
            color = "var(--bs-crit)"
            r = 4
            y = err_y
        elif level == "Warning":
            color = "var(--bs-warn)"
            r = 3.5
            y = err_y + 18
        else:
            color = "var(--bs-info)"
            r = 2.6
            y = info_y
        dots.append(
            f'<circle cx="{x:.1f}" cy="{y}" r="{r}" fill="{color}">'
            f"<title>{msg}</title></circle>"
        )

    hour_labels = "".join(
        f'<text x="{pad_l + (hr / 24.0) * (strip_w - pad_l):.1f}" y="{strip_h - 4}" '
        f'font-family="JetBrains Mono" font-size="9" fill="var(--bs-ink-faint)" '
        f'text-anchor="middle">{str(hr).zfill(2)}:00</text>'
        for hr in [0, 4, 8, 12, 16, 20, 24]
    )

    strip_svg = f"""
    <svg viewBox="0 0 {strip_w} {strip_h}" preserveAspectRatio="none"
         style="width: 100%; height: {strip_h}px; display: block">
      {grid_lines}
      <g font-family="JetBrains Mono" font-size="9" fill="var(--bs-ink-faint)">
        <text x="0" y="{err_y + 3}">errors</text>
        <text x="0" y="{info_y + 3}">info</text>
      </g>
      {''.join(dots)}
      {hour_labels}
    </svg>
    """

    timeline_card = f"""
    <div class="card">
      <div class="card-head">
        <div class="left">
          <h2>24-hour event timeline</h2>
          <span class="sub">{len(events)} events · window
            {window.get('start','')[:10]} → {window.get('end','')[:10]}</span>
        </div>
        <div class="right">
          <span class="ghost"><span style="display:inline-block;width:8px;height:8px;
            background:var(--bs-crit);border-radius:50%;margin-right:4px"></span>error</span>
          <span class="ghost"><span style="display:inline-block;width:8px;height:8px;
            background:var(--bs-warn);border-radius:50%;margin-right:4px"></span>warning</span>
          <span class="ghost"><span style="display:inline-block;width:8px;height:8px;
            background:var(--bs-info);border-radius:50%;margin-right:4px"></span>info</span>
        </div>
      </div>
      <div class="chart-wrap">{strip_svg}</div>
    </div>
    """

    user_meta = (
        f'<span class="sep">·</span><span class="mono">{html.escape(user_id)}</span>'
        if user_id
        else ""
    )
    page = f"""
    <div class="broadsheet">
      <div class="page-head">
        <div class="title">
          <h1>
            <span class="status-dot {status_dot_cls}"></span>
            {html.escape(host_id)}
          </h1>
          <div class="meta">
            <span>{window.get('start', '')[:10]} → {window.get('end', '')[:10]}</span>
            <span class="sep">·</span>
            <span class="mono">run {html.escape(run_id)}</span>
            {user_meta}
          </div>
        </div>
      </div>
      {kpis_html}
      {incidents_html}
      {timeline_card}
    </div>
    """
    page = "\n".join(line.lstrip() for line in page.splitlines())
    st.markdown(page, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# Validation tab
# ─────────────────────────────────────────────────────────


def render_validation(store, run_id: str) -> None:
    summary: Dict = {}
    summary_key = f"{run_id}/validation_summary.json"
    if store.exists(summary_key):
        try:
            summary = json.loads(store.read_text(summary_key))
        except Exception:
            pass

    if summary:
        precision = summary.get("incident_type_precision")
        recall = summary.get("incident_type_recall")
        ranking = summary.get("ranking_score")
        schema_errors = summary.get("schema_errors", [])
        cluster_ok = summary.get("cluster_detected", False)
        warnings = summary.get("scenario_warnings", [])

        def _card(label: str, value: str, sub: str, ok: bool | None) -> str:
            if ok is True:
                badge_color = "var(--leaf-400)"
                badge = "✓ pass"
            elif ok is False:
                badge_color = "var(--sev-3)"
                badge = "✗ fail"
            else:
                badge_color = "var(--ink-faint)"
                badge = "—"
            return (
                f'<div style="flex:1;min-width:140px;background:var(--paper);border:1px solid var(--leaf-50,#eef2e8);'
                f'border-radius:12px;padding:18px 20px">'
                f'<div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:var(--ink-faint);margin-bottom:6px">{label}</div>'
                f'<div style="font-size:32px;font-family:var(--font-display);font-weight:500;color:var(--ink);line-height:1">{value}</div>'
                f'<div style="font-size:11px;color:var(--ink-faint);margin-top:4px">{sub}</div>'
                f'<div style="margin-top:10px;font-size:11px;font-weight:600;color:{badge_color}">{badge}</div>'
                f"</div>"
            )

        p_ok = precision >= 0.8 if precision is not None else None
        r_ok = recall >= 0.8 if recall is not None else None
        k_ok = ranking >= 0.6 if ranking is not None else None
        s_ok = len(schema_errors) == 0

        cards_html = (
            f'<div style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:24px">'
            + _card(
                "Precision",
                f"{precision:.0%}" if precision is not None else "—",
                "incident type match",
                p_ok,
            )
            + _card(
                "Recall",
                f"{recall:.0%}" if recall is not None else "—",
                "types detected",
                r_ok,
            )
            + _card(
                "Ranking",
                f"{ranking:.0%}" if ranking is not None else "—",
                "host hit rate",
                k_ok,
            )
            + _card("Schema errors", str(len(schema_errors)), "validation errors", s_ok)
            + f"</div>"
        )
        st.markdown(cards_html, unsafe_allow_html=True)

        if cluster_ok is not None:
            dot = "var(--leaf-400)" if cluster_ok else "var(--sev-3)"
            label = "Cluster detected" if cluster_ok else "No cluster detected"
            st.markdown(
                f'<p style="font-size:13px;color:var(--ink-faint);margin:0 0 12px">'
                f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
                f'background:{dot};margin-right:6px;vertical-align:middle"></span>{label}</p>',
                unsafe_allow_html=True,
            )
        if warnings:
            st.warning("Scenario warnings: " + " · ".join(warnings))

    report_key = f"{run_id}/validation_report.md"
    if store.exists(report_key):
        with st.expander("Full validation report", expanded=not bool(summary)):
            st.markdown(store.read_text(report_key))
    elif not summary:
        st.info("No validation report found for this run.")


# ─────────────────────────────────────────────────────────
# Pipeline / "How it works" tab
# ─────────────────────────────────────────────────────────


def render_pipeline(fleet: Dict, store, run_id: str) -> None:
    # Gather live stats from the current run
    total_events = error_count = warning_count = snapshot_count = 0
    sample_event_msg = ""
    for key in store.list(f"{run_id}/snapshots"):
        if not key.endswith(".json"):
            continue
        try:
            snap = json.loads(store.read_text(key))
            evts = snap.get("events", [])
            total_events += len(evts)
            for e in evts:
                lvl = e.get("level", "")
                if lvl == "Error":
                    error_count += 1
                    if not sample_event_msg:
                        sample_event_msg = e.get("message", "")[:80]
                elif lvl == "Warning":
                    warning_count += 1
            snapshot_count += 1
        except Exception:
            pass

    clusters = fleet.get("clusters", [])
    incident_count = fleet.get("incident_count", 0)
    cluster_types = len({c.get("type") for c in clusters})
    risk = fleet.get("overall_risk_score", 0)
    host_count = fleet.get("host_count", snapshot_count)
    top_hosts = fleet.get("top_hosts", [])
    action_counts: Dict[str, int] = {}
    for h in top_hosts:
        a = h.get("action", "monitor")
        action_counts[a] = action_counts.get(a, 0) + 1
    action_text = " · ".join(
        f"{v} {k}" for k, v in sorted(action_counts.items(), key=lambda x: -x[1])
    )

    # Sample incident for the "detect" step
    sample_inc = ""
    for c in clusters[:1]:
        sample_inc = html.escape(c.get("signature_key", "")[:60])

    steps = [
        {
            "n": "1",
            "title": "Collect",
            "stat": f"{total_events:,} events · {snapshot_count} hosts",
            "desc": (
                "A PowerShell script runs on each Windows endpoint and queries "
                "System and Application event logs for the last 24 hours."
            ),
            "code": (
                "# query event logs\n"
                "Get-WinEvent -FilterHashtable @{\n"
                "  LogName   = 'System', 'Application'\n"
                "  StartTime = (Get-Date).AddHours(-24)\n"
                "}"
            ),
        },
        {
            "n": "2",
            "title": "Sanitise",
            "stat": f"{error_count} errors · {warning_count} warnings",
            "desc": (
                "PII is scrubbed on the host before the snapshot leaves the machine: "
                "email addresses, file paths, and IP addresses are replaced with safe tokens."
            ),
            "code": (
                "# e-mail\n"
                "$msg -replace '[A-Za-z0-9._%+-]+@[^\\s]+', '[REDACTED_EMAIL]'\n"
                "# paths\n"
                "$msg -replace '[A-Za-z]:\\\\[^\\s]+', '[REDACTED_PATH]'"
            ),
        },
        {
            "n": "3",
            "title": "Detect",
            "stat": f"{incident_count} incidents in {host_count} host timelines",
            "desc": (
                "Each host's event stream is matched against known signatures: "
                "BSOD / BugCheck, service crash loops, Windows Update failures, "
                "disk pressure, and network instability."
            ),
            "code": f"# matched pattern\n{sample_inc}" if sample_inc else None,
        },
        {
            "n": "4",
            "title": "Cluster",
            "stat": f"{len(clusters)} clusters · {cluster_types} distinct types",
            "desc": (
                "Incidents sharing the same signature hash across hosts are grouped. "
                "Seven machines with identical BSODs is a fleet-wide driver problem, "
                "not seven coincidences."
            ),
            "code": None,
        },
        {
            "n": "5",
            "title": "Score",
            "stat": f"Fleet risk {risk} · {host_count} hosts ranked",
            "desc": (
                "Each host gets a 0–100 risk score weighted by incident severity, "
                "detection confidence, and how many other machines share the same pattern."
            ),
            "code": None,
        },
        {
            "n": "6",
            "title": "Act",
            "stat": action_text or f"{host_count} hosts triaged",
            "desc": (
                "Every host gets a verdict (monitor / investigate / contact) "
                "and specific remediation steps — roll back a driver, restart a service, "
                "capture a minidump before the next reboot wipes it."
            ),
            "code": None,
        },
    ]

    st.markdown(
        '<div class="card" style="margin-top:24px;padding:28px 32px 20px;">'
        '<p class="eyebrow">Under the hood</p>'
        '<h2 style="font-family:var(--font-display);font-style:italic;font-weight:500;margin-bottom:20px;">'
        "How the data flows</h2>"
        "</div>",
        unsafe_allow_html=True,
    )

    row1 = st.columns(3)
    row2 = st.columns(3)
    col_map = [row1[0], row1[1], row1[2], row2[0], row2[1], row2[2]]

    for col, s in zip(col_map, steps):
        code_block = ""
        if s.get("code"):
            code_block = f'<pre class="pipe-code">{html.escape(s["code"])}</pre>'
        with col:
            st.markdown(
                f'<div class="pipe-step">'
                f'<div class="pipe-num">{s["n"]}</div>'
                f'<div class="pipe-title">{html.escape(s["title"])}</div>'
                f'<div class="pipe-stat">{html.escape(s["stat"])}</div>'
                f'<div class="pipe-desc">{s["desc"]}</div>'
                f"{code_block}"
                f"</div>",
                unsafe_allow_html=True,
            )


# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(
        page_title="Pre-emptive IT — Incident Dashboard",
        page_icon="🖥️",
        layout="wide",
    )
    inject_css()

    # Dark mode — set/remove data-mode="dark" on <html> via iframe JS
    dark = st.session_state.get("dark_mode", False)
    _dm_js = (
        'setAttribute("data-mode","dark")' if dark else 'removeAttribute("data-mode")'
    )
    components.html(
        f"<script>window.parent.document.documentElement.{_dm_js}</script>",
        height=0,
    )

    # Handle regeneration before rendering anything else
    if st.session_state.get("_regen"):
        del st.session_state["_regen"]
        spinner_msg = "Rebooting the fleet…" if _is_workshop() else "Growing new data…"
        with st.spinner(spinner_msg):
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
            "Synthetic fleet of 20 Windows endpoints. "
            "Incidents detected before they become outages."
        )
        st.divider()
        if "_view" not in st.session_state:
            st.session_state["_view"] = "📰 Broadsheet"
        st.radio(
            "Fleet view",
            ["📰 Broadsheet", "🌿 Meadow", "🖥️ Workshop"],
            key="_view",
            horizontal=True,
            help="Meadow: clusters as plants. Workshop: clusters as computers. Broadsheet: editorial dashboard.",
        )
        st.divider()
        st.toggle("🌙 Dark mode", key="dark_mode")
        st.divider()
        if _is_broadsheet():
            regen_icon = "↻"
        else:
            regen_icon = "🔄" if _is_workshop() else "🌱"
        if st.button(f"{regen_icon} Regenerate demo data", use_container_width=True):
            st.session_state["_regen"] = True
            st.rerun()
        st.divider()
        if _is_broadsheet():
            st.caption("*The facts, neatly typeset.*")
        elif _is_workshop():
            st.caption("*Every machine deserves a good morning.*")
        else:
            st.caption("*Kevät tulee aina.* Spring always comes.")
        st.markdown(
            "[Source on GitHub](https://github.com/Alleyfoo/Pre-emptive-IT-Incident-Dashboard)"
        )
        st.divider()
        st.caption("Run this app locally:")
        st.code("streamlit run streamlit_app.py", language="powershell")

    store = _store()
    runs = _available_runs(store)
    if not runs:
        st.error(
            "Demo data generation failed. Click 'Regenerate demo data' in the sidebar."
        )
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

    if _is_broadsheet():
        fleet_tab_label = "Fleet — last 24 h"
        tab_labels = [fleet_tab_label, "Host timeline", "How it works", "Validation"]
    else:
        fleet_tab_label = (
            "🖥️ Fleet — last 24 h" if _is_workshop() else "🌿 Fleet — last 24 h"
        )
        tab_labels = [
            fleet_tab_label,
            "🔍 Host timeline",
            "⚙️ How it works",
            "✅ Validation",
        ]
    tab_fleet, tab_host, tab_pipeline, tab_validation = st.tabs(tab_labels)

    with tab_fleet:
        if _is_broadsheet():
            render_broadsheet(fleet, run_id, store)
        else:
            render_hero(fleet, run_id)
            if _is_workshop():
                render_workshop(fleet.get("clusters", []))
            else:
                render_meadow(fleet.get("clusters", []))
            clusters = fleet.get("clusters", [])
            render_cluster_ledger(clusters)
            selected_hash = st.session_state.get("selected_cluster")
            selected_cluster = next(
                (c for c in clusters if c.get("signature_hash") == selected_hash), None
            )
            render_hosts_grid(
                fleet.get("top_hosts", []), selected_cluster=selected_cluster
            )

    with tab_host:
        host_options = _host_options(store, run_id, fleet)
        if not host_options:
            st.info("No hosts available for this run.")
        else:
            # Keep selection in session; reset if it falls out of this run.
            if (
                "_host_id" not in st.session_state
                or st.session_state["_host_id"] not in host_options
            ):
                st.session_state["_host_id"] = host_options[0]

            # Score map for the chip labels (hosts not in top_hosts get no number).
            score_map = {
                h.get("host_id"): int(h.get("score", 0) or 0)
                for h in fleet.get("top_hosts", [])
            }

            st.markdown(
                "<div style='font-size:11px;letter-spacing:0.08em;"
                "text-transform:uppercase;color:var(--bs-ink-faint,#888);"
                "margin:4px 0 6px'>Hosts · click to inspect</div>",
                unsafe_allow_html=True,
            )
            cols_per_row = 6 if len(host_options) > 12 else 4
            for row_start in range(0, len(host_options), cols_per_row):
                row = host_options[row_start : row_start + cols_per_row]
                cols = st.columns(cols_per_row)
                for col, host in zip(cols, row):
                    sc = score_map.get(host, 0)
                    label = f"{host} · {sc}" if sc else host
                    selected = host == st.session_state["_host_id"]
                    if col.button(
                        label,
                        key=f"hostbtn_{host}",
                        use_container_width=True,
                        type="primary" if selected else "secondary",
                    ):
                        st.session_state["_host_id"] = host
                        st.rerun()

            st.divider()
            render_host_view(store, run_id, st.session_state["_host_id"], fleet)

    with tab_pipeline:
        render_pipeline(fleet, store, run_id)

    with tab_validation:
        render_validation(store, run_id)

    _view = st.session_state.get("_view", "🖥️ Workshop")
    if "Broadsheet" in _view:
        _footer_left = "<em>The facts, neatly typeset.</em>"
    elif "Workshop" in _view:
        _footer_left = "<em>Every machine deserves a good morning.</em>"
    else:
        _footer_left = "<em>Kevät tulee aina.</em>"
    _seed_used = st.session_state.get("_seed", 42)
    st.markdown(
        f'<div class="footnote">{_footer_left}'
        f"<span>All data is synthetic · seed {_seed_used}</span></div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
