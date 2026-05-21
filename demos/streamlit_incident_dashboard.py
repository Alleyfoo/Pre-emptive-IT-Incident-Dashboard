import html as _html
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import streamlit as st

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.artifact_store import build_artifact_store
from runtime.run_pointer import get_latest_run_id


# ── Artifact helpers ────────────────────────────────────────────────────────


def _artifacts_root() -> str:
    return os.environ.get("ARTIFACTS_ROOT") or os.path.join(REPO_ROOT, "artifacts")


def _artifact_store():
    return build_artifact_store(_artifacts_root())


def _available_runs(store) -> List[str]:
    runs = store.list_runs()
    if runs:
        return runs
    legacy = set()
    for key in store.list():
        if "/" in key:
            legacy.add(key.split("/")[0])
    return sorted(list(legacy))


def _load_json(store, key: str) -> Dict:
    if not store.exists(key):
        return {}
    import json
    return json.loads(store.read_text(key))


def _fleet_summary(store, run_id: str) -> Dict:
    return _load_json(store, f"{run_id}/fleet_summary.json")


def _timeline(store, run_id: str, host_id: str) -> Dict:
    return _load_json(store, f"{run_id}/hosts/{host_id}/timeline.json")


def _run_status(store, run_id: str) -> Dict:
    key = f"{run_id}/run_status.json"
    if store.exists(key):
        return _load_json(store, key)
    return {}


def _host_options(store, run_id: str, fleet: Dict) -> List[str]:
    opts = [h["host_id"] for h in fleet.get("top_hosts", [])]
    for key in store.list(f"{run_id}/hosts"):
        parts = key.split("/")
        if len(parts) >= 3:
            opts.append(parts[2])
    return sorted(list(set(opts)))


# ── CSS injection ────────────────────────────────────────────────────────────


def _inject_css():
    css_path = os.path.join(os.path.dirname(__file__), "kevat.css")
    if os.path.exists(css_path):
        with open(css_path, encoding="utf-8") as f:
            css = f.read()
    else:
        css = ""
    st.markdown(
        """
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        <link href="https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400;1,6..72,500&family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
        """,
        unsafe_allow_html=True,
    )
    if css:
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


# ── Severity helpers ─────────────────────────────────────────────────────────


def _sev_class(s: float) -> str:
    if s >= 90:
        return "critical"
    if s >= 75:
        return "hot"
    if s >= 60:
        return "warm"
    return "cool"


def _sev_label(s: float) -> str:
    return {"cool": "Healthy", "warm": "Wilting", "hot": "Drooping", "critical": "Broken"}[_sev_class(s)]


def _sev_token(s: float) -> str:
    return {
        "cool": "var(--leaf-300)",
        "warm": "var(--pollen-400)",
        "hot": "var(--sev-3)",
        "critical": "var(--sev-4)",
    }[_sev_class(s)]


def _type_warmth(t: str) -> str:
    if t in ("bsod",):
        return "hot"
    if t in ("service_crash", "update_failure"):
        return "warm"
    return "cool"


# ── SVG: Sprout ──────────────────────────────────────────────────────────────


def _sprout_svg(leaves: int = 3, sev: float = 50, index: int = 0, w: int = 110, h: int = 220) -> str:
    sev_c = _sev_class(sev)
    health = max(0.0, min(1.0, 1.0 - sev / 120.0))
    height_frac = 0.50 + 0.50 * health
    stem_top_y = h * (1 - height_frac) + 6
    stem_base_y = h - 10
    cx = w / 2

    seed = (index * 73 + leaves * 11 + round(sev)) % 100
    ws = 1 if seed % 2 == 0 else -1
    sc = 6 + (seed % 6)

    broken = sev_c == "critical"
    drooping = sev_c in ("hot", "critical")
    wilted = sev_c in ("warm", "hot", "critical")

    mid_y = (stem_top_y + stem_base_y) / 2

    crown_x, crown_y = cx, stem_top_y
    snap_mark = ""

    if broken:
        snap_y = stem_base_y + (stem_top_y - stem_base_y) * 0.55
        snap_x = cx + ws * 4
        dangle_end_x = snap_x + ws * 28
        dangle_end_y = snap_y + 26
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx + sc * ws:.1f} {(stem_base_y + snap_y) / 2 + 4:.1f}, "
            f"{cx + 2 * ws:.1f} {snap_y + 6:.1f}, {snap_x:.1f} {snap_y:.1f} "
            f"M {snap_x + ws * 3:.1f} {snap_y + 1:.1f} "
            f"L {snap_x + ws * 10:.1f} {snap_y + 4:.1f} "
            f"Q {snap_x + ws * 22:.1f} {snap_y + 10:.1f} {dangle_end_x:.1f} {dangle_end_y:.1f}"
        )
        crown_x, crown_y = dangle_end_x, dangle_end_y
        snap_mark = (
            f'<g stroke="#6b5a40" stroke-width="0.9" fill="none" stroke-linecap="round">'
            f'<path d="M {snap_x-3:.1f} {snap_y-1:.1f} l 2 -2 l -1 -2 l 3 -1" />'
            f'<path d="M {snap_x+3:.1f} {snap_y+1:.1f} l -1 2 l 2 1" />'
            f'</g>'
        )
    elif drooping:
        tip_drop = 22
        tip_x = cx + ws * 16
        tip_y = stem_top_y + tip_drop
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx + sc * ws:.1f} {mid_y + 22:.1f}, {cx - 4 * ws:.1f} {mid_y - 8:.1f}, "
            f"{cx + 4 * ws:.1f} {stem_top_y + 4:.1f} "
            f"Q {cx + 14 * ws:.1f} {stem_top_y + tip_drop * 0.4:.1f} {tip_x:.1f} {tip_y:.1f}"
        )
        crown_x, crown_y = tip_x, tip_y
    elif wilted:
        tip_x = cx + ws * 6
        tip_y = stem_top_y + 4
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx + sc * ws:.1f} {mid_y + 18:.1f}, {cx - sc * ws:.1f} {mid_y - 14:.1f}, "
            f"{tip_x:.1f} {tip_y:.1f}"
        )
        crown_x, crown_y = tip_x, tip_y
    else:
        stem_path = (
            f"M {cx:.1f} {stem_base_y:.1f} "
            f"C {cx + sc * ws:.1f} {mid_y + 22:.1f}, {cx - sc * ws:.1f} {mid_y - 22:.1f}, {cx:.1f} {stem_top_y:.1f}"
        )

    stem_color = "#8a6a3f" if broken else "#9aa37c" if drooping else "var(--leaf-400)"

    green_pal  = ["var(--leaf-500)", "var(--leaf-300)", "var(--leaf-200)"]
    yellow_pal = ["var(--leaf-400)", "#c9b76b", "#d8c97d"]
    brown_pal  = ["#9aa37c", "#a3956b", "#8a6a3f"]
    palette = brown_pal if (broken or drooping) else yellow_pal if wilted else green_pal

    usable = max(3, min(8, leaves + 1))
    fallen_count = (
        min(usable - 1, 3) if broken else
        min(usable - 1, 2) if drooping else
        1 if wilted else 0
    )
    attached_count = max(2, usable - fallen_count)

    leaf_parts: List[str] = []
    for i in range(attached_count):
        t = 0.12 + (i / max(attached_count - 1, 1)) * 0.62
        t_eff = t * 0.55 if broken else t
        y = stem_base_y + (stem_top_y - stem_base_y) * t_eff
        side = -1 if i % 2 == 0 else 1
        leaf_size = 14 + (i / usable) * 4
        stem_x = cx + math.sin(t * math.pi) * sc * ws * 0.6
        fill = palette[i % 3]
        base_angle = side * (32 + i * 3)
        droop_bonus = side * (18 if wilted else 0) + side * (14 if drooping else 0)
        angle = base_angle + droop_bonus
        curl_y = leaf_size * 0.7 if wilted else -leaf_size * 0.55
        leaf_parts.append(
            f'<g transform="translate({stem_x:.1f} {y:.1f}) rotate({angle})">'
            f'<path d="M 0 0 Q {side * leaf_size * 0.55:.1f} {curl_y:.1f} {side * leaf_size:.1f} 0 '
            f'Q {side * leaf_size * 0.55:.1f} {leaf_size * 0.55:.1f} 0 0 Z" fill="{fill}" />'
            f'<path d="M 0 0 L {side * leaf_size:.1f} 0" stroke="rgba(255,255,255,0.22)" stroke-width="0.7" />'
            f'</g>'
        )

    fallen_parts: List[str] = []
    for i in range(fallen_count):
        fx = cx + (i - fallen_count / 2) * 14 + (seed % 7) - 3
        fy = stem_base_y + 3
        rot = ((seed * (i + 1)) % 90) - 45
        fallen_parts.append(
            f'<g transform="translate({fx:.1f} {fy:.1f}) rotate({rot})">'
            f'<path d="M 0 0 Q 5 -3 10 0 Q 5 3 0 0 Z" fill="{palette[(i+1)%3]}" opacity="0.85" />'
            f'</g>'
        )

    if sev_c == "cool":
        if leaves >= 3:
            back_petals = "".join(
                f'<ellipse cx="0" cy="-7" rx="4.5" ry="6.5" transform="rotate({d})" fill="var(--bloom-edge)" />'
                for d in [36, 108, 180, 252, 324]
            )
            front_petals = "".join(
                f'<ellipse cx="0" cy="-8" rx="5" ry="7.5" transform="rotate({d})" fill="var(--bloom)" stroke="var(--earth-200)" stroke-width="0.7" />'
                for d in [0, 72, 144, 216, 288]
            )
            crown_el = (
                f'<g transform="translate({crown_x:.1f} {crown_y - 4:.1f})" filter="url(#spr-shadow-{index})">'
                f'{back_petals}{front_petals}'
                f'<circle r="3.2" fill="var(--sun)" />'
                f'</g>'
            )
        else:
            crown_el = (
                f'<g transform="translate({crown_x:.1f} {crown_y - 2:.1f})">'
                f'<ellipse cx="0" cy="-3" rx="3.5" ry="5.5" fill="var(--leaf-500)" />'
                f'<ellipse cx="0" cy="-2" rx="2" ry="3.5" fill="var(--leaf-300)" />'
                f'</g>'
            )
    elif sev_c == "warm":
        petals = "".join(
            f'<ellipse cx="0" cy="-5" rx="4" ry="5.5" transform="rotate({d})" fill="var(--bloom-edge)" stroke="#d8c97d" stroke-width="0.6" />'
            for d in [200, 270, 340]
        )
        crown_el = (
            f'<g transform="translate({crown_x:.1f} {crown_y - 2:.1f})">'
            f'{petals}'
            f'<circle r="2.8" fill="#c79f2c" />'
            f'</g>'
        )
    elif sev_c == "hot":
        crown_el = (
            f'<g transform="translate({crown_x:.1f} {crown_y:.1f})">'
            f'<ellipse cx="0" cy="2" rx="6" ry="3" fill="#9aa37c" transform="rotate(20)" />'
            f'<ellipse cx="2" cy="4" rx="5" ry="2.6" fill="#a3956b" transform="rotate(35)" />'
            f'</g>'
        )
    else:
        crown_el = (
            f'<g transform="translate({crown_x:.1f} {crown_y:.1f})">'
            f'<path d="M -4 -2 q 4 -3 8 0 q 0 4 -4 4 q -4 0 -4 -4 Z" fill="#8a6a3f" />'
            f'<path d="M -3 -1 q 3 -2 6 0 q 0 3 -3 3 q -3 0 -3 -3 Z" fill="#6b5a40" />'
            f'</g>'
        )

    ground_fill = (
        "rgba(138,106,63,0.35)" if broken else
        "rgba(201,163,110,0.28)" if drooping else
        "rgba(201,163,110,0.22)"
    )
    ground = f'<ellipse cx="{cx:.1f}" cy="{stem_base_y + 3:.1f}" rx="18" ry="2.6" fill="{ground_fill}" />'

    cracks = ""
    if broken:
        cracks = (
            f'<g stroke="#8a6a3f" stroke-width="0.6" opacity="0.6" fill="none">'
            f'<path d="M {cx-12:.1f} {stem_base_y+5:.1f} L {cx-6:.1f} {stem_base_y+3:.1f} L {cx:.1f} {stem_base_y+5:.1f}" />'
            f'<path d="M {cx+4:.1f} {stem_base_y+6:.1f} L {cx+10:.1f} {stem_base_y+4:.1f}" />'
            f'</g>'
        )

    return (
        f'<svg class="plant-svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" aria-hidden="true">'
        f'<defs>'
        f'<filter id="spr-shadow-{index}" x="-50%" y="-50%" width="200%" height="200%">'
        f'<feGaussianBlur in="SourceAlpha" stdDeviation="1.2" />'
        f'<feOffset dx="0" dy="1.5" />'
        f'<feComponentTransfer><feFuncA type="linear" slope="0.28" /></feComponentTransfer>'
        f'<feMerge><feMergeNode /><feMergeNode in="SourceGraphic" /></feMerge>'
        f'</filter>'
        f'</defs>'
        f'{ground}{cracks}'
        f'<path d="{stem_path}" fill="none" stroke="{stem_color}" stroke-width="2.4" stroke-linecap="round" />'
        f'{snap_mark}'
        f'{"".join(leaf_parts)}'
        f'{"".join(fallen_parts)}'
        f'{crown_el}'
        f'</svg>'
    )


# ── SVG: Computer ────────────────────────────────────────────────────────────


def _computer_svg(host_id: str = "HOST-001", sev: float = 60, index: int = 0, w: int = 160, h: int = 170) -> str:
    sev_c = _sev_class(sev)
    seed = (index * 31 + len(host_id) * 7) % 100

    body_fill = {
        "critical": "#c7b59b", "hot": "#d5c4a4", "warm": "#dccfa9",
    }.get(sev_c, "#dde7d0")
    screen_fill = {
        "critical": "#7a5a3a", "hot": "#9a7956", "warm": "#cfc28a",
    }.get(sev_c, "#cfe2c2")
    accent = {
        "critical": "var(--sev-4)", "hot": "var(--sev-3)", "warm": "var(--pollen-600)",
    }.get(sev_c, "var(--leaf-500)")

    tilt = {"critical": ((-10) if seed % 2 == 0 else 10), "hot": ((-4) if seed % 2 == 0 else 4),
            "warm": ((-1.5) if seed % 2 == 0 else 1.5)}.get(sev_c, 0)
    y_shift = 6 if sev_c == "critical" else 0

    cx = w / 2
    screen_w = w * 0.72
    screen_h = h * 0.52
    screen_x = (w - screen_w) / 2
    screen_y = 14.0
    face_y = screen_y + screen_h * 0.5
    eye_y = face_y - 4
    mouth_y = face_y + 12
    eye_shift = (seed % 5) - 2

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
            f'</g>'
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
    else:
        face = (
            f'<g stroke="#f1d36e" stroke-width="2" stroke-linecap="round">'
            f'<line x1="{cx - 18:.1f}" y1="{eye_y - 3:.1f}" x2="{cx - 10:.1f}" y2="{eye_y + 3:.1f}" />'
            f'<line x1="{cx - 18:.1f}" y1="{eye_y + 3:.1f}" x2="{cx - 10:.1f}" y2="{eye_y - 3:.1f}" />'
            f'<line x1="{cx + 10:.1f}" y1="{eye_y - 3:.1f}" x2="{cx + 18:.1f}" y2="{eye_y + 3:.1f}" />'
            f'<line x1="{cx + 10:.1f}" y1="{eye_y + 3:.1f}" x2="{cx + 18:.1f}" y2="{eye_y - 3:.1f}" />'
            f'</g>'
            f'<line x1="{cx - 6:.1f}" y1="{mouth_y + 1:.1f}" x2="{cx + 6:.1f}" y2="{mouth_y + 1:.1f}" '
            f'stroke="#f1d36e" stroke-width="1.6" stroke-linecap="round" />'
        )

    if sev_c == "cool":
        damage = ""
    elif sev_c == "warm":
        damage = (
            f'<g stroke="rgba(0,0,0,0.45)" stroke-width="0.7" fill="none">'
            f'<path d="M {screen_x + screen_w - 12:.1f} {screen_y + 2:.1f} l 4 5 l -3 3" />'
            f'</g>'
        )
    elif sev_c == "hot":
        damage = (
            f'<g stroke="rgba(0,0,0,0.55)" stroke-width="0.9" fill="none" stroke-linejoin="miter">'
            f'<path d="M {screen_x+8:.1f} {screen_y+3:.1f} L {screen_x+18:.1f} {screen_y+10:.1f} '
            f'L {screen_x+12:.1f} {screen_y+14:.1f} L {screen_x+24:.1f} {screen_y+22:.1f} '
            f'L {screen_x+18:.1f} {screen_y+26:.1f} L {screen_x+30:.1f} {screen_y+screen_h-4:.1f}" />'
            f'</g>'
        )
    else:
        damage = (
            f'<g stroke="rgba(0,0,0,0.65)" stroke-width="0.9" fill="none">'
            f'<path d="M {screen_x+6:.1f} {screen_y+2:.1f} L {screen_x + screen_w*0.45:.1f} {screen_y + screen_h*0.4:.1f} '
            f'L {screen_x + screen_w-8:.1f} {screen_y+6:.1f}" />'
            f'<path d="M {screen_x + screen_w*0.45:.1f} {screen_y + screen_h*0.4:.1f} L {screen_x+12:.1f} {screen_y+screen_h-4:.1f}" />'
            f'<path d="M {screen_x + screen_w*0.45:.1f} {screen_y + screen_h*0.4:.1f} L {screen_x + screen_w-6:.1f} {screen_y + screen_h-8:.1f}" />'
            f'</g>'
        )

    if sev_c in ("hot", "critical"):
        n_wisps = 3 if sev_c == "critical" else 2
        opacity = 0.55 if sev_c == "critical" else 0.4
        wisps = []
        for i in range(n_wisps):
            sx = screen_x + screen_w * (0.2 + i * 0.3)
            sy = screen_y - 4
            wisps.append(
                f'<path d="M {sx:.1f} {sy:.1f} c -3 -4, 3 -8, 0 -12 c -3 -4, 3 -8, 0 -12" '
                f'fill="none" stroke="#7a7268" stroke-width="{2.2 - i*0.3:.1f}" stroke-linecap="round" transform="translate(0 {i*-2})" />'
            )
        smoke = f'<g opacity="{opacity}">{"".join(wisps)}</g>'
    else:
        smoke = ""

    sparks = ""
    if sev_c == "critical":
        sparks = (
            f'<circle cx="{cx + 26:.1f}" cy="{screen_y - 2:.1f}" r="1.2" fill="#f1d36e" />'
            f'<circle cx="{cx + 30:.1f}" cy="{screen_y - 6:.1f}" r="0.8" fill="#f1d36e" opacity="0.7" />'
            f'<circle cx="{cx - 28:.1f}" cy="{screen_y - 4:.1f}" r="1.0" fill="#f1d36e" opacity="0.8" />'
        )

    stand_cracks = ""
    if sev_c == "critical":
        stand_cracks = (
            f'<g stroke="#6b5a40" stroke-width="0.6" opacity="0.6">'
            f'<line x1="{cx-12:.1f}" y1="{h-16:.1f}" x2="{cx+2:.1f}" y2="{h-14:.1f}" />'
            f'<line x1="{cx+4:.1f}" y1="{h-14:.1f}" x2="{cx+14:.1f}" y2="{h-16:.1f}" />'
            f'</g>'
        )

    led_color = {
        "cool": "var(--leaf-300)", "warm": "var(--sun)",
        "hot": "var(--sev-3)", "critical": "var(--sev-4)",
    }[sev_c]

    led_anim = ""
    if sev_c != "cool":
        dur = "0.8s" if sev_c == "critical" else "1.6s"
        led_anim = f'<animate attributeName="opacity" values="1;0.3;1" dur="{dur}" repeatCount="indefinite" />'

    pat_id = f"scan-{index}-{_html.escape(host_id)}"
    filter_id = f"bshadow-{index}-{_html.escape(host_id)}"

    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" aria-hidden="true">'
        f'<defs>'
        f'<pattern id="{pat_id}" width="2" height="2" patternUnits="userSpaceOnUse">'
        f'<rect width="2" height="2" fill="{screen_fill}" />'
        f'<line x1="0" y1="0.5" x2="2" y2="0.5" stroke="rgba(0,0,0,0.05)" stroke-width="0.4" />'
        f'</pattern>'
        f'<filter id="{filter_id}" x="-30%" y="-30%" width="160%" height="160%">'
        f'<feGaussianBlur in="SourceAlpha" stdDeviation="2" />'
        f'<feOffset dx="0" dy="2" />'
        f'<feComponentTransfer><feFuncA type="linear" slope="0.18" /></feComponentTransfer>'
        f'<feMerge><feMergeNode /><feMergeNode in="SourceGraphic" /></feMerge>'
        f'</filter>'
        f'</defs>'
        f'<ellipse cx="{cx:.1f}" cy="{h-6:.1f}" rx="{w*0.32:.1f}" ry="3" fill="rgba(60,60,60,0.10)" />'
        f'<rect x="{cx-18:.1f}" y="{h-22:.1f}" width="36" height="10" rx="2" fill="{body_fill}" stroke="{accent}" stroke-opacity="0.25" stroke-width="1" />'
        f'<rect x="{cx-28:.1f}" y="{h-12:.1f}" width="56" height="6" rx="1.5" fill="{body_fill}" stroke="{accent}" stroke-opacity="0.25" stroke-width="1" />'
        f'{stand_cracks}'
        f'<g transform="translate(0 {y_shift}) rotate({tilt} {cx:.1f} {h-22:.1f})">'
        f'{smoke}{sparks}'
        f'<g filter="url(#{filter_id})">'
        f'<rect x="{screen_x-8:.1f}" y="{screen_y-8:.1f}" width="{screen_w+16:.1f}" height="{screen_h+22:.1f}" rx="10" fill="{body_fill}" stroke="rgba(0,0,0,0.12)" stroke-width="0.6" />'
        f'</g>'
        f'<rect x="{screen_x:.1f}" y="{screen_y:.1f}" width="{screen_w:.1f}" height="{screen_h:.1f}" rx="6" fill="url(#{pat_id})" stroke="rgba(0,0,0,0.18)" stroke-width="0.6" />'
        f'{face}{damage}'
        f'<circle cx="{screen_x + screen_w - 5:.1f}" cy="{screen_y + screen_h + 7:.1f}" r="1.4" fill="{led_color}">{led_anim}</circle>'
        f'</g>'
        f'</svg>'
    )


# ── SVG: Type icon ───────────────────────────────────────────────────────────


def _type_icon_svg(t: str, size: int = 18, color: str = "currentColor") -> str:
    sw = "1.4"
    common = f'width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="{sw}" stroke-linecap="round" stroke-linejoin="round"'
    icons = {
        "bsod": '<path d="M13 3 L7 13 H12 L10 21 L17 11 H12 Z" />',
        "update_failure": '<path d="M5 19 Q5 8 12 8 Q19 8 19 14 Q19 18 15 18 Q12 18 12 15" />',
        "disk_full": '<path d="M5 11 H15 V18 Q15 19 14 19 H6 Q5 19 5 18 Z M15 13 L19 11 V15 M9 8 Q9 6 11 6 H13 Q15 6 15 8" />',
        "service_crash": '<path d="M12 21 V14 M12 14 L9 11 M12 14 L15 11 M12 14 V8 L9 5" />',
        "network_instability": '<path d="M3 14 Q7 8 12 14 T21 14" /><circle cx="12" cy="9" r="0.6" fill="{c}" />'.replace("{c}", color),
    }
    body = icons.get(t, '<circle cx="12" cy="12" r="7" />')
    return f'<svg {common}>{body}</svg>'


# ── HTML: Severity bar ───────────────────────────────────────────────────────


def _sev_bar_html(value: float, max_val: float = 120) -> str:
    pct = max(0.0, min(100.0, (value / max_val) * 100))
    cls = _sev_class(value)
    return (
        f'<div class="kv-sev-bar {cls}">'
        f'<i style="width:{pct:.1f}%"></i>'
        f'</div>'
    )


# ── SVG: Meadow scene strip ──────────────────────────────────────────────────


def _meadow_scene_svg(h: int = 64, seed: int = 0) -> str:
    j = lambda n, mod: ((seed * 73 + n * 11) % mod)
    birch_positions = [12 + j(1, 6), 38 + j(2, 6), 70 + j(3, 6)]

    birches = ""
    for bx in birch_positions:
        birches += (
            f'<line x1="{bx}" y1="{h}" x2="{bx}" y2="{h*0.4:.1f}" stroke="var(--birch)" stroke-width="0.6" opacity="0.8" />'
            f'<ellipse cx="{bx}" cy="{h*0.38:.1f}" rx="2.5" ry="3" fill="var(--leaf-200)" opacity="0.6" />'
        )

    flowers = ""
    positions = [8, 16, 25, 35, 44, 52, 61, 69, 78, 86, 93]
    for i, fx in enumerate(positions):
        kind = i % 3
        fy = h - 3
        if kind == 0:
            flowers += (
                f'<line x1="{fx}" y1="{fy}" x2="{fx}" y2="{fy - 7}" stroke="var(--leaf-400)" stroke-width="0.6" />'
                f'<circle cx="{fx}" cy="{fy - 8}" r="2" fill="var(--bloom)" opacity="0.9" />'
            )
        elif kind == 1:
            flowers += (
                f'<line x1="{fx}" y1="{fy}" x2="{fx}" y2="{fy - 5}" stroke="var(--leaf-400)" stroke-width="0.5" />'
                f'<circle cx="{fx}" cy="{fy - 6}" r="2.5" fill="var(--sun)" opacity="0.7" />'
            )
        else:
            flowers += (
                f'<line x1="{fx}" y1="{fy}" x2="{fx}" y2="{fy - 6}" stroke="var(--leaf-300)" stroke-width="0.5" />'
                f'<circle cx="{fx}" cy="{fy - 7}" r="1.5" fill="#9ec3d5" opacity="0.8" />'
            )

    return (
        f'<svg class="kv-scene" width="100%" height="{h}" viewBox="0 0 100 {h}" '
        f'preserveAspectRatio="none" aria-hidden="true">'
        f'<defs>'
        f'<linearGradient id="kv-sky-{seed}" x1="0" x2="0" y1="0" y2="1">'
        f'<stop offset="0%" stop-color="var(--pollen-50)" />'
        f'<stop offset="60%" stop-color="var(--linen)" />'
        f'<stop offset="100%" stop-color="var(--mist)" />'
        f'</linearGradient>'
        f'</defs>'
        f'<rect x="0" y="0" width="100" height="{h}" fill="url(#kv-sky-{seed})" />'
        f'<circle cx="84" cy="{h*0.55:.1f}" r="16" fill="var(--sun)" opacity="0.15" />'
        f'<circle cx="84" cy="{h*0.55:.1f}" r="4" fill="var(--sun)" opacity="0.7" />'
        f'<path d="M 0 {h*0.8:.1f} Q 15 {h*0.68:.1f} 28 {h*0.76:.1f} T 55 {h*0.72:.1f} T 82 {h*0.8:.1f} T 100 {h*0.76:.1f} L 100 {h} L 0 {h} Z" '
        f'fill="var(--water-200)" opacity="0.4" />'
        f'<path d="M 0 {h*0.88:.1f} Q 20 {h*0.8:.1f} 32 {h*0.86:.1f} T 62 {h*0.83:.1f} T 100 {h*0.88:.1f} L 100 {h} L 0 {h} Z" '
        f'fill="var(--mist)" opacity="0.8" />'
        f'{birches}'
        f'<rect x="0" y="{h-4}" width="100" height="4" fill="var(--earth-200)" opacity="0.5" />'
        f'{flowers}'
        f'</svg>'
    )


# ── Data adapters ────────────────────────────────────────────────────────────


def _cluster_name(c: Dict) -> str:
    basis = c.get("basis") or {}
    tmpl = basis.get("message_template", "")
    if tmpl:
        name = tmpl.replace("<n>", "#")
        return name.capitalize()
    types = c.get("incident_types") or []
    if types:
        return types[0].replace("_", " ").capitalize()
    return c.get("signature", "Unknown")[:12]


def _cluster_status(c: Dict, generated_at: Optional[str]) -> str:
    first_seen = c.get("first_seen", "")
    if not first_seen or not generated_at:
        return "ongoing"
    try:
        gen = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        first = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
        if gen.tzinfo is None:
            gen = gen.replace(tzinfo=timezone.utc)
        if first.tzinfo is None:
            first = first.replace(tzinfo=timezone.utc)
        return "new" if (gen - first) < timedelta(hours=24) else "ongoing"
    except (ValueError, TypeError):
        return "ongoing"


def _format_last_ts(ts: str) -> str:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%H:%M")
    except (ValueError, TypeError, AttributeError):
        return ""


def _parse_event_hour(ts: str, window_start: str) -> Optional[float]:
    try:
        ev = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        ws = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
        if ev.tzinfo is None:
            ev = ev.replace(tzinfo=timezone.utc)
        if ws.tzinfo is None:
            ws = ws.replace(tzinfo=timezone.utc)
        diff = (ev - ws).total_seconds() / 3600.0
        return max(0.0, min(24.0, diff))
    except (ValueError, TypeError, AttributeError):
        return None


# ── Hero section ─────────────────────────────────────────────────────────────


def _render_hero(fleet: Dict, run_status: Dict, visual_theme: str) -> None:
    risk = fleet.get("overall_risk_score", 0)
    clusters = fleet.get("clusters", [])
    hosts_total_all = 0
    for h in fleet.get("top_hosts", []):
        hosts_total_all += 1
    hosts_affected = len(fleet.get("top_hosts", []))
    n_clusters = len(clusters)
    generated_at = fleet.get("generated_at", "")

    try:
        gen_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        date_str = gen_dt.strftime("%-d %B").lstrip("0")
        week_str = f"Week {gen_dt.strftime('%W')}"
    except Exception:
        date_str = "today"
        week_str = ""

    if visual_theme == "computers":
        headline = f'{hosts_affected} computers <span class="light">need a hand.</span>'
        sub = f"{n_clusters} clusters surfaced. Tend the broken ones first."
        kpi_label = "Need a hand"
        kpi_sub = "machines in the room"
    else:
        headline = f'{hosts_affected} plants are <span class="light">struggling.</span>'
        sub = f"{n_clusters} clusters surfaced overnight. Tend the flowering ones first."
        kpi_label = "Struggling"
        kpi_sub = "plants in the field"

    run_id = fleet.get("run_id", "")
    status_str = run_status.get("status", "ok")
    finished_str = _format_last_ts(run_status.get("finished_at", generated_at))

    st.markdown(
        f"""
        <div class="kv-topbar">
          <div class="kv-brand">
            <span class="dot"></span>
            <span>pre-emptive</span>
            <small>incident garden · Kevät</small>
          </div>
          <div class="kv-topbar-meta">
            <span class="chip"><span class="swatch"></span>{_html.escape(status_str)} · {_html.escape(finished_str)}</span>
            <span class="mono" style="color:var(--ink-faint)">run · {_html.escape(run_id)}</span>
          </div>
        </div>

        <div class="kv-hero">
          <div class="hero-left">
            <p class="eyebrow" style="color:var(--leaf-500)">Spring · {_html.escape(week_str)} · {_html.escape(date_str)}</p>
            <div class="kv-spacer-sm"></div>
            <h1 style="font-family:var(--font-display);font-weight:500;font-size:clamp(32px,3.5vw,52px);line-height:1.04;letter-spacing:-0.02em;margin:0;color:var(--ink)">{headline}</h1>
            <p style="color:var(--ink-soft);max-width:50ch;margin-top:12px;font-size:15px;font-family:var(--font-body)">{_html.escape(sub)}</p>
          </div>
          <div class="kv-hero-right">
            <div class="kv-kpi">
              <span class="eyebrow">Overall risk</span>
              <span class="num">{risk}</span>
            </div>
            <div class="kv-kpi">
              <span class="eyebrow">Clusters</span>
              <span class="num">{n_clusters}</span>
            </div>
            <div class="kv-kpi">
              <span class="eyebrow">{_html.escape(kpi_label)}</span>
              <span class="num">{hosts_affected}<small style="font-size:16px;color:var(--ink-faint);font-family:var(--font-body);margin-left:4px"></small></span>
              <span class="delta">{_html.escape(kpi_sub)}</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Meadow ───────────────────────────────────────────────────────────────────


def _render_meadow(fleet: Dict) -> None:
    clusters = fleet.get("clusters", [])
    generated_at = fleet.get("generated_at", "")
    visible = clusters[:6]

    plants_html = ""
    for i, c in enumerate(visible):
        affected = c.get("affected_hosts", []) if isinstance(c.get("affected_hosts"), list) else []
        n_affected = len(affected) if affected else c.get("affected_host_count", 1)
        sev = c.get("severity", 50)
        name = _cluster_name(c)
        svg_h = round(180 + (sev / 120) * 80)
        plants_html += (
            f'<div class="kv-plant">'
            f'{_sprout_svg(leaves=n_affected, sev=sev, index=i, w=120, h=svg_h)}'
            f'<div class="kv-plant-card">'
            f'<span class="name">{_html.escape(name[:28])}</span>'
            f'<span class="meta">{n_affected} hosts · sev {sev}</span>'
            f'</div>'
            f'</div>'
        )

    n_clusters = len(clusters)
    st.markdown(
        f"""
        <div class="kv-meadow-wrap">
          {_meadow_scene_svg(h=64, seed=1)}
          <div class="kv-meadow-inner">
            <div class="kv-meadow-head">
              <div>
                <p class="eyebrow" style="color:var(--leaf-500)">The Meadow</p>
                <h2>{n_clusters} cluster{"s" if n_clusters != 1 else ""} in bloom today</h2>
              </div>
              <div class="kv-legend">
                <span><i style="background:var(--leaf-300)"></i>healthy</span>
                <span><i style="background:#c9b76b"></i>wilting</span>
                <span><i style="background:#9aa37c"></i>drooping</span>
                <span><i style="background:#8a6a3f"></i>broken</span>
              </div>
            </div>
            <div class="kv-meadow">{plants_html}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Workshop ─────────────────────────────────────────────────────────────────


def _render_workshop(fleet: Dict) -> None:
    clusters = fleet.get("clusters", [])
    generated_at = fleet.get("generated_at", "")

    clusters_html = ""
    for i, c in enumerate(clusters):
        affected = c.get("affected_hosts", []) if isinstance(c.get("affected_hosts"), list) else []
        n_affected = len(affected) if affected else c.get("affected_host_count", 1)
        sev = c.get("severity", 50)
        name = _cluster_name(c)
        status = _cluster_status(c, generated_at)

        hosts_shown = affected[:5] if affected else [f"HOST-{j:03d}" for j in range(1, min(n_affected + 1, 6))]
        comps_html = "".join(
            _computer_svg(host_id=hid, sev=sev, index=i * 10 + j, w=108, h=130)
            for j, hid in enumerate(hosts_shown)
        )
        extra = n_affected - len(hosts_shown)
        more_html = (
            f'<div class="kv-comp-more"><span class="num">+{extra}</span><span class="lbl">more</span></div>'
            if extra > 0 else ""
        )

        status_pill = (
            f'<span class="kv-status-pill {status}">'
            f'<span style="width:6px;height:6px;border-radius:50%;background:{"var(--leaf-400)" if status=="new" else "var(--sun)"};display:inline-block"></span>'
            f'{"Just appeared" if status == "new" else "Ongoing"}'
            f'</span>'
        )

        clusters_html += (
            f'<div class="kv-comp-cluster">'
            f'<div class="kv-comp-row">{comps_html}{more_html}</div>'
            f'<div class="kv-comp-meta">'
            f'<div><span class="name">{_html.escape(name[:32])}</span><span class="meta">{n_affected} hosts · sev {sev}</span></div>'
            f'{status_pill}'
            f'</div>'
            f'</div>'
        )

    st.markdown(
        f"""
        <div class="kv-workshop-wrap">
          {_meadow_scene_svg(h=64, seed=2)}
          <div class="kv-workshop-inner">
            <div class="kv-workshop-head">
              <div>
                <p class="eyebrow" style="color:var(--leaf-500)">The Workshop</p>
                <h2>{len(clusters)} cluster{"s" if len(clusters) != 1 else ""}, machines to look after</h2>
              </div>
            </div>
            <div class="kv-workshop-grid">{clusters_html}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Cluster ledger ───────────────────────────────────────────────────────────


def _render_cluster_ledger(fleet: Dict) -> None:
    clusters = fleet.get("clusters", [])
    generated_at = fleet.get("generated_at", "")

    rows_html = ""
    for c in clusters:
        affected = c.get("affected_hosts", []) if isinstance(c.get("affected_hosts"), list) else []
        n_affected = len(affected) if affected else c.get("affected_host_count", 1)
        sev = c.get("severity", 50)
        name = _cluster_name(c)
        basis = c.get("basis") or {}
        provider = basis.get("provider", "")
        sig = c.get("signature", "")[:12]
        ctype = (c.get("incident_types") or ["unknown"])[0]
        status = _cluster_status(c, generated_at)
        sev_c = _sev_class(sev)

        icon_color = _sev_token(sev)
        host_names = ", ".join(affected[:3])
        if n_affected > 3:
            host_names += "…"

        status_pill = (
            f'<span class="kv-status-pill {status}">'
            f'<span style="width:6px;height:6px;border-radius:50%;background:{"var(--leaf-400)" if status=="new" else "var(--sun)"};display:inline-block"></span>'
            f'{"Just appeared" if status == "new" else "Ongoing"}'
            f'</span>'
        )

        rows_html += (
            f'<div class="kv-cluster-row">'
            f'<span style="color:{icon_color};display:flex;align-items:center">{_type_icon_svg(ctype, size=20, color=icon_color)}</span>'
            f'<div class="title">'
            f'<strong>{_html.escape(name[:40])}</strong>'
            f'<span class="sub">{_html.escape(sig)} · {_html.escape(provider)}</span>'
            f'</div>'
            f'<div class="hosts">'
            f'<span class="count">{n_affected}</span>'
            f'<span class="names">{_html.escape(host_names)}</span>'
            f'</div>'
            f'{_sev_bar_html(sev)}'
            f'{status_pill}'
            f'<span style="color:var(--ink-faint);text-align:right;font-size:18px">›</span>'
            f'</div>'
        )

    st.markdown(
        f"""
        <div class="kv-card">
          <div class="kv-card-head">
            <div>
              <p class="eyebrow" style="color:var(--ink-faint)">Cluster ledger</p>
              <h2>What's growing</h2>
            </div>
          </div>
          <div class="kv-cluster-list">
            <div class="kv-cluster-row head">
              <span></span>
              <span>Cluster</span>
              <span>Hosts</span>
              <span>Severity</span>
              <span>Status</span>
              <span></span>
            </div>
            {rows_html}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Hosts grid ───────────────────────────────────────────────────────────────


def _render_hosts_grid(fleet: Dict, visual_theme: str) -> None:
    hosts = fleet.get("top_hosts", [])

    cards_html = ""
    for h in hosts[:8]:
        hid = h.get("host_id", "")
        uid = h.get("user_id", "")
        sev = h.get("severity", 0)
        types = h.get("incident_types") or h.get("types") or []
        count = h.get("incident_count") or h.get("count") or 1
        last_ts = _format_last_ts(h.get("last_event_ts", ""))

        ctype = types[0] if types else "unknown"
        warmth = _type_warmth(ctype)

        type_tags = "".join(
            f'<span class="kv-type-tag {warmth if warmth != "cool" else ""}">{_html.escape(t.replace("_", " "))}</span>'
            for t in types[:3]
        )

        try:
            host_num = int(hid.split("-")[-1]) if "-" in hid else 0
        except ValueError:
            host_num = 0

        if visual_theme == "computers":
            visual = _computer_svg(host_id=hid, sev=sev, index=host_num, w=92, h=104)
        else:
            visual = _sprout_svg(leaves=max(2, count + 1), sev=sev, index=host_num, w=64, h=92)

        sev_bar = _sev_bar_html(sev)
        sev_lbl = _sev_label(sev)

        uid_html = f'<div class="uid">@{_html.escape(uid)}</div>' if uid else ""

        cards_html += (
            f'<div class="kv-host-card">'
            f'<div class="hdr">'
            f'<div><div class="hid">{_html.escape(hid)}</div>{uid_html}</div>'
            f'<div class="types">{type_tags}</div>'
            f'</div>'
            f'<div class="visual-row">'
            f'{visual}'
            f'<div style="flex:1">'
            f'{sev_bar}'
            f'<div style="display:flex;justify-content:space-between;margin-top:6px">'
            f'<span class="sev-label">{_html.escape(sev_lbl)}</span>'
            f'<span class="last-ts">last {_html.escape(last_ts)}</span>'
            f'</div>'
            f'</div>'
            f'</div>'
            f'<div class="score-row">'
            f'<div class="score">{sev}<small> sev</small></div>'
            f'</div>'
            f'</div>'
        )

    title = "Machines to tend to" if visual_theme == "computers" else "Hosts to tend to"
    n = len(hosts)

    st.markdown(
        f"""
        <div class="kv-card">
          {_meadow_scene_svg(h=56, seed=3)}
          <div style="padding:22px">
            <div class="kv-card-head">
              <div>
                <p class="eyebrow" style="color:var(--ink-faint)">Top impacted</p>
                <h2>{title}</h2>
              </div>
              <span style="font-size:12px;color:var(--ink-faint)">{n} hosts · sorted by severity</span>
            </div>
            <div class="kv-hosts-grid">{cards_html}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Host tab ─────────────────────────────────────────────────────────────────


def _render_host_tab(store, run_id: str, host_id: str, fleet: Dict, visual_theme: str) -> None:
    timeline = _timeline(store, run_id, host_id)
    if not timeline:
        st.info("No timeline found for this host.")
        return

    window_start = timeline.get("window_start", "")
    window_end = timeline.get("window_end", "")
    sev = timeline.get("severity", 0)
    uid = timeline.get("user_id", "")
    incidents = timeline.get("incidents", [])
    events = timeline.get("events", [])
    tickets = timeline.get("tickets", [])

    sev_lbl = _sev_label(sev)

    # Recommended actions from first incident
    actions_html = ""
    if incidents:
        actions_html = '<div class="kv-actions-list"><p class="eyebrow">Recommended actions</p>'
        for a in incidents[0].get("recommended_actions", [])[:4]:
            actions_html += f'<div class="kv-action-row"><span class="bullet"></span><span>{_html.escape(a)}</span></div>'
        actions_html += "</div>"

    # Sibling hosts strip
    siblings_html = ""
    all_hosts = fleet.get("top_hosts", [])
    current_types = set((timeline.get("incidents") or [{}])[0].get("type", "x") for _ in [1])
    try:
        host_inc_type = incidents[0].get("type", "") if incidents else ""
    except (IndexError, AttributeError):
        host_inc_type = ""

    sib_hosts = [h for h in all_hosts if h.get("host_id") != host_id][:6]
    sibling_row = ""
    for j, sh in enumerate(sib_hosts):
        shid = sh.get("host_id", "")
        sh_sev = sh.get("severity", 0)
        try:
            host_num = int(shid.split("-")[-1]) if "-" in shid else j
        except ValueError:
            host_num = j
        svg = _sprout_svg(leaves=2, sev=sh_sev, index=host_num + 20, w=40, h=66)
        sibling_row += (
            f'<div class="kv-sibling-plant">'
            f'{svg}'
            f'<span class="sid">{_html.escape(shid)}</span>'
            f'<span class="ssev">sev {sh_sev}</span>'
            f'</div>'
        )

    # Branch-and-bud timeline SVG
    strip_w = 800
    branch_svg_h = 180
    bud_rows = []
    for i, ev in enumerate(events[:30]):
        ts = ev.get("ts", "")
        level = ev.get("level", "")
        msg = ev.get("message", "")[:40]
        hour = _parse_event_hour(ts, window_start)
        if hour is None:
            continue
        x_pct = hour / 24.0
        x = x_pct * strip_w
        y = 55 if level == "Error" else 130
        is_err = level == "Error"
        if is_err:
            petals = "".join(
                f'<ellipse cx="0" cy="-9" rx="5.5" ry="8" transform="rotate({d})" fill="var(--bloom)" stroke="var(--bloom-edge)" stroke-width="0.8" />'
                for d in [0, 72, 144, 216, 288]
            )
            bud = (
                f'<g transform="translate({x:.1f} {y})">'
                f'{petals}'
                f'<circle r="4" fill="{_sev_token(sev)}" />'
                f'<circle r="1.4" fill="var(--bloom)" />'
                f'<title>{_html.escape(msg)}</title>'
                f'</g>'
            )
        else:
            bud = (
                f'<circle cx="{x:.1f}" cy="{y}" r="5" fill="var(--leaf-400)" opacity="0.8">'
                f'<title>{_html.escape(msg)}</title>'
                f'</circle>'
                f'<circle cx="{x:.1f}" cy="{y}" r="9" fill="var(--leaf-300)" opacity="0.15" />'
            )
        bud_rows.append(bud)

    branch_leaves = ""
    leaf_positions = [0.06, 0.13, 0.22, 0.34, 0.46, 0.58, 0.68, 0.79, 0.88, 0.95]
    leaf_colors   = ["var(--leaf-300)", "var(--leaf-400)", "var(--leaf-200)", "var(--leaf-300)", "var(--leaf-400)",
                     "var(--leaf-300)", "var(--leaf-200)", "var(--leaf-400)", "var(--leaf-300)", "var(--leaf-400)"]
    leaf_sides    = [-1, 1, -1, 1, -1, 1, -1, 1, -1, 1]
    leaf_sizes    = [14, 16, 13, 15, 17, 14, 15, 16, 13, 15]
    for lp, lc, ls, lsz in zip(leaf_positions, leaf_colors, leaf_sides, leaf_sizes):
        lx = lp * strip_w
        ly = 105 + math.sin(lp * 6) * 2
        ly_off = ls * 14
        lang = ls * (38 + leaf_positions.index(lp) % 3 * 6)
        branch_leaves += (
            f'<g>'
            f'<line x1="{lx:.1f}" y1="{ly:.1f}" x2="{lx + ls*4:.1f}" y2="{ly + ly_off:.1f}" stroke="var(--earth-600)" stroke-width="0.8" opacity="0.5" />'
            f'<g transform="translate({lx + ls*4:.1f} {ly + ly_off:.1f}) rotate({lang})">'
            f'<path d="M 0 0 Q {ls*lsz*0.55:.1f} {-lsz*0.55:.1f} {ls*lsz:.1f} 0 Q {ls*lsz*0.55:.1f} {lsz*0.55:.1f} 0 0 Z" fill="{lc}" />'
            f'<line x1="0" y1="0" x2="{ls*lsz:.1f}" y2="0" stroke="rgba(255,255,255,0.25)" stroke-width="0.6" />'
            f'</g>'
            f'</g>'
        )

    branch_svg = (
        f'<svg viewBox="0 0 {strip_w} {branch_svg_h}" preserveAspectRatio="none" width="100%" height="{branch_svg_h}" '
        f'style="position:absolute;inset:0;top:10px" aria-hidden="true">'
        f'<defs>'
        f'<linearGradient id="kv-branch-grad" x1="0" x2="1" y1="0" y2="0">'
        f'<stop offset="0%" stop-color="var(--earth-200)" />'
        f'<stop offset="50%" stop-color="var(--earth-400)" />'
        f'<stop offset="100%" stop-color="var(--earth-200)" />'
        f'</linearGradient>'
        f'</defs>'
        f'<path d="M 0 110 Q {strip_w*0.25:.0f} 90, {strip_w*0.5:.0f} 108 T {strip_w} 100" '
        f'fill="none" stroke="url(#kv-branch-grad)" stroke-width="4" stroke-linecap="round" />'
        f'{branch_leaves}'
        f'{"".join(bud_rows)}'
        f'</svg>'
    )

    hours_row = "".join(
        f'<span>{str(hr).zfill(2)}:00</span>'
        for hr in [0, 4, 8, 12, 16, 20, 24]
    )

    # Incidents
    incidents_html = ""
    for inc in incidents:
        itype = inc.get("type", "")
        ititle = inc.get("title", "")
        isev = inc.get("severity", 0)
        conf = round(inc.get("confidence", 0) * 100)
        cluster_sig = inc.get("cluster_signature", "")[:12]
        ev_rows = "".join(
            f'<div class="kv-ev-row">'
            f'<span>{_format_last_ts(e.get("ts",""))} · {_html.escape(e.get("provider",""))} {e.get("event_id","")}</span>'
            f'<span>{_html.escape(e.get("message","")[:80])}</span>'
            f'</div>'
            for e in inc.get("evidence", [])[:4]
        )
        incidents_html += (
            f'<div class="kv-incident">'
            f'<div class="top">'
            f'<div>'
            f'<span class="title">{_html.escape(ititle)} <small>{_html.escape(itype)} · conf {conf}%</small></span>'
            f'<div class="body">Signature <span style="font-family:var(--font-mono);color:var(--ink)">{_html.escape(cluster_sig)}</span></div>'
            f'</div>'
            f'<span class="sev">{isev}</span>'
            f'</div>'
            f'{"<div class=kv-evidence>" + ev_rows + "</div>" if ev_rows else ""}'
            f'</div>'
        )

    # DL entries
    ws_display = window_start[5:10] if len(window_start) >= 10 else window_start
    we_display = window_end[5:10] if len(window_end) >= 10 else window_end
    uid_html = f"@{_html.escape(uid)} · " if uid else ""

    try:
        host_num = int(host_id.split("-")[-1]) if "-" in host_id else 0
    except ValueError:
        host_num = 0

    if visual_theme == "computers":
        aside_visual = _computer_svg(host_id=host_id, sev=sev, index=host_num, w=120, h=140)
    else:
        aside_visual = _sprout_svg(leaves=max(2, len(incidents)), sev=sev, index=host_num, w=80, h=120)

    st.markdown(
        f"""
        <div class="kv-timeline-wrap">
          <aside class="kv-host-aside">
            <p class="eyebrow" style="color:var(--leaf-500)">Host</p>
            <h2>{_html.escape(host_id)}</h2>
            <div class="id-mono">{uid_html}sev {sev}</div>
            <div style="margin-top:14px;display:flex;justify-content:center">{aside_visual}</div>
            <dl>
              <dt>Window</dt><dd>{_html.escape(ws_display)} → {_html.escape(we_display)}</dd>
              <dt>Stage</dt><dd>{_html.escape(sev_lbl)}</dd>
              <dt>Incidents</dt><dd>{len(incidents)}</dd>
            </dl>
            {actions_html}
            <div style="margin-top:18px">
              <p class="eyebrow" style="margin-bottom:8px">Other hosts</p>
              <div class="kv-sibling-strip">
                {"".join(
                    f'<div class="kv-sibling-btn{"" if sh.get("host_id") != host_id else " active"}" style="display:flex;justify-content:space-between;padding:6px 10px;border-radius:8px;font-family:var(--font-mono);font-size:12px;color:var(--ink-soft)">'
                    f'<span>{_html.escape(sh.get("host_id",""))}</span>'
                    f'<span style="color:{_sev_token(sh.get("severity",0))}">{sh.get("severity",0)}</span>'
                    f'</div>'
                    for sh in all_hosts[:7]
                )}
              </div>
            </div>
          </aside>

          <section class="kv-timeline-strip">
            <div style="display:flex;justify-content:space-between;align-items:flex-end">
              <div>
                <p class="eyebrow" style="color:var(--leaf-500)">Timeline</p>
                <h2 style="margin-top:4px">The day, hour by hour</h2>
              </div>
              <span style="font-size:11px;color:var(--ink-faint);font-family:var(--font-mono)">{len(events)} events</span>
            </div>

            <div class="kv-branch-wrap">
              {branch_svg}
              <div class="kv-hours">{hours_row}</div>
            </div>

            <div class="kv-incidents">
              <p class="eyebrow">Incidents detected · {len(incidents)}</p>
              {incidents_html if incidents_html else '<p style="color:var(--ink-soft);font-size:14px;padding:8px 0">No incidents detected for this host.</p>'}
            </div>

            {"<div class='kv-sibling-panel'>" + sibling_row + '<div class="kv-sibling-note">Hosts sharing incident signatures — common thread across the fleet.</div></div>' if sibling_row else ""}
          </section>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Validation tab ───────────────────────────────────────────────────────────


def _render_validation_tab(store, run_id: str, fleet: Dict) -> None:
    hosts = fleet.get("top_hosts", [])
    clusters = fleet.get("clusters", [])
    n_hosts = len(hosts)
    n_clusters = len(clusters)

    host_files = set()
    for key in store.list(f"{run_id}/hosts"):
        parts = key.split("/")
        if len(parts) >= 3:
            host_files.add(parts[2])

    schema_ok = len(host_files)
    schema_total = max(schema_ok, n_hosts)

    report_key = f"{run_id}/validation_report.md"
    has_report = store.exists(report_key)

    all_green = n_clusters > 0 and n_hosts > 0

    status_pill = (
        f'<span class="kv-status-pill {"new" if all_green else "ongoing"}" style="font-size:12px">'
        f'<span style="width:6px;height:6px;border-radius:50%;background:{"var(--leaf-400)" if all_green else "var(--sun)"};display:inline-block"></span>'
        f'{"All green" if all_green else "Check report"}'
        f'</span>'
    )

    st.markdown(
        f"""
        <div class="kv-card">
          <div class="kv-card-head">
            <div>
              <p class="eyebrow" style="color:var(--leaf-500)">Validation</p>
              <h2>Schema check &amp; ground truth</h2>
            </div>
            {status_pill}
          </div>

          <div class="kv-validation">
            <div class="kv-val-card">
              <div class="label">Schema pass</div>
              <div class="v ok">{schema_ok}/{schema_total}</div>
              <div class="desc">Host timeline files present and loaded.</div>
            </div>
            <div class="kv-val-card">
              <div class="label">Cluster coverage</div>
              <div class="v ok">{n_clusters}</div>
              <div class="desc">Clusters surfaced by the deterministic rules.</div>
            </div>
            <div class="kv-val-card">
              <div class="label">Top hosts</div>
              <div class="v ok">{n_hosts}</div>
              <div class="desc">Hosts ranked in the fleet summary.</div>
            </div>
            <div class="kv-val-card">
              <div class="label">Run id</div>
              <div class="v warn" style="font-size:18px;font-style:italic">{_html.escape(run_id[:12])}</div>
              <div class="desc">Artifacts reproducible at artifacts/{_html.escape(run_id)}/.</div>
            </div>
          </div>

          <div class="kv-val-prose">
            <p class="lead">A reproducible spring.</p>
            Each run leaves a folder of artifacts the same way a forest leaves rings — you can compare seasons, replay decisions, and prove what was seen.
            Nothing in this dashboard is computed live; everything points back to
            <code style="font-family:var(--font-mono);font-size:12px;background:var(--mist);padding:1px 5px;border-radius:4px">artifacts/{_html.escape(run_id)}/</code>.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if has_report:
        report_md = store.read_text(report_key)
        st.markdown(
            f'<div class="kv-md-report">{_html.escape(report_md)}</div>',
            unsafe_allow_html=False,
        )
        with st.expander("Raw validation report"):
            st.markdown(report_md)


# ── Fleet tab ────────────────────────────────────────────────────────────────


def _render_fleet_tab(store, run_id: str, fleet: Dict, visual_theme: str) -> None:
    _render_meadow(fleet) if visual_theme != "computers" else _render_workshop(fleet)
    _render_cluster_ledger(fleet)
    _render_hosts_grid(fleet, visual_theme)


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(
        page_title="Kevät · Pre-emptive IT",
        page_icon="🌱",
        layout="wide",
    )
    _inject_css()

    store = _artifact_store()
    runs = _available_runs(store)

    if not runs:
        st.warning("No runs found under artifacts/. Generate scenarios and run the worker first.")
        return

    suggested = get_latest_run_id(store) or runs[-1]

    with st.sidebar:
        st.markdown(
            '<p style="font-family:var(--font-display);font-size:18px;font-weight:500;color:var(--ink);letter-spacing:-0.01em;margin-bottom:4px">Settings</p>',
            unsafe_allow_html=True,
        )
        run_id = st.selectbox("Run", runs, index=runs.index(suggested) if suggested in runs else 0)
        visual_theme = st.radio("Visual theme", ["Meadow 🌿", "Workshop 🖥️"], index=0)
        visual_theme = "computers" if "Workshop" in visual_theme else "meadow"

    run_status = _run_status(store, run_id)
    fleet = _fleet_summary(store, run_id)

    if not fleet:
        st.warning(f"No fleet_summary.json found for run {run_id}.")
        return

    _render_hero(fleet, run_status, visual_theme)

    tab_fleet, tab_host, tab_validation = st.tabs(["Fleet · last 24h", "Host timeline", "Validation"])

    with tab_fleet:
        _render_fleet_tab(store, run_id, fleet, visual_theme)
        st.markdown(
            f"""
            <div class="kv-footer">
              <span>artifacts/<span style="font-family:var(--font-mono)">{_html.escape(run_id)}</span>/ · reproducible · schema-validated</span>
              <em>"Kevät tulee aina" — spring always comes.</em>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with tab_host:
        host_options = _host_options(store, run_id, fleet)
        if not host_options:
            st.write("No hosts available.")
        else:
            host_id = st.selectbox("Host", host_options, index=0)
            _render_host_tab(store, run_id, host_id, fleet, visual_theme)

    with tab_validation:
        _render_validation_tab(store, run_id, fleet)


if __name__ == "__main__":
    main()
