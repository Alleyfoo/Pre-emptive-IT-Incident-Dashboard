"""
Pre-emptive IT Incident Dashboard — public demo.
Entry point for Streamlit Cloud. Runs on synthetic data generated at first load.
"""
import json
import os
import sys
from typing import Dict, List

import pandas as pd
import streamlit as st

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from runtime.artifact_store import build_artifact_store
from runtime.incident_flow import run_incident_flow
from runtime.run_pointer import get_latest_run_id
from tools.generate_ticket_scenarios import ScenarioConfig, ScenarioGenerator

DEMO_RUN_ID = "demo"
ARTIFACTS_ROOT = os.environ.get("ARTIFACTS_ROOT") or os.path.join(REPO_ROOT, "artifacts")


def _store():
    return build_artifact_store(ARTIFACTS_ROOT)


def _bootstrap_demo_data():
    """Generate synthetic scenarios and run the detection flow if not already done."""
    store = _store()
    if store.exists(f"{DEMO_RUN_ID}/fleet_summary.json"):
        return
    config = ScenarioConfig(
        run_id=DEMO_RUN_ID,
        seed=42,
        n_hosts=20,
        days=1,
        scenario_tags=["driver_rollout_wave", "slow_burn", "single_host_hardware"],
    )
    ScenarioGenerator(config=config, artifacts_root=ARTIFACTS_ROOT).generate()
    run_incident_flow(DEMO_RUN_ID, ARTIFACTS_ROOT)


def _regenerate_demo_data():
    """Delete existing demo run and regenerate from scratch."""
    store = _store()
    store.delete_prefix(DEMO_RUN_ID)
    _bootstrap_demo_data()


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


def _run_status(store, run_id: str) -> Dict:
    key = f"{run_id}/run_status.json"
    if store.exists(key):
        return _load_json(store, key)
    return {}


def render_fleet_tab(fleet: Dict):
    type_options = sorted({inc.get("type") for inc in fleet.get("clusters", []) if inc.get("type")})
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_types = st.multiselect("Filter by type", type_options, default=type_options)
    with col2:
        min_hosts = st.number_input("Min affected hosts", min_value=1, value=1, step=1)
    with col3:
        status_filter = st.selectbox("Cluster status", ["all", "new", "ongoing"], index=0)
    min_severity = st.slider("Min severity (hosts)", min_value=0, max_value=100, value=0, step=5)
    host_search = st.text_input("Search host/user", "")

    st.subheader("Overall")
    st.metric(label="Overall risk score", value=fleet.get("overall_risk_score", 0))

    st.subheader("Top impacted hosts")
    hosts = fleet.get("top_hosts", [])
    if host_search:
        hosts = [
            h for h in hosts
            if host_search.lower() in h.get("host_id", "").lower()
            or host_search.lower() in str(h.get("user_id", "")).lower()
        ]
    hosts = [h for h in hosts if h.get("score", 0) >= min_severity]
    if hosts:
        st.dataframe(pd.DataFrame(hosts), use_container_width=True)
    else:
        st.write("No hosts match the current filters.")

    st.subheader("Incident clusters")
    clusters = fleet.get("clusters", [])
    if status_filter != "all":
        clusters = [c for c in clusters if c.get("status") == status_filter]
    if selected_types:
        clusters = [c for c in clusters if c.get("type") in selected_types]
    clusters = [c for c in clusters if c.get("affected_hosts", 0) >= min_hosts]
    if clusters:
        st.dataframe(pd.DataFrame(clusters), use_container_width=True)
    else:
        st.write("No clusters match the current filters.")


def render_host_tab(store, run_id: str, host_id: str):
    timeline = _timeline(store, run_id, host_id)
    if not timeline:
        st.write("No timeline found for this host.")
        return
    window = timeline.get("window") or {
        "start": timeline.get("window_start"),
        "end": timeline.get("window_end"),
    }
    st.subheader(f"Host: {host_id}")
    st.caption(f"Window {window.get('start')} → {window.get('end')}")

    fleet = _fleet_summary(store, run_id)
    host_meta = next((h for h in fleet.get("top_hosts", []) if h.get("host_id") == host_id), {})
    if host_meta:
        st.info(
            f"Action: **{host_meta.get('action', 'n/a')}** | "
            f"Score: {host_meta.get('score')} (Δ {host_meta.get('delta_score')}) | "
            f"Reason: {host_meta.get('action_reason')}"
        )
        if host_meta.get("reasons"):
            st.caption("Score breakdown: " + "; ".join(host_meta["reasons"]))

    cluster_index = {c.get("signature_hash"): c for c in fleet.get("clusters", [])}
    incidents = timeline.get("incidents", [])
    if incidents:
        for inc in incidents:
            with st.expander(f"[{inc.get('severity')}] {inc.get('title')} ({inc.get('type')})"):
                st.write(f"**Confidence:** {inc.get('confidence')}")
                signature_hash = inc.get("signature", {}).get("signature_hash")
                cluster = cluster_index.get(signature_hash)
                if cluster:
                    st.write(f"**Fleet cluster:** {cluster.get('signature_key')} ({cluster.get('status')})")
                st.write(f"**Signature:** {signature_hash}")
                st.write("**Recommended actions:**")
                for action in inc.get("recommended_actions", []):
                    st.write(f"- {action}")
                st.write("**Evidence (sample):**")
                st.json(inc.get("evidence", [])[:3])
    else:
        st.write("No incidents detected for this host.")

    tickets = timeline.get("tickets", [])
    if tickets:
        st.subheader("User tickets")
        st.json(tickets)

    events = timeline.get("events", [])
    if events:
        st.subheader("Recent events (sample)")
        st.dataframe(pd.DataFrame(events).head(50), use_container_width=True)


def main():
    st.set_page_config(
        page_title="Pre-emptive IT Incident Dashboard",
        page_icon="🖥️",
        layout="wide",
    )

    with st.sidebar:
        st.title("Pre-emptive IT")
        st.caption("Demo — synthetic data only")
        st.markdown(
            "This dashboard watches a simulated fleet of 20 Windows endpoints, "
            "detects developing incidents before they become outages, and surfaces "
            "the evidence behind each alert."
        )
        st.divider()
        if st.button("Regenerate demo data", use_container_width=True):
            with st.spinner("Generating synthetic scenarios and running detection…"):
                _regenerate_demo_data()
            st.rerun()
        st.divider()
        st.markdown(
            "[Source on GitHub](https://github.com/Alleyfoo/Pre-emptive-IT-Incident-Dashboard)",
            unsafe_allow_html=False,
        )

    st.title("Pre-emptive IT Incident Dashboard")
    st.caption("All data shown is synthetic — generated deterministically for demonstration purposes.")

    with st.spinner("Loading demo data…"):
        _bootstrap_demo_data()

    store = _store()
    runs = _available_runs(store)
    if not runs:
        st.error("Demo data generation failed. Try clicking 'Regenerate demo data' in the sidebar.")
        return

    suggested = get_latest_run_id(store) or runs[-1]
    run_id = st.selectbox("Run", runs, index=runs.index(suggested) if suggested in runs else 0)

    status = _run_status(store, run_id)
    if status:
        st.info(
            f"Run status: **{status.get('status')}** | "
            f"started {status.get('started_at')} | "
            f"finished {status.get('finished_at')} | "
            f"{status.get('message')}"
        )

    fleet = _fleet_summary(store, run_id)
    if not fleet:
        st.warning(f"No fleet summary found for run `{run_id}`.")
        return

    tab_fleet, tab_host, tab_validation = st.tabs(["Fleet — last 24 h", "Host timeline", "Validation report"])

    with tab_fleet:
        render_fleet_tab(fleet)

    with tab_host:
        host_options = _host_options(store, run_id, fleet)
        if not host_options:
            st.write("No hosts available.")
        else:
            host_id = st.selectbox("Host", host_options, index=0)
            render_host_tab(store, run_id, host_id)

    with tab_validation:
        report_key = f"{run_id}/validation_report.md"
        if store.exists(report_key):
            st.markdown(store.read_text(report_key))
        else:
            st.write("No validation report found for this run.")


if __name__ == "__main__":
    main()
