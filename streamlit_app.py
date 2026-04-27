from __future__ import annotations

from datetime import date, datetime
import hashlib
from io import BytesIO
import json
import math
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st


st.set_page_config(page_title="?? ???? ????", layout="wide")


TEAMS_DEFAULT_WEBHOOK = "https://defaulte2d70a05f3524e9d8c182194f1d9ef.31.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/98f10010be974d57a6f4065239b83ca4/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=tVbGSnsTMHbildXcbLsoBQj_WXrvSSOLnqktQNSDFBM"
DEFAULT_GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1KmsdyvfHJEOXnZvGtUl1TEhWz0JzJ3NWkg3Amb8l91U/edit"
WORK_DIR = Path(__file__).resolve().parent
LOG_DIR = WORK_DIR / "logs"
LATEST_UPLOAD_INFO_PATH = LOG_DIR / "latest_sheet_upload.json"
SHEET_SYNC_HISTORY_PATH = LOG_DIR / "sheet_sync_history.json"
COMPLETION_HISTORY_PATH = LOG_DIR / "completion_history.json"
DASHBOARD_STATE_PATH = LOG_DIR / "dashboard_state.json"
UPLOAD_INFO_WORKSHEET_NAME = "DASHBOARD_UPLOAD_INFO"
SYNC_HISTORY_WORKSHEET_NAME = "DASHBOARD_SYNC_HISTORY"
KST = ZoneInfo("Asia/Seoul")
STREAMLIT_APP_REVISION = "2026-04-27 16:05"
BORING_WORKSHEET_GID_BY_SYNC_TIME = {
    "2026-04-23 10:15:08": "1062250441",
    "2026-04-24 10:56:07": "1321320597",
}


BORING_MACHINE_CONFIG = [
    {"line": "??", "machine": "?? #1", "installDate": "2026-03-01"},
    {"line": "??", "machine": "?? #2", "installDate": "2026-03-04"},
    {"line": "??", "machine": "?? #3", "installDate": "2026-03-02"},
    {"line": "???", "machine": "??? #3", "installDate": "2026-02-28", "actionStep": ""},
    {"line": "??", "machine": "?? #26", "installDate": "2026-03-03"},
    {"line": "??", "machine": "?? #27", "installDate": "2026-03-05"},
    {"line": "??", "machine": "?? #19", "installDate": "2026-02-22"},
    {"line": "??", "machine": "?? #20", "installDate": "2026-03-06"},
    {"line": "??", "machine": "?? #21", "installDate": "2026-03-01"},
    {"line": "??", "machine": "?? #22", "installDate": "2026-03-04"},
    {"line": "??", "machine": "?? #23", "installDate": "2026-03-05"},
    {"line": "??", "machine": "?? #24", "installDate": "2026-03-02"},
]

BORING_BLADE_SPECS = [
    {"suffix": "035", "bladeName": "?35 ??", "standard": 10000, "avg7d": 420, "quality": 0, "spindle": "H1"},
    {"suffix": "020", "bladeName": "?20 ??", "standard": 10000, "avg7d": 320, "quality": 0, "spindle": "H2"},
    {"suffix": "012", "bladeName": "?12(??) ??", "standard": 10000, "avg7d": 410, "quality": 0, "spindle": "H3"},
    {"suffix": "008", "bladeName": "?8(??) ??", "standard": 10000, "avg7d": 355, "quality": 0, "spindle": "MAIN"},
    {"suffix": "015", "bladeName": "?15 ??", "standard": 10000, "avg7d": 300, "quality": 0, "spindle": "H4"},
    {"suffix": "005", "bladeName": "?5(??) ??", "standard": 10000, "avg7d": 280, "quality": 0, "spindle": "H5"},
]

BORING_HISTORY_BLADE_COLUMNS = [
    "?5(??) ??",
    "?8(??) ??",
    "?12(??) ??",
    "?15 ??",
    "?20 ??",
    "?35 ??",
]

BORING_HISTORY_BLADE_NAMES = [
    "?5(??) ??",
    "?8(??) ??",
    "?12(??) ??",
    "?15 ??",
    "?20 ??",
    "?35 ??",
]

EDGE_MACHINE_DEFAULTS = [
    {"line": "??", "machine": "?? #1", "spindle": "H1", "bladeCode": "AT-013-B", "bladeName": "AT ??(??)", "installDate": "2026-03-03", "usage": 0, "standard": 15000, "avg7d": 2000, "quality": 0},
    {"line": "??", "machine": "?? #2", "spindle": "H2", "bladeCode": "AT-014-B", "bladeName": "AT ??(??)", "installDate": "2026-03-05", "usage": 0, "standard": 15000, "avg7d": 2000, "quality": 0},
    {"line": "??", "machine": "?? #3,4", "spindle": "H1/H3", "bladeCode": "AT-015-016-B", "bladeName": "AT ??(??)", "installDate": "2026-03-06", "usage": 0, "standard": 90000, "avg7d": 15000, "quality": 0},
    {"line": "??", "machine": "?? #5", "spindle": "H2", "bladeCode": "AT-017-B", "bladeName": "AT ??(??)", "installDate": "2026-02-27", "usage": 0, "standard": 15000, "avg7d": 2125, "quality": 0},
    {"line": "??", "machine": "?? #6", "spindle": "MAIN-F", "bladeCode": "AT-018-F", "bladeName": "AT ??(??)", "installDate": "2026-03-26", "usage": 0, "standard": 75000, "avg7d": 10000, "quality": 0, "actionStep": ""},
    {"line": "??", "machine": "?? #6", "spindle": "MAIN-B", "bladeCode": "AT-018-B", "bladeName": "AT ??(??)", "installDate": "2026-03-26", "usage": 0, "standard": 75000, "avg7d": 10000, "quality": 0, "actionStep": ""},
]


def build_initial_raw_data() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    row_id = 1
    line_prefix = {"??": "V", "???": "P", "??": "D", "??": "R"}
    for machine_config in BORING_MACHINE_CONFIG:
        for blade_spec in BORING_BLADE_SPECS:
            standard = blade_spec["standard"]
            if machine_config["line"] == "??" and blade_spec["bladeName"] == "?5(??) ??":
                standard = 50000
            row = {
                "id": row_id,
                "plant": "??",
                "line": machine_config["line"],
                "machine": machine_config["machine"],
                "spindle": blade_spec["spindle"],
                "bladeCode": f"{line_prefix[machine_config['line']]}-{blade_spec['suffix']}",
                "bladeName": blade_spec["bladeName"],
                "installDate": machine_config["installDate"],
                "usage": 0,
                "standard": standard,
                "avg7d": blade_spec["avg7d"],
                "quality": blade_spec["quality"],
            }
            if "actionStep" in machine_config:
                row["actionStep"] = machine_config["actionStep"]
            rows.append(row)
            row_id += 1

    for edge_config in EDGE_MACHINE_DEFAULTS:
        rows.append({"id": row_id, "plant": "??", **edge_config})
        row_id += 1
    return rows


def now_kst() -> datetime:
    return datetime.now(KST)


def extract_sync_time_from_text(value: Any) -> str:
    raw = str(value or "").strip()
    match = __import__("re").search(r"grd_List_(\d{14})", raw, __import__("re").IGNORECASE)
    if not match:
        return now_kst().strftime("%Y-%m-%d %H:%M:%S")
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=KST)
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return now_kst().strftime("%Y-%m-%d %H:%M:%S")


def normalize_history_date_value(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            return raw
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return raw


def normalize_display_timestamp(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    try:
        parsed_iso = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed_iso.tzinfo is None:
            return parsed_iso.strftime("%Y-%m-%d %H:%M:%S")
        return parsed_iso.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return raw


INITIAL_RAW_DATA = build_initial_raw_data()


def equipment_row_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("machine", "")).strip(), str(row.get("bladeCode", row.get("bladeName", ""))).strip())


def ensure_default_equipment_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    legacy_edge_group_machines = {"?ｌ? #3", "?ｌ? #4"}
    existing_rows = [
        row
        for row in data
        if (
            isinstance(row, dict)
            and str(row.get("machine", "")).strip() not in legacy_edge_group_machines
            and not (
                str(row.get("line", "")).strip() == "?ｌ?"
                and str(row.get("bladeName", "")).strip() == "AT ?좊Ъ"
            )
        )
    ]
    existing_map = {equipment_row_key(row): row for row in existing_rows if equipment_row_key(row) != ("", "")}
    merged_rows: list[dict[str, Any]] = []

    for default_row in INITIAL_RAW_DATA:
        key = equipment_row_key(default_row)
        existing_row = existing_map.get(key, {})
        merged_row = {**default_row, **existing_row}
        for locked_field in ["line", "machine", "spindle", "bladeCode", "bladeName", "standard"]:
            merged_row[locked_field] = default_row.get(locked_field)
        merged_rows.append(merged_row)

    known_keys = {equipment_row_key(row) for row in INITIAL_RAW_DATA}
    for row in existing_rows:
        key = equipment_row_key(row)
        if key not in known_keys:
            merged_rows.append(row)

    for index, row in enumerate(merged_rows, start=1):
        row["id"] = index
    return merged_rows


def reset_all_usage_data() -> None:
    default_map = {equipment_row_key(row): row for row in INITIAL_RAW_DATA}
    reset_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    next_rows: list[dict[str, Any]] = []
    for row in st.session_state.get("equipment_data", []):
        key = equipment_row_key(row)
        default_row = default_map.get(key, {})
        next_rows.append(
            {
                **row,
                "usage": 0,
                "quality": 0,
                "avg7d": default_row.get("avg7d", row.get("avg7d", 0)),
                "installDate": default_row.get("installDate", row.get("installDate", "")),
                "actionStep": "",
            }
        )
    st.session_state.equipment_data = next_rows
    st.session_state.replace_alert_history = {}
    st.session_state.last_sheet_sync_details = []
    st.session_state.last_sheet_sync_at = ""
    st.session_state.last_applied_upload_at = st.session_state.get("auto_sheet_updated_at", "")
    st.session_state.usage_reset_at = reset_at
    st.session_state.machine_reset_at = {}
    st.session_state.upload_summary = None
    st.session_state.send_result = "?ㅻ퉬 ?ъ슜瑜좎쓣 紐⑤몢 由ъ뀑?덉뒿?덈떎."
    save_dashboard_state()


def reset_last_sheet_sync_result() -> None:
    st.session_state.last_sheet_sync_details = []
    st.session_state.last_sheet_sync_at = ""
    st.session_state.send_result = "?곗씠??諛섏쁺 ?대젰??由ъ뀑?덉뒿?덈떎."
    save_dashboard_state()


def reset_sheet_sync_history_data() -> None:
    st.session_state.sheet_sync_history = []
    save_sheet_sync_history([])
    st.session_state.send_result = "?곗씠??諛섏쁺 ?대젰??由ъ뀑?덉뒿?덈떎."
    save_dashboard_state()


def reset_completion_history_data() -> None:
    st.session_state.completion_history = []
    save_completion_history([])
    st.session_state.send_result = "援먯껜?꾨즺 ?쒖젏??由ъ뀑?덉뒿?덈떎."
    save_dashboard_state()


EDGE_UPLOAD_RULES = {
    "?ｌ? #1": {"periodDays": 15},
    "?ｌ? #2": {"periodDays": 15},
    "?ｌ? #3,4": {"periodDays": 7},
    "?ｌ? #5": {"periodDays": 15},
    "?ｌ? #6": {"periodDays": 7},
}

MACHINE_GROUPS = {
    "?ｌ? ?꾩껜": ["?ｌ? #1", "?ｌ? #2", "?ｌ? #3,4", "?ｌ? #5", "?ｌ? #6"],
    "蹂대쭅 ?꾩껜": [
        "?섏쭅 #1",
        "?섏쭅 #2",
        "?섏쭅 #3",
        "?ъ씤??#3",
        "?묐㈃ #26",
        "?묐㈃ #27",
        "?곕떇 #19",
        "?곕떇 #20",
        "?곕떇 #21",
        "?곕떇 #22",
        "?곕떇 #23",
        "?곕떇 #24",
    ],
}

EDGE_FIXED_STANDARDS = {
    "?ｌ? #1": 15000,
    "?ｌ? #2": 15000,
    "?ｌ? #3,4": 90000,
    "?ｌ? #5": 15000,
    "?ｌ? #6": 75000,
}

STATUS_META = {
    "normal": {"label": "?뺤긽", "color": "green"},
    "caution": {"label": "二쇱쓽", "color": "orange"},
    "replace": {"label": "援먯껜", "color": "red"},
}

STATUS_STYLES = {
    "normal": {
        "badge_bg": "#ecfdf5",
        "badge_text": "#047857",
        "badge_border": "#a7f3d0",
        "bar": "#10b981",
    },
    "caution": {
        "badge_bg": "#fff7ed",
        "badge_text": "#c2410c",
        "badge_border": "#fdba74",
        "bar": "#f59e0b",
    },
    "replace": {
        "badge_bg": "#fff1f2",
        "badge_text": "#be123c",
        "badge_border": "#fda4af",
        "bar": "#f43f5e",
    },
}

LINE_FILTER_ORDER = ["??", "??", "??", "???", "??"]
LINE_MACHINE_OPTIONS = {
    "??": ["?? #1", "?? #2", "?? #3,4", "?? #5", "?? #6"],
    "??": ["?? #19", "?? #20", "?? #21", "?? #22", "?? #23", "?? #24"],
    "??": ["?? #26", "?? #27"],
    "???": ["??? #3"],
    "??": ["?? #1", "?? #2", "?? #3"],
}


def init_state() -> None:
    saved_state = load_dashboard_state()
    if "equipment_data" not in st.session_state:
        raw_equipment = saved_state.get("equipment_data", INITIAL_RAW_DATA.copy())
        st.session_state.equipment_data = ensure_default_equipment_rows(raw_equipment if isinstance(raw_equipment, list) else INITIAL_RAW_DATA.copy())
    if "send_result" not in st.session_state:
        st.session_state.send_result = saved_state.get("send_result", "")
    if "replace_alert_history" not in st.session_state:
        st.session_state.replace_alert_history = saved_state.get("replace_alert_history", {})
    if "upload_summary" not in st.session_state:
        st.session_state.upload_summary = None
    if "last_sheet_sync_at" not in st.session_state:
        st.session_state.last_sheet_sync_at = saved_state.get("last_sheet_sync_at", "")
    if "last_sheet_sync_details" not in st.session_state:
        raw_details = saved_state.get("last_sheet_sync_details", [])
        st.session_state.last_sheet_sync_details = normalize_last_sheet_sync_details(raw_details if isinstance(raw_details, list) else [])
    if "sheet_sync_history" not in st.session_state:
        raw_history = load_sheet_sync_history()
        st.session_state.sheet_sync_history = normalize_sheet_sync_history(raw_history if isinstance(raw_history, list) else [])
    if "completion_history" not in st.session_state:
        raw_completion = saved_state.get("completion_history", load_completion_history())
        st.session_state.completion_history = raw_completion if isinstance(raw_completion, list) else []
    if "machine_reset_at" not in st.session_state:
        raw_machine_reset_at = saved_state.get("machine_reset_at", {})
        st.session_state.machine_reset_at = raw_machine_reset_at if isinstance(raw_machine_reset_at, dict) else {}
    if "sheet_sync_hashes" not in st.session_state:
        st.session_state.sheet_sync_hashes = saved_state.get("sheet_sync_hashes", {})
    if "teams_webhook_url" not in st.session_state:
        st.session_state.teams_webhook_url = saved_state.get("teams_webhook_url", TEAMS_DEFAULT_WEBHOOK)
    if "auto_sheet_url" not in st.session_state:
        latest_info = load_latest_upload_info()
        st.session_state.auto_sheet_url = latest_info.get("spreadsheet_url", saved_state.get("auto_sheet_url", DEFAULT_GOOGLE_SHEET_URL))
        st.session_state.auto_sheet_name = latest_info.get("worksheet_title", saved_state.get("auto_sheet_name", ""))
        st.session_state.auto_sheet_gid = latest_info.get("worksheet_gid", saved_state.get("auto_sheet_gid", ""))
        st.session_state.auto_sheet_updated_at = latest_info.get("updated_at", saved_state.get("auto_sheet_updated_at", ""))
    if "auto_sheet_gid" not in st.session_state:
        latest_info = load_latest_upload_info()
        st.session_state.auto_sheet_gid = latest_info.get("worksheet_gid", saved_state.get("auto_sheet_gid", ""))
    if "last_applied_upload_at" not in st.session_state:
        st.session_state.last_applied_upload_at = saved_state.get("last_applied_upload_at", "")
    if "last_snapshot_sync_key" not in st.session_state:
        st.session_state.last_snapshot_sync_key = saved_state.get("last_snapshot_sync_key", "")
    if "boring_snapshot_loaded_key" not in st.session_state:
        st.session_state.boring_snapshot_loaded_key = saved_state.get("boring_snapshot_loaded_key", "")
    if "usage_reset_at" not in st.session_state:
        st.session_state.usage_reset_at = saved_state.get("usage_reset_at", "")
    if "line_filter_toggle" not in st.session_state:
        st.session_state.line_filter_toggle = saved_state.get("line_filter_toggle", "all")
    if "line_machine_filter" not in st.session_state:
        st.session_state.line_machine_filter = saved_state.get("line_machine_filter", "?꾩껜")
    st.session_state.equipment_data = reconcile_edge_usage_from_history(
        st.session_state.equipment_data,
        st.session_state.sheet_sync_history,
        st.session_state.get("usage_reset_at", ""),
    )
    st.session_state.equipment_data = reconcile_boring_usage_from_history(
        st.session_state.equipment_data,
        st.session_state.sheet_sync_history,
        st.session_state.get("usage_reset_at", ""),
    )


def get_status(rate: float, quality: int) -> str:
    if rate >= 1:
        return "replace"
    if rate >= 0.6:
        return "caution"
    return "normal"


def days_left(remaining: float, avg7d: float) -> int:
    if avg7d <= 0:
        return 999
    return max(0, math.ceil(remaining / avg7d))


def format_cycle_value(row: dict[str, Any], value: float) -> str:
    if row["line"] == "??":
        return f"{round(value):,}m"
    return f"{value:,.0f} ?"

def get_display_blade_name(row: dict[str, Any]) -> str:
    if row["line"] == "??":
        blade_name = str(row.get("bladeName", "")).strip()
        if blade_name:
            return blade_name
        return "AT ??(??)"
    if "(??)" in row["bladeName"]:
        return row["bladeName"]
    if any(token in row["bladeName"] for token in ["?5", "?8", "?12"]):
        return row["bladeName"].replace(" ??", "") + "(??) ??"
    return row["bladeName"]

def get_machine_blade_summary(machine: str, rows: list[dict[str, Any]] | None = None) -> str:
    blade_names: list[str] = []
    source_rows = rows if rows is not None else st.session_state.get("equipment_data", INITIAL_RAW_DATA)
    for row in source_rows:
        if str(row.get("machine", "")).strip() != machine:
            continue
        blade_name = get_display_blade_name(row)
        if blade_name not in blade_names:
            blade_names.append(blade_name)
    return ", ".join(blade_names)


def get_history_blade_list(machine: str, rows: list[dict[str, Any]] | None = None) -> list[str]:
    normalized_machine = normalize_machine_name(machine)
    if normalized_machine.startswith(("??", "??", "???", "??")):
        return [
            "?35 ??",
            "?20 ??",
            "?12(??) ??",
            "?8(??) ??",
            "?15 ??",
            "?5(??) ??",
        ]
    if normalized_machine == "?ｌ? #6":
        return ["AT ?좊Ъ(?꾨㈃)", "AT ?좊Ъ(?꾨㈃)"]
    if normalized_machine.startswith("?ｌ?"):
        return ["AT ?좊Ъ(?꾨㈃)"]
    return []


def get_machine_sort_key(machine: str) -> tuple[int, int]:
    normalized_machine = normalize_machine_name(machine)
    line_order = {"??": 0, "??": 1, "??": 2, "???": 3, "??": 4}
    line_name = infer_line_from_machine(normalized_machine)
    digits = "".join(ch for ch in normalized_machine if ch.isdigit())
    machine_no = int(digits) if digits else 999
    return line_order.get(line_name, 99), machine_no


def get_blade_sort_key(blade_name: str) -> int:
    preferred_order = [
        "過35 ?좊Ъ",
        "過20 ?좊Ъ",
        "過12(愿?? ?좊Ъ",
        "過8(愿?? ?좊Ъ",
        "過15 ?좊Ъ",
        "過5(愿?? ?좊Ъ",
        "AT ?좊Ъ(?꾨㈃)",
        "AT ?좊Ъ(?꾨㈃)",
    ]
    try:
        return preferred_order.index(str(blade_name).strip())
    except ValueError:
        return len(preferred_order)


def normalize_edge_blade_name(machine: str, blade_name: Any) -> str:
    normalized_machine = normalize_machine_name(machine)
    raw_blade_name = str(blade_name or "").strip()
    if normalized_machine == "?ｌ? #6":
        if "?꾨㈃" in raw_blade_name:
            return "AT ?좊Ъ(?꾨㈃)"
        if "?꾨㈃" in raw_blade_name:
            return "AT ?좊Ъ(?꾨㈃)"
        return raw_blade_name
    if normalized_machine.startswith("?ｌ?"):
        return "AT ?좊Ъ(?꾨㈃)"
    return raw_blade_name


def load_latest_upload_info() -> dict[str, str]:
    local_info: dict[str, str] = {}
    if LATEST_UPLOAD_INFO_PATH.exists():
        try:
            local_info = json.loads(LATEST_UPLOAD_INFO_PATH.read_text(encoding="utf-8"))
        except Exception:
            local_info = {}

    remote_info = load_latest_upload_info_from_sheet()
    if remote_info.get("updated_at"):
        if not local_info.get("updated_at") or remote_info["updated_at"] >= local_info.get("updated_at", ""):
            return remote_info
    return local_info


def load_latest_upload_info_from_sheet() -> dict[str, str]:
    try:
        csv_url = to_google_sheet_csv_url(DEFAULT_GOOGLE_SHEET_URL, worksheet_name=UPLOAD_INFO_WORKSHEET_NAME)
        session = requests.Session()
        session.trust_env = False
        response = session.get(csv_url, timeout=15)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content))
        if df.empty:
            return {}
        df.columns = [str(col).replace("\ufeff", "").strip() for col in df.columns]
        row = df.iloc[0].fillna("")
        return {
            "spreadsheet_name": str(row.get("spreadsheet_name", "")).strip(),
            "spreadsheet_url": str(row.get("spreadsheet_url", "")).strip(),
            "worksheet_title": str(row.get("worksheet_title", "")).strip(),
            "worksheet_gid": str(row.get("worksheet_gid", "")).strip(),
            "erp_file_name": str(row.get("erp_file_name", "")).strip(),
            "dataset_type": str(row.get("dataset_type", "")).strip(),
            "updated_at": str(row.get("updated_at", "")).strip(),
        }
    except Exception:
        return {}


def load_sheet_sync_history() -> list[dict[str, Any]]:
    remote_history = load_sheet_sync_history_from_sheet()
    local_history: list[dict[str, Any]] = []
    if not SHEET_SYNC_HISTORY_PATH.exists():
        return normalize_sheet_sync_history(remote_history)
    try:
        data = json.loads(SHEET_SYNC_HISTORY_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list):
            local_history = normalize_sheet_sync_history(data)
            if local_history != data:
                save_sheet_sync_history(local_history)
    except Exception:
        local_history = []
    if remote_history or local_history:
        return merge_sheet_sync_history(local_history, remote_history)
    return []


def load_sheet_sync_history_from_sheet() -> list[dict[str, Any]]:
    try:
        csv_url = to_google_sheet_csv_url(DEFAULT_GOOGLE_SHEET_URL, worksheet_name=SYNC_HISTORY_WORKSHEET_NAME)
        session = requests.Session()
        session.trust_env = False
        response = session.get(csv_url, timeout=15)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content))
        if df.empty:
            return []
        df.columns = [str(col).replace("\ufeff", "").strip() for col in df.columns]
        records = df.fillna("").to_dict(orient="records")
        return normalize_sheet_sync_history(records)
    except Exception:
        return []


def save_sheet_sync_history(history: list[dict[str, Any]]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    normalized = normalize_sheet_sync_history(history)
    SHEET_SYNC_HISTORY_PATH.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")


def load_completion_history() -> list[dict[str, Any]]:
    if not COMPLETION_HISTORY_PATH.exists():
        return []
    try:
        data = json.loads(COMPLETION_HISTORY_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_completion_history(history: list[dict[str, Any]]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    COMPLETION_HISTORY_PATH.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_sheet_sync_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for entry in history:
        if not isinstance(entry, dict):
            continue
        raw_machine = str(entry.get("??", entry.get("???", ""))).strip()
        target = str(entry.get("??", entry.get("????", ""))).strip()
        usage_m_key = "?? ???(m)" if "?? ???(m)" in entry else "??? ?????m)"
        usage_count_key = "?? ???(?)" if "?? ???(?)" in entry else "??? ???????"
        sync_at = entry.get("????", entry.get("??????", ""))
        start_date = entry.get("???", entry.get("?????", ""))
        blade_name = entry.get("???", entry.get("?????", ""))
        usage_m = entry.get(usage_m_key, "")
        usage_count = entry.get(usage_count_key, "")
        if not raw_machine and not target:
            continue
        machine = normalize_machine_name(raw_machine)
        if not machine:
            continue

        is_boring = machine.startswith(("??", "???", "??", "??"))
        is_edge = machine.startswith(("??",))

        if is_boring:
            target = "?? ??"
            usage_m = ""
        elif is_edge:
            target = "?? ??"
            usage_count = ""
            blade_name = normalize_edge_blade_name(machine, blade_name)
        if is_edge and not str(blade_name).strip():
            blade_name = get_machine_blade_summary(machine, INITIAL_RAW_DATA)

        normalized.append(
            {
                "????": str(sync_at).strip(),
                "??": target,
                "??": machine,
                "???": blade_name,
                "?? ???(m)": usage_m,
                "?? ???(?)": usage_count,
                "???": start_date,
            }
        )
    return normalized

def merge_sheet_sync_history(existing_history: list[dict[str, Any]], new_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing_rows = normalize_sheet_sync_history(existing_history if isinstance(existing_history, list) else [])
    new_rows = normalize_sheet_sync_history(new_entries if isinstance(new_entries, list) else [])
    if not new_rows:
        return existing_rows

    key_columns = ["????", "??", "??", "???"]
    replacement_keys = {
        tuple(str(row.get(column, "")).strip() for column in key_columns)
        for row in new_rows
    }
    if replacement_keys:
        existing_rows = [
            row
            for row in existing_rows
            if tuple(str(row.get(column, "")).strip() for column in key_columns) not in replacement_keys
        ]

    history_df = pd.DataFrame([*existing_rows, *new_rows])
    if history_df.empty:
        return []

    history_df["_sort_time"] = pd.to_datetime(history_df["????"], errors="coerce")
    history_df = history_df.sort_values(by=["_sort_time", "????", "??", "???"], ascending=[True, True, True, True], na_position="last")
    history_df = history_df.drop(columns=["_sort_time"], errors="ignore")
    return history_df.to_dict(orient="records")

def normalize_last_sheet_sync_details(details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for entry in details:
        if not isinstance(entry, dict):
            continue
        machine = normalize_machine_name(str(entry.get("machine", entry.get("??", entry.get("???", "")))).strip())
        blade_name = entry.get("blade_name", entry.get("???", entry.get("?????", "")))
        usage_m = entry.get("usage_m", entry.get("?? ???(m)", entry.get("??? ?????m)", "")))
        usage_count = entry.get("usage_count", entry.get("?? ???(?)", entry.get("??? ???????", "")))
        start_date = entry.get("start_date", entry.get("???", entry.get("?????", "")))

        is_boring = machine.startswith(("??", "???", "??", "??"))
        is_edge = machine.startswith(("??",))

        if is_boring:
            usage_m = ""
        elif is_edge:
            usage_count = ""
            blade_name = normalize_edge_blade_name(machine, blade_name)
        if is_edge and not str(blade_name).strip():
            blade_name = get_machine_blade_summary(machine, INITIAL_RAW_DATA)

        normalized.append(
            {
                "machine": machine,
                "blade_name": blade_name,
                "usage_m": usage_m,
                "usage_count": usage_count,
                "start_date": start_date,
            }
        )
    return normalized

def restore_last_sync_result_from_history() -> bool:
    history = normalize_sheet_sync_history(st.session_state.get("sheet_sync_history", []))
    if not history:
        return False

    latest_sync_at = normalize_display_timestamp(history[0].get("????", ""))
    if not latest_sync_at:
        return False

    latest_entries: list[dict[str, Any]] = []
    for entry in history:
        if normalize_display_timestamp(entry.get("????", "")) != latest_sync_at:
            break
        latest_entries.append(entry)

    if not latest_entries:
        return False

    st.session_state.last_sheet_sync_at = latest_sync_at
    st.session_state.last_sheet_sync_details = normalize_last_sheet_sync_details(
        [
            {
                "machine": entry.get("??", ""),
                "blade_name": entry.get("???", ""),
                "usage_m": entry.get("?? ???(m)", ""),
                "usage_count": entry.get("?? ???(?)", ""),
                "start_date": entry.get("???", ""),
            }
            for entry in latest_entries
        ]
    )
    save_dashboard_state()
    return True

def center_align_dataframe(df: pd.DataFrame):
    return df.style.set_properties(**{"text-align": "center"}).set_table_styles(
        [
            {"selector": "th", "props": [("text-align", "center")]},
            {"selector": "td", "props": [("text-align", "center")]},
        ]
    )


def format_sync_display_dataframe(df: pd.DataFrame):
    display_df = df.copy()
    if "諛섏쁺 ?ъ슜??m)" in display_df.columns:
        display_df["諛섏쁺 ?ъ슜??m)"] = display_df["諛섏쁺 ?ъ슜??m)"].apply(
            lambda value: "" if value in ("", None) or pd.isna(value) else f"{float(value):.2f}".rstrip("0").rstrip(".")
        )
    if "諛섏쁺 ?ъ슜????" in display_df.columns:
        display_df["諛섏쁺 ?ъ슜????"] = display_df["諛섏쁺 ?ъ슜????"].apply(
            lambda value: "" if value in ("", None) or pd.isna(value) else str(int(round(float(value))))
        )
    return display_df.style.hide(axis="index").set_properties(**{"text-align": "center"}).set_table_styles(
        [
            {"selector": "th", "props": [("text-align", "center")]},
            {"selector": "td", "props": [("text-align", "center")]},
        ]
    )


def load_dashboard_state() -> dict[str, Any]:
    if not DASHBOARD_STATE_PATH.exists():
        return {}
    try:
        data = json.loads(DASHBOARD_STATE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_dashboard_state() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "equipment_data": st.session_state.get("equipment_data", INITIAL_RAW_DATA),
        "send_result": st.session_state.get("send_result", ""),
        "replace_alert_history": st.session_state.get("replace_alert_history", {}),
        "last_sheet_sync_at": st.session_state.get("last_sheet_sync_at", ""),
        "last_sheet_sync_details": normalize_last_sheet_sync_details(st.session_state.get("last_sheet_sync_details", [])),
        "sheet_sync_history": normalize_sheet_sync_history(st.session_state.get("sheet_sync_history", [])),
        "completion_history": st.session_state.get("completion_history", []),
        "sheet_sync_hashes": st.session_state.get("sheet_sync_hashes", {}),
        "teams_webhook_url": st.session_state.get("teams_webhook_url", TEAMS_DEFAULT_WEBHOOK),
        "auto_sheet_url": st.session_state.get("auto_sheet_url", DEFAULT_GOOGLE_SHEET_URL),
        "auto_sheet_name": st.session_state.get("auto_sheet_name", ""),
        "auto_sheet_gid": st.session_state.get("auto_sheet_gid", ""),
        "auto_sheet_updated_at": st.session_state.get("auto_sheet_updated_at", ""),
        "last_applied_upload_at": st.session_state.get("last_applied_upload_at", ""),
        "last_snapshot_sync_key": st.session_state.get("last_snapshot_sync_key", ""),
        "boring_snapshot_loaded_key": st.session_state.get("boring_snapshot_loaded_key", ""),
        "usage_reset_at": st.session_state.get("usage_reset_at", ""),
        "machine_reset_at": st.session_state.get("machine_reset_at", {}),
        "line_filter_toggle": st.session_state.get("line_filter_toggle", "all"),
        "line_machine_filter": st.session_state.get("line_machine_filter", "?꾩껜"),
    }
    DASHBOARD_STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def reconcile_edge_usage_from_history(data: list[dict[str, Any]], history: list[dict[str, Any]], reset_at: str = "") -> list[dict[str, Any]]:
    aggregated: dict[tuple[str, str], dict[str, Any]] = {}
    machine_reset_at = st.session_state.get("machine_reset_at", {})
    for entry in history:
        sync_at = str(entry.get("????", entry.get("??", ""))).strip()
        if reset_at and sync_at and sync_at <= reset_at:
            continue
        machine = normalize_machine_name(str(entry.get("??", entry.get("???", ""))).strip())
        machine_cutoff = str(machine_reset_at.get(machine, "")).strip()
        if machine_cutoff and sync_at and sync_at <= machine_cutoff:
            continue
        blade_name = str(entry.get("???", entry.get("?????", ""))).strip()
        usage_m = parse_numeric_value(entry.get("?? ???(m)", entry.get("??? ?????m)", 0)))
        start_date = str(entry.get("???", entry.get("?????", ""))).strip()
        if not machine or usage_m <= 0:
            continue
        key = (machine, blade_name)
        aggregated.setdefault(key, {"usage": 0.0, "start_date": ""})
        aggregated[key]["usage"] += usage_m
        if start_date:
            current_start = aggregated[key]["start_date"]
            aggregated[key]["start_date"] = min(current_start, start_date) if current_start else start_date

    next_rows: list[dict[str, Any]] = []
    for item in data:
        key = (str(item.get("machine", "")).strip(), get_display_blade_name(item))
        if item.get("line") != "??" or key not in aggregated:
            next_rows.append(item)
            continue
        total_usage = round(aggregated[key]["usage"], 3)
        period_days = EDGE_UPLOAD_RULES.get(item["machine"], {"periodDays": 7})["periodDays"]
        next_rows.append(
            {
                **item,
                "usage": total_usage,
                "standard": EDGE_FIXED_STANDARDS.get(item["machine"], item["standard"]),
                "avg7d": max(1, round(total_usage / period_days, 3)),
                "installDate": aggregated[key]["start_date"] or item.get("installDate", ""),
            }
        )
    return next_rows

def reconcile_boring_usage_from_history(data: list[dict[str, Any]], history: list[dict[str, Any]], reset_at: str = "") -> list[dict[str, Any]]:
    boring_entries = []
    machine_reset_at = st.session_state.get("machine_reset_at", {})
    for entry in history:
        sync_at = str(entry.get("????", entry.get("??", ""))).strip()
        if reset_at and sync_at and sync_at <= reset_at:
            continue
        machine = normalize_machine_name(str(entry.get("??", entry.get("???", ""))).strip())
        machine_cutoff = str(machine_reset_at.get(machine, "")).strip()
        if machine_cutoff and sync_at and sync_at <= machine_cutoff:
            continue
        if infer_line_from_machine(machine) in {"", "엣지"}:
            continue
        blade_name = normalize_boring_blade_name(str(entry.get("???", entry.get("?????", ""))).strip())
        usage_count = parse_numeric_value(entry.get("?? ???(?)", entry.get("??? ???", 0)))
        start_date = str(entry.get("???", entry.get("?????", ""))).strip()
        if not machine or not blade_name:
            continue
        boring_entries.append(
            {
                "sync_at": sync_at,
                "machine": machine,
                "blade_name": blade_name,
                "usage_count": usage_count,
                "start_date": start_date,
            }
        )

    if not boring_entries:
        return data

    aggregated: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in boring_entries:
        key = (entry["machine"], entry["blade_name"])
        aggregated.setdefault(key, {"usage": 0.0, "start_date": ""})
        aggregated[key]["usage"] += entry["usage_count"]
        if entry["start_date"]:
            current_start = aggregated[key]["start_date"]
            aggregated[key]["start_date"] = min(current_start, entry["start_date"]) if current_start else entry["start_date"]

    next_rows: list[dict[str, Any]] = []
    for item in data:
        machine = str(item.get("machine", "")).strip()
        if infer_line_from_machine(machine) in {"", "엣지"}:
            next_rows.append(item)
            continue

        blade_name = normalize_boring_blade_name(get_display_blade_name(item))
        key = (machine, blade_name)
        if key not in aggregated:
            next_rows.append(
                {
                    **item,
                    "usage": 0,
                    "standard": get_boring_standard(machine, blade_name),
                    "avg7d": 0,
                }
            )
            continue

        total_usage = round(aggregated[key]["usage"], 3)
        next_rows.append(
            {
                **item,
                "usage": total_usage,
                "standard": get_boring_standard(machine, blade_name),
                "avg7d": max(0, round(total_usage / 7, 3)),
                "installDate": aggregated[key]["start_date"] or item.get("installDate", ""),
            }
        )
    return next_rows

def refresh_auto_sheet_target() -> dict[str, str]:
    latest_info = load_latest_upload_info()
    st.session_state.auto_sheet_url = latest_info.get("spreadsheet_url", st.session_state.get("auto_sheet_url", DEFAULT_GOOGLE_SHEET_URL))
    st.session_state.auto_sheet_name = latest_info.get("worksheet_title", st.session_state.get("auto_sheet_name", ""))
    st.session_state.auto_sheet_gid = latest_info.get("worksheet_gid", st.session_state.get("auto_sheet_gid", ""))
    st.session_state.auto_sheet_updated_at = latest_info.get("updated_at", st.session_state.get("auto_sheet_updated_at", ""))
    return latest_info


@st.fragment(run_every="10s")
def auto_sync_fragment() -> None:
    latest_info = refresh_auto_sheet_target()
    latest_updated_at = latest_info.get("updated_at", "")
    has_sync_result = bool(st.session_state.get("last_sheet_sync_details")) and bool(st.session_state.get("last_sheet_sync_at"))
    if not latest_updated_at or (latest_updated_at == st.session_state.get("last_applied_upload_at", "") and has_sync_result):
        return
    try:
        sync_from_google_sheet(
            st.session_state.get("auto_sheet_url", DEFAULT_GOOGLE_SHEET_URL),
            "auto",
            worksheet_name=st.session_state.get("auto_sheet_name") or None,
            worksheet_gid=st.session_state.get("auto_sheet_gid") or None,
            silent=True,
        )
        st.session_state.last_applied_upload_at = latest_updated_at
        save_dashboard_state()
    except Exception:
        return


def enrich_data(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in data:
        standard = row["standard"]
        rate = row["usage"] / standard if standard else 0
        remaining = max(0, standard - row["usage"])
        remain_days = days_left(remaining, row["avg7d"])
        enriched.append(
            {
                **row,
                "rate": rate,
                "remaining": remaining,
                "remainDays": remain_days,
                "predictedDate": "-" if remain_days == 999 else f"{remain_days}? ?",
                "displayStandard": format_cycle_value(row, standard),
                "displayRemaining": format_cycle_value(row, remaining),
                "displayBladeName": get_display_blade_name(row),
                "status": get_status(rate, row["quality"]),
            }
        )
    return enriched


def normalize_machine_name(value: Any) -> str:
    raw = str(value or "").strip()
    compact = raw.replace(" ", "")
    edge_aliases = {
        "?ｌ?諛대뜑#1": "?ｌ? #1",
        "?ｌ?諛대뜑#2": "?ｌ? #2",
        "?ｌ?#3,4": "?ｌ? #3,4",
        "?ｌ?#3": "?ｌ? #3,4",
        "?ｌ?#4": "?ｌ? #3,4",
        "?좉퇋?ｌ?諛대뜑#3": "?ｌ? #3,4",
        "?좉퇋?ｌ?諛대뜑#4": "?ｌ? #3,4",
        "?좉퇋?ｌ?諛대뜑#5": "?ｌ? #5",
        "?붾툝?ｌ?諛대뜑#6": "?ｌ? #6",
    }
    if compact in edge_aliases:
        return edge_aliases[compact]
    boring_aliases = {
        "NC蹂대쭅湲곗닔吏?1": "?섏쭅 #1",
        "NC蹂대쭅湲곗닔吏?2": "?섏쭅 #2",
        "NC蹂대쭅湲곗닔吏?3": "?섏쭅 #3",
        "NC蹂대쭅湲?3(?ъ씤?몃낫留곴린)": "?ъ씤??#3",
        "NC蹂대쭅湲?19": "?곕떇 #19",
        "NC蹂대쭅湲?20": "?곕떇 #20",
        "NC蹂대쭅湲?21": "?곕떇 #21",
        "NC蹂대쭅湲?22": "?곕떇 #22",
        "NC蹂대쭅湲?23": "?곕떇 #23",
        "NC蹂대쭅湲?24": "?곕떇 #24",
        "NC蹂대쭅湲?26(?좉퇋?묐㈃蹂대쭅湲?": "?묐㈃ #26",
        "NC蹂대쭅湲?27(?좉퇋?묐㈃蹂대쭅湲?": "?묐㈃ #27",
    }
    if compact in boring_aliases:
        return boring_aliases[compact]
    if compact.startswith("NC蹂대쭅湲곗닔吏?"):
        digits = "".join(ch for ch in compact if ch.isdigit())
        if digits:
            return f"?섏쭅 #{digits[-1]}"
    if "A?? in raw or "A?? in compact:
        return raw
    if compact.startswith("NC蹂대쭅湲?17"):
        return raw
    if compact.startswith("NC蹂대쭅湲?3(") and "A?? not in compact:
        return "?ъ씤??#3"
    if compact.startswith("NC蹂대쭅湲?26"):
        return "?묐㈃ #26"
    if compact.startswith("NC蹂대쭅湲?27"):
        return "?묐㈃ #27"
    for running_no in ["19", "20", "21", "22", "23", "24"]:
        if compact.startswith(f"NC蹂대쭅湲?{running_no}"):
            return f"?곕떇 #{running_no}"
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits and digits[0] in "123456" and "?ｌ?" in raw:
        return f"?ｌ? #{digits[0]}"
    if "?섏쭅" in raw and digits:
        return f"?섏쭅 #{digits[0]}"
    if "?ъ씤?? in raw and digits:
        return f"?ъ씤??#{digits[0]}"
    if "?묐㈃" in raw and digits:
        return f"?묐㈃ #{digits[0:2] if digits.startswith('2') and len(digits) > 1 else digits[0]}"
    if "?곕떇" in raw and digits:
        return f"?곕떇 #{digits}"
    if "NC蹂대쭅湲? in compact and digits:
        machine_no = digits
        if compact.startswith("NC蹂대쭅湲곗닔吏?):
            return f"?섏쭅 #{machine_no[0]}"
        if machine_no == "3":
            return "?ъ씤??#3"
        if machine_no in {"26", "27"}:
            return f"?묐㈃ #{machine_no}"
        if machine_no in {"19", "20", "21", "22", "23", "24"}:
            return f"?곕떇 #{machine_no}"
    return raw


def machine_matches_target(machine: str, target_machine: str) -> bool:
    if target_machine == "auto":
        return True
    if target_machine in MACHINE_GROUPS:
        return machine in MACHINE_GROUPS[target_machine]
    return machine == target_machine


def infer_line_from_machine(machine: str) -> str:
    normalized = normalize_machine_name(machine)
    if normalized.startswith("?ｌ?"):
        return "?ｌ?"
    if normalized.startswith("?곕떇"):
        return "?곕떇"
    if normalized.startswith("?묐㈃"):
        return "?묐㈃"
    if normalized.startswith("?ъ씤??):
        return "?ъ씤??
    if normalized.startswith("?섏쭅"):
        return "?섏쭅"
    return ""


def is_edge_machine(machine: str) -> bool:
    return infer_line_from_machine(machine) == "?節?"


def is_boring_machine(machine: str) -> bool:
    line_name = infer_line_from_machine(machine)
    return bool(line_name) and line_name != "?節?"


def parse_date_only(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = normalize_display_timestamp(raw)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized[:19] if fmt == "%Y-%m-%d %H:%M:%S" else normalized[:10], fmt).date()
        except ValueError:
            continue
    return None


def build_date_filter_options(values: list[Any]) -> list[str]:
    unique_dates = sorted({parsed.isoformat() for value in values if (parsed := parse_date_only(value)) is not None}, reverse=True)
    return ["?꾩껜", *unique_dates]


def build_year_month_day_options(values: list[Any]) -> tuple[list[str], dict[str, list[str]], dict[tuple[str, str], list[str]]]:
    parsed_dates = sorted(
        {parsed for value in values if (parsed := parse_date_only(value)) is not None},
        reverse=True,
    )
    years = sorted({str(value.year) for value in parsed_dates}, reverse=True)
    months_by_year: dict[str, list[str]] = {}
    days_by_year_month: dict[tuple[str, str], list[str]] = {}
    for parsed in parsed_dates:
        year = str(parsed.year)
        month = f"{parsed.month:02d}"
        day = f"{parsed.day:02d}"
        months_by_year.setdefault(year, [])
        if month not in months_by_year[year]:
            months_by_year[year].append(month)
        days_by_year_month.setdefault((year, month), [])
        if day not in days_by_year_month[(year, month)]:
            days_by_year_month[(year, month)].append(day)
    return ["?꾩껜", *years], months_by_year, days_by_year_month


def apply_date_dropdown_filter(df: pd.DataFrame, column: str, prefix: str, container) -> pd.DataFrame:
    parsed_dates = [parsed for parsed in (parse_date_only(value) for value in df[column].tolist()) if parsed is not None]
    if not parsed_dates:
        container.date_input("?좎쭨", value=None, key=f"{prefix}_date_filter", format="YYYY-MM-DD")
        return df

    selected_date = container.date_input(
        "?좎쭨",
        value=None,
        key=f"{prefix}_date_filter",
        format="YYYY-MM-DD",
    )
    if selected_date is None:
        return df
    return df[df[column].apply(lambda value: parse_date_only(value) == selected_date)]


def expand_history_rows_by_blade(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return history_df

    expanded_rows: list[dict[str, Any]] = []
    for _, history_row in history_df.iterrows():
        row_dict = history_row.to_dict()
        machine = str(row_dict.get("?ㅻ퉬", "")).strip()
        blade_name = str(row_dict.get("?좊Ъ紐?, "")).strip()
        if blade_name:
            blade_parts = [part.strip() for part in blade_name.split(",") if part.strip()]
            if blade_parts:
                for blade in blade_parts:
                    copied = dict(row_dict)
                    copied["?좊Ъ紐?] = blade
                    expanded_rows.append(copied)
            else:
                expanded_rows.append(row_dict)
            continue
        blade_list = get_history_blade_list(machine)
        if blade_list:
            for blade in blade_list:
                copied = dict(row_dict)
                copied["?좊Ъ紐?] = blade
                expanded_rows.append(copied)
        else:
            expanded_rows.append(row_dict)
    return pd.DataFrame(expanded_rows)


def remove_redundant_boring_summary_rows(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return history_df

    normalized_df = history_df.copy()
    machine_col = next((col for col in ["?ㅻ퉬", "??삵돩"] if col in normalized_df.columns), None)
    blade_col = next((col for col in ["?좊Ъ紐?, "?醫듢わ쭗?"] if col in normalized_df.columns), None)
    time_col = next((col for col in ["諛섏쁺?쒓컖", "獄쏆꼷???볦퍟"] if col in normalized_df.columns), None)
    if not machine_col or not blade_col or not time_col:
        return normalized_df

    normalized_df[machine_col] = normalized_df[machine_col].fillna("").astype(str).str.strip()
    normalized_df[blade_col] = normalized_df[blade_col].fillna("").astype(str).str.strip()
    normalized_df[time_col] = normalized_df[time_col].fillna("").astype(str).str.strip()

    boring_mask = normalized_df[machine_col].apply(is_boring_machine)
    if not boring_mask.any():
        return normalized_df

    detailed_keys = {
        (row[time_col], row[machine_col])
        for _, row in normalized_df.loc[boring_mask & normalized_df[blade_col].ne("")].iterrows()
    }
    if not detailed_keys:
        return normalized_df

    keep_mask = ~(
        boring_mask
        & normalized_df[blade_col].eq("")
        & normalized_df.apply(lambda row: (row[time_col], row[machine_col]) in detailed_keys, axis=1)
    )
    return normalized_df.loc[keep_mask].reset_index(drop=True)


def aggregate_history_rows(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return history_df

    normalized_df = history_df.copy()
    normalized_df = remove_redundant_boring_summary_rows(normalized_df)
    for column in ["諛섏쁺 ?ъ슜??m)", "諛섏쁺 ?ъ슜????"]:
        if column in normalized_df.columns:
            normalized_df[column] = pd.to_numeric(normalized_df[column], errors="coerce").fillna(0)

    group_columns = [column for column in ["諛섏쁺?쒓컖", "?ㅻ퉬", "?좊Ъ紐?] if column in normalized_df.columns]
    if not group_columns:
        return normalized_df

    aggregation_map: dict[str, Any] = {}
    if "諛섏쁺 ?ъ슜??m)" in normalized_df.columns:
        aggregation_map["諛섏쁺 ?ъ슜??m)"] = "sum"
    if "諛섏쁺 ?ъ슜????" in normalized_df.columns:
        aggregation_map["諛섏쁺 ?ъ슜????"] = "sum"
    if "?곗씠??湲곗??쇱옄" in normalized_df.columns:
        aggregation_map["?곗씠??湲곗??쇱옄"] = lambda values: min(
            [str(value).strip() for value in values if str(value).strip() and str(value).strip().lower() != "nan"],
            default="",
        )

    if not aggregation_map:
        return normalized_df

    aggregated_df = normalized_df.groupby(group_columns, as_index=False).agg(aggregation_map)
    return aggregated_df


def send_teams_complete_alert(row: dict[str, Any]) -> None:
    webhook_url = st.session_state.teams_webhook_url.strip()
    if not webhook_url:
        raise ValueError("Teams Webhook URL???ㅼ젙?섏? ?딆븯?듬땲??")

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {"type": "TextBlock", "size": "Large", "weight": "Bolder", "text": "?좊Ъ 援먯껜 ?꾨즺"},
                        {"type": "TextBlock", "wrap": True, "text": f"{row['line']} / {row['machine']} / {row['spindle']}"},
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "?ㅻ퉬", "value": row["machine"]},
                                {"title": "?좊Ъ", "value": row["bladeName"]},
                                {"title": "援먯껜 ?쒖젏 ?ъ슜??, "value": format_cycle_value(row, parse_numeric_value(row.get("usage", 0)))},
                                {"title": "議곗튂", "value": "援먯껜?꾨즺"},
                                {"title": "泥섎━??, "value": date.today().isoformat()},
                            ],
                        },
                    ],
                },
            }
        ],
    }

    response = requests.post(webhook_url, json=payload, timeout=30)
    if not response.ok:
        raise RuntimeError(f"Teams ?뚮┝ ?ㅽ뙣: HTTP {response.status_code}")


def send_teams_replace_alert(row: dict[str, Any]) -> None:
    webhook_url = st.session_state.teams_webhook_url.strip()
    if not webhook_url:
        raise ValueError("Teams Webhook URL???ㅼ젙?섏? ?딆븯?듬땲??")

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {"type": "TextBlock", "size": "Large", "weight": "Bolder", "text": "?좊Ъ 援먯껜 ?뚮┝"},
                        {"type": "TextBlock", "wrap": True, "text": f"{row['line']} / {row['machine']} / {row['spindle']}"},
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "?ㅻ퉬", "value": row["machine"]},
                                {"title": "?좊Ъ", "value": row["displayBladeName"]},
                                {"title": "?ъ슜瑜?, "value": f"{round(row['rate'] * 100)}%"},
                                {"title": "?붿뿬?ъ슜??, "value": row["displayRemaining"]},
                                {"title": "?덉륫援먯껜", "value": row["predictedDate"]},
                            ],
                        },
                    ],
                },
            }
        ],
    }

    response = requests.post(webhook_url, json=payload, timeout=30)
    if not response.ok:
        raise RuntimeError(f"Teams ?뚮┝ ?ㅽ뙣: HTTP {response.status_code}")


def get_replace_alert_signature(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("machine", "")),
            str(round(parse_numeric_value(row.get("usage", 0)), 3)),
            str(row.get("quality", 0)),
            str(row.get("status", "")),
        ]
    )


def process_replace_alerts(enriched: list[dict[str, Any]]) -> None:
    alert_history = st.session_state.get("replace_alert_history", {})
    active_machines = {str(row.get("machine", "")) for row in enriched if row.get("status") == "replace"}
    next_history = {machine: signature for machine, signature in alert_history.items() if machine in active_machines}
    latest_message = ""

    for row in enriched:
        machine = str(row.get("machine", "")).strip()
        if not machine or row.get("status") != "replace":
            continue

        if next_history.get(machine) == "sent":
            continue

        try:
            send_teams_replace_alert(row)
            next_history[machine] = "sent"
            latest_message = f"{machine} ?ㅻ퉬 ?좊Ъ 援먯껜 ?뚮┝???꾩넚?덉뒿?덈떎."
        except Exception as exc:
            latest_message = f"{machine} ?ㅻ퉬 ?좊Ъ 援먯껜 ?뚮┝ ?꾩넚 ?ㅽ뙣: {exc}"

    if next_history != alert_history or latest_message:
        st.session_state.replace_alert_history = next_history
        if latest_message:
            st.session_state.send_result = latest_message
        save_dashboard_state()


def parse_numeric_value(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    normalized = str(value).replace(",", "").strip()
    try:
        return float(normalized)
    except ValueError:
        return 0.0


def parse_edge_material_counts(value: Any) -> tuple[int, int]:
    raw = str(value or "").strip()
    if not raw or raw == "-":
        return 0, 0
    parts = [part.strip() for part in raw.split("/")[:4]]
    while len(parts) < 4:
        parts.append("")

    flags = []
    for part in parts:
        normalized = part.strip().replace("-", "")
        flags.append(1 if normalized and any(ch.isdigit() for ch in normalized) else 0)

    front_count = flags[0] + flags[1]
    back_count = flags[2] + flags[3]
    return front_count, back_count


def recommend_edge_standard(total: float) -> float:
    if total <= 0:
        return 0
    if total < 1000:
        return math.ceil(total / 10) * 10
    if total < 10000:
        return math.ceil(total / 100) * 100
    return math.ceil(total / 1000) * 1000


def to_google_sheet_csv_url(url: str, worksheet_name: str | None = None, worksheet_gid: str | None = None) -> str:
    raw = url.strip()
    if not raw or "docs.google.com/spreadsheets" not in raw:
        return raw
    match = __import__("re").search(r"/d/([a-zA-Z0-9-_]+)", raw)
    if not match:
        return raw
    if worksheet_gid:
        return f"https://docs.google.com/spreadsheets/d/{match.group(1)}/export?format=csv&gid={worksheet_gid}"
    if worksheet_name:
        return f"https://docs.google.com/spreadsheets/d/{match.group(1)}/gviz/tq?tqx=out:csv&sheet={worksheet_name}"
    gid_match = __import__("re").search(r"[?&#]gid=([0-9]+)", raw)
    gid = gid_match.group(1) if gid_match else "0"
    return f"https://docs.google.com/spreadsheets/d/{match.group(1)}/export?format=csv&gid={gid}"


def update_machine_usage(machine: str, total_usage_m: float, start_date: str, period_days: int, blade_name: str | None = None) -> None:
    next_rows = []
    for item in st.session_state.equipment_data:
        item_blade_name = get_display_blade_name(item)
        matches_machine = item["machine"] == machine
        matches_blade = blade_name is None or item_blade_name == blade_name
        if matches_machine and matches_blade:
            next_standard = EDGE_FIXED_STANDARDS.get(item["machine"], item["standard"]) if item["line"] == "?ｌ?" else item["standard"]
            current_usage = parse_numeric_value(item.get("usage", 0))
            accumulated_usage = round(current_usage + total_usage_m, 3)
            current_install = str(item.get("installDate", "") or "")
            if start_date and current_install:
                next_install_date = min(current_install, start_date)
            else:
                next_install_date = start_date or current_install
            next_rows.append(
                {
                    **item,
                    "usage": accumulated_usage,
                    "standard": next_standard,
                    "avg7d": max(1, round(accumulated_usage / period_days, 3)),
                    "installDate": next_install_date,
                }
            )
        else:
            next_rows.append(item)
    st.session_state.equipment_data = next_rows
    save_dashboard_state()


def get_boring_blade_code(value: Any) -> str:
    raw = str(value or "").strip()
    match = __import__("re").search(r"(\d+)", raw)
    return match.group(1) if match else raw


def normalize_boring_blade_name(value: Any) -> str:
    blade_code = get_boring_blade_code(value)
    blade_map = {
        "5": "過5(愿?? ?좊Ъ",
        "8": "過8(愿?? ?좊Ъ",
        "12": "過12(愿?? ?좊Ъ",
        "15": "過15 ?좊Ъ",
        "20": "過20 ?좊Ъ",
        "35": "過35 ?좊Ъ",
    }
    return blade_map.get(blade_code, str(value or "").strip())


def get_boring_standard(machine: Any, blade_name: Any) -> int:
    normalized_machine = normalize_machine_name(machine)
    normalized_blade = normalize_boring_blade_name(blade_name)
    if normalized_machine.startswith("?곕떇") and normalized_blade == "過5(愿?? ?좊Ъ":
        return 50000
    return 10000


def build_boring_history_entries_from_dataframe(df: pd.DataFrame, sync_time: str) -> list[dict[str, Any]]:
    df.columns = [str(col).replace("\ufeff", "").strip() for col in df.columns]
    machine_col = next((c for c in ["?ㅻ퉬紐?, "?ㅻ퉬", "?ㅻ퉬紐끸뼹"] if c in df.columns), None)
    date_col = next((c for c in ["?앹궛??, "?묒뾽??, "date"] if c in df.columns), None)
    if machine_col is None:
        return []

    aggregated: dict[tuple[str, str], dict[str, Any]] = {}
    machine_start_dates: dict[str, str] = {}
    boring_machines_seen: list[str] = []
    for _, row in df.iterrows():
        machine = normalize_machine_name(row.get(machine_col, ""))
        if not is_boring_machine(machine):
            continue

        if machine not in boring_machines_seen:
            boring_machines_seen.append(machine)
        row_date = normalize_history_date_value(row.get(date_col, "")) if date_col else ""
        if row_date:
            current_machine_start = machine_start_dates.get(machine, "")
            machine_start_dates[machine] = min(current_machine_start, row_date) if current_machine_start else row_date
        for blade_column in BORING_HISTORY_BLADE_COLUMNS:
            blade_name = normalize_boring_blade_name(blade_column)
            key = (machine, blade_name)
            aggregated.setdefault(key, {"usage_count": 0.0, "start_date": ""})
            usage_count = parse_numeric_value(row.get(blade_column, 0))
            aggregated[key]["usage_count"] += usage_count
            if row_date and usage_count > 0:
                current_start = str(aggregated[key]["start_date"]).strip()
                aggregated[key]["start_date"] = min(current_start, row_date) if current_start else row_date

    if not boring_machines_seen:
        return []

    entries: list[dict[str, Any]] = []
    for machine in boring_machines_seen:
        for blade_name in BORING_HISTORY_BLADE_NAMES:
            payload = aggregated.get((machine, blade_name), {"usage_count": 0.0, "start_date": ""})
            entries.append(
                {
                    "諛섏쁺?쒓컖": sync_time,
                    "???: "蹂대쭅 ?꾩껜",
                    "?ㅻ퉬": machine,
                    "?좊Ъ紐?: blade_name,
                    "諛섏쁺 ?ъ슜??m)": "",
                    "諛섏쁺 ?ъ슜????": round(float(payload["usage_count"]), 3),
                    "?쒖옉??: payload["start_date"] or machine_start_dates.get(machine, ""),
                }
            )
    return entries


def load_latest_boring_snapshot_entries() -> list[dict[str, Any]]:
    latest_info = load_latest_upload_info()
    if str(latest_info.get("dataset_type", "")).strip() != "蹂대쭅":
        return []

    erp_file_name = str(latest_info.get("erp_file_name", "")).strip()
    worksheet_title = str(latest_info.get("worksheet_title", "")).strip()
    sync_time = extract_sync_time_from_text(worksheet_title or erp_file_name)
    df: pd.DataFrame | None = None

    spreadsheet_url = str(latest_info.get("spreadsheet_url", "")).strip()
    worksheet_gid = str(latest_info.get("worksheet_gid", "")).strip()
    if spreadsheet_url and worksheet_gid:
        try:
            csv_url = to_google_sheet_csv_url(spreadsheet_url, worksheet_gid=worksheet_gid)
            session = requests.Session()
            session.trust_env = False
            response = session.get(csv_url, timeout=30)
            response.raise_for_status()
            df = pd.read_csv(BytesIO(response.content))
        except Exception:
            df = None

    if df is None:
        if not erp_file_name:
            return []

        stem = Path(erp_file_name).stem
        xlsx_path = WORK_DIR / "output" / f"{stem}_merged.xlsx"
        csv_path = WORK_DIR / "output" / f"{stem}_merged.csv"
        if xlsx_path.exists():
            try:
                df = pd.read_excel(xlsx_path)
            except Exception:
                return []
        elif csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
            except Exception:
                return []
        else:
            return []

    return build_boring_history_entries_from_dataframe(df, sync_time)


def sync_time_to_boring_worksheet_title(sync_time: str) -> str:
    digits = "".join(ch for ch in str(sync_time or "") if ch.isdigit())
    if len(digits) >= 14:
        return f"蹂대쭅_grd_List_{digits[:14]}"
    return ""


def load_boring_snapshot_entries_for_sync_time(sync_time: str, spreadsheet_url: str = DEFAULT_GOOGLE_SHEET_URL) -> list[dict[str, Any]]:
    worksheet_title = sync_time_to_boring_worksheet_title(sync_time)
    worksheet_gid = BORING_WORKSHEET_GID_BY_SYNC_TIME.get(str(sync_time).strip(), "")
    if (not worksheet_title and not worksheet_gid) or not spreadsheet_url:
        return []
    try:
        csv_url = to_google_sheet_csv_url(
            spreadsheet_url,
            worksheet_name=None if worksheet_gid else worksheet_title,
            worksheet_gid=worksheet_gid or None,
        )
        session = requests.Session()
        session.trust_env = False
        response = session.get(csv_url, timeout=30)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content))
        if df.empty:
            return []
        return build_boring_history_entries_from_dataframe(df, sync_time)
    except Exception:
        return []


def rebuild_boring_history_from_remote(history: list[dict[str, Any]], spreadsheet_url: str = DEFAULT_GOOGLE_SHEET_URL) -> list[dict[str, Any]]:
    normalized_history = normalize_sheet_sync_history(history)
    if not normalized_history:
        return []

    preserved_history = [
        entry
        for entry in normalized_history
        if not (
            str(entry.get("???, "")).strip() == "蹂대쭅 ?꾩껜"
            or is_boring_machine(str(entry.get("?ㅻ퉬", "")).strip())
        )
    ]
    boring_sync_times = sorted(
        {
            str(entry.get("諛섏쁺?쒓컖", "")).strip()
            for entry in normalized_history
            if (str(entry.get("???, "")).strip() == "蹂대쭅 ?꾩껜" or is_boring_machine(str(entry.get("?ㅻ퉬", "")).strip()))
            and str(entry.get("諛섏쁺?쒓컖", "")).strip()
        }
    )
    rebuilt_entries: list[dict[str, Any]] = []
    for sync_time in boring_sync_times:
        replacement_entries = load_boring_snapshot_entries_for_sync_time(sync_time, spreadsheet_url)
        if replacement_entries:
            rebuilt_entries.extend(replacement_entries)
        else:
            rebuilt_entries.extend(
                [
                    entry
                    for entry in normalized_history
                    if (
                        (str(entry.get("???, "")).strip() == "蹂대쭅 ?꾩껜" or is_boring_machine(str(entry.get("?ㅻ퉬", "")).strip()))
                        and str(entry.get("諛섏쁺?쒓컖", "")).strip() == sync_time
                    )
                ]
            )
    return merge_sheet_sync_history(preserved_history, rebuilt_entries)


def overlay_latest_boring_snapshot_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_boring_entries = load_latest_boring_snapshot_entries()
    if not latest_boring_entries:
        return history

    sync_time = str(latest_boring_entries[0].get("諛섏쁺?쒓컖", "")).strip()
    preserved_history = [
        entry
        for entry in normalize_sheet_sync_history(history)
        if not (
            str(entry.get("???, "")).strip() == "蹂대쭅 ?꾩껜"
            and str(entry.get("諛섏쁺?쒓컖", "")).strip() == sync_time
        )
    ]
    return merge_sheet_sync_history(preserved_history, latest_boring_entries)


def replace_boring_usage_snapshot(grouped: dict[tuple[str, str], dict[str, Any]]) -> None:
    snapshot_map: dict[tuple[str, str], dict[str, Any]] = {}
    for (machine, blade_name), payload in grouped.items():
        normalized_machine = normalize_machine_name(machine)
        blade_code = get_boring_blade_code(normalize_boring_blade_name(blade_name))
        snapshot_map[(normalized_machine, blade_code)] = payload

    next_rows: list[dict[str, Any]] = []
    for item in st.session_state.equipment_data:
        normalized_machine = normalize_machine_name(item.get("machine", ""))
        if not is_boring_machine(normalized_machine):
            next_rows.append(item)
            continue

        blade_code = get_boring_blade_code(get_display_blade_name(item))
        payload = snapshot_map.get((normalized_machine, blade_code))
        if payload is None:
            next_rows.append(
                {
                    **item,
                    "usage": 0,
                    "standard": get_boring_standard(normalized_machine, get_display_blade_name(item)),
                    "avg7d": 0,
                }
            )
            continue

        total_usage = round(parse_numeric_value(payload.get("usage_count", 0)), 3)
        start_date = str(payload.get("start_date", "") or item.get("installDate", "")).strip()
        next_rows.append(
            {
                **item,
                "usage": total_usage,
                "standard": get_boring_standard(normalized_machine, get_display_blade_name(item)),
                "avg7d": max(1, round(total_usage / 7, 3)) if total_usage > 0 else 0,
                "installDate": start_date or item.get("installDate", ""),
            }
        )

    st.session_state.equipment_data = next_rows
    save_dashboard_state()


def has_boring_history_rows(history: list[dict[str, Any]]) -> bool:
    for entry in history:
        machine = normalize_machine_name(str(entry.get("?ㅻ퉬", entry.get("??삵돩", ""))).strip())
        if is_boring_machine(machine):
            return True
    return False


def handle_excel_upload(uploaded_file, target_machine: str) -> None:
    if uploaded_file is None:
        return
    df = pd.read_excel(uploaded_file)
    usage_col = "?ｌ??ъ슜??m)" if "?ｌ??ъ슜??m)" in df.columns else "珥앹뿣吏?ъ슜??m)"
    if usage_col not in df.columns:
        st.session_state.send_result = "?묒????ｌ??ъ슜??m) ?먮뒗 珥앹뿣吏?ъ슜??m) ?댁씠 ?놁뒿?덈떎."
        return

    valid_rows = df[df[usage_col].notna()].copy()
    total_usage_m = float(valid_rows[usage_col].apply(parse_numeric_value).sum())
    date_candidates = pd.to_datetime(valid_rows["?앹궛??], errors="coerce") if "?앹궛?? in valid_rows.columns else pd.Series(dtype="datetime64[ns]")
    dates = date_candidates.dropna().sort_values()
    start_date = dates.iloc[0].date().isoformat() if not dates.empty else ""
    end_date = dates.iloc[-1].date().isoformat() if not dates.empty else ""
    period_days = EDGE_UPLOAD_RULES[target_machine]["periodDays"]
    update_machine_usage(target_machine, total_usage_m, start_date, period_days)

    st.session_state.upload_summary = {
        "fileName": uploaded_file.name,
        "rows": len(valid_rows),
        "totalUsageM": round(total_usage_m, 3),
        "startDate": start_date or "-",
        "endDate": end_date or "-",
        "targetMachine": target_machine,
        "periodDays": period_days,
    }
    st.session_state.send_result = f"?묒? ?낅줈???꾨즺: {uploaded_file.name} / {target_machine} / {total_usage_m:.3f} m 諛섏쁺"


def sync_from_google_sheet(
    sheet_url: str,
    target_machine: str,
    worksheet_name: str | None = None,
    worksheet_gid: str | None = None,
    silent: bool = False,
) -> None:
    if not sheet_url.strip():
        if not silent:
            st.session_state.send_result = "援ш? ?ㅽ봽?덈뱶?쒗듃 留곹겕瑜??낅젰??二쇱꽭??"
        return

    csv_url = to_google_sheet_csv_url(sheet_url, worksheet_name, worksheet_gid)
    session = requests.Session()
    session.trust_env = False
    response = session.get(csv_url, timeout=30)
    response.raise_for_status()
    sync_hash = hashlib.sha1(response.content).hexdigest()
    hash_bucket_key = f"content::{target_machine}"
    existing_hashes = st.session_state.sheet_sync_hashes.get(hash_bucket_key, [])
    if isinstance(existing_hashes, str):
        existing_hashes = [existing_hashes]
    is_duplicate_sync = sync_hash in existing_hashes
    df = pd.read_csv(BytesIO(response.content))
    df.columns = [str(col).replace("\ufeff", "").strip() for col in df.columns]

    boring_blade_columns = BORING_HISTORY_BLADE_COLUMNS
    has_boring_blade_columns = any(column in df.columns for column in boring_blade_columns)

    usage_col = next(
        (candidate for candidate in ["?ｌ??ъ슜??m)", "珥앹뿣吏?ъ슜??m)", "usage_m", "?ｌ??ъ슜??, "珥앹뿣吏?ъ슜??] if candidate in df.columns),
        None,
    )
    quantity_col = next(
        (candidate for candidate in ["?앹궛??, "qty", "quantity"] if candidate in df.columns),
        None,
    )
    if usage_col is None and quantity_col is None:
        raise ValueError("?쒗듃???ｌ??ъ슜??m) ?먮뒗 ?앹궛???댁씠 ?놁뒿?덈떎.")

    machine_col = next(
        (c for c in ["?ㅻ퉬", "?ㅻ퉬紐?, "?ㅻ퉬紐끸뼹", "?멸린", "machine", "machine_name"] if c in df.columns),
        None,
    )
    date_col = next((c for c in ["?앹궛??, "date", "?묒뾽??] if c in df.columns), None)
    material_col = next((c for c in ["?ъ쭏", "?ъ쭏??, "material"] if c in df.columns), None)

    records = []
    for _, row in df.iterrows():
        machine = normalize_machine_name(row[machine_col]) if machine_col else target_machine
        parsed_usage = parse_numeric_value(row[usage_col]) if usage_col else 0.0
        parsed_quantity = parse_numeric_value(row[quantity_col]) if quantity_col else 0.0
        usage_m = parsed_usage
        usage_count = 0.0
        prod_date = row[date_col] if date_col else None
        if not machine:
            continue
        if machine.startswith(("?섏쭅", "?ъ씤??, "?곕떇", "?묐㈃")):
            boring_blade_records = []
            if has_boring_blade_columns:
                for blade_column in boring_blade_columns:
                    blade_usage = parse_numeric_value(row.get(blade_column, 0))
                    boring_blade_records.append(
                        {
                            "machine": machine,
                            "blade_name": normalize_boring_blade_name(blade_column),
                            "usageM": 0.0,
                            "usageCount": blade_usage,
                            "prodDate": prod_date,
                        }
                    )
                records.extend(boring_blade_records)
                continue
            for blade_column in boring_blade_columns:
                blade_usage = parse_numeric_value(row.get(blade_column, 0))
                if blade_usage <= 0:
                    continue
                boring_blade_records.append(
                    {
                        "machine": machine,
                        "blade_name": normalize_boring_blade_name(blade_column),
                        "usageM": 0.0,
                        "usageCount": blade_usage,
                        "prodDate": prod_date,
                    }
                )
            if quantity_col and parsed_quantity > 0:
                usage_count = parsed_quantity
                usage_m = parsed_quantity
            else:
                continue
        elif usage_m <= 0:
            continue
        if machine == "?ｌ? #6":
            front_count, back_count = parse_edge_material_counts(row[material_col]) if material_col else (0, 0)
            total_count = front_count + back_count
            if total_count <= 0:
                records.append(
                    {
                        "machine": machine,
                        "blade_name": "AT ?좊Ъ(?꾨㈃)",
                        "usageM": usage_m,
                        "usageCount": 0.0,
                        "prodDate": prod_date,
                    }
                )
                continue
            front_usage = usage_m * (front_count / total_count)
            back_usage = usage_m * (back_count / total_count)
            if front_usage > 0:
                records.append(
                    {
                        "machine": machine,
                        "blade_name": "AT ?좊Ъ(?꾨㈃)",
                        "usageM": front_usage,
                        "usageCount": 0.0,
                        "prodDate": prod_date,
                    }
                )
            if back_usage > 0:
                records.append(
                    {
                        "machine": machine,
                        "blade_name": "AT ?좊Ъ(?꾨㈃)",
                        "usageM": back_usage,
                        "usageCount": 0.0,
                        "prodDate": prod_date,
                    }
                )
            continue

        blade_name = "AT ?좊Ъ(?꾨㈃)" if machine.startswith("?ｌ?") else ""
        records.append(
            {
                "machine": machine,
                "blade_name": blade_name,
                "usageM": usage_m,
                "usageCount": usage_count,
                "prodDate": prod_date,
            }
        )

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    known_machine_blades = {
        (item["machine"], get_display_blade_name(item))
        for item in st.session_state.equipment_data
    }
    for row in records:
        blade_name = str(row.get("blade_name", "") or "")
        machine_blade_key = (row["machine"], blade_name)
        if blade_name and machine_blade_key not in known_machine_blades:
            continue
        if not blade_name and row["machine"] not in {machine for machine, _ in known_machine_blades}:
            continue
        if not machine_matches_target(row["machine"], target_machine):
            continue
        grouped.setdefault(machine_blade_key, {"total": 0.0, "usage_count": 0.0, "dates": []})
        grouped[machine_blade_key]["total"] += row["usageM"]
        grouped[machine_blade_key]["usage_count"] += row["usageCount"]
        if row["prodDate"]:
            grouped[machine_blade_key]["dates"].append(row["prodDate"])

    grouped_machines = [machine for machine, _ in grouped.keys()]
    edge_count = sum(1 for machine in grouped_machines if machine.startswith("?ｌ?"))
    boring_count = sum(1 for machine in grouped_machines if machine.startswith(("?섏쭅", "?ъ씤??, "?곕떇", "?묐㈃")))
    if target_machine == "auto":
        if edge_count and not boring_count:
            effective_target_label = "?ｌ? ?꾩껜"
        elif boring_count and not edge_count:
            effective_target_label = "蹂대쭅 ?꾩껜"
        elif edge_count and boring_count:
            effective_target_label = "?꾩껜"
        else:
            effective_target_label = "?먮룞"
    else:
        effective_target_label = target_machine

    sync_details = []
    is_boring_snapshot = boring_count > 0 and edge_count == 0 and any(str(blade_name).strip() for _, blade_name in grouped.keys())
    if is_boring_snapshot:
        replace_boring_usage_snapshot(grouped)
    for (machine, blade_name), payload in grouped.items():
        dates = pd.to_datetime(pd.Series(payload["dates"]), errors="coerce").dropna().sort_values()
        start_date = dates.iloc[0].date().isoformat() if not dates.empty else ""
        period_days = EDGE_UPLOAD_RULES.get(machine, {"periodDays": 7})["periodDays"]
        if not is_duplicate_sync and not is_boring_snapshot:
            update_machine_usage(machine, payload["total"], start_date, period_days, blade_name or None)
        sync_details.append(
            {
                "machine": machine,
                "blade_name": blade_name or get_machine_blade_summary(machine),
                "usage_m": round(payload["total"], 3) if machine.startswith("?ｌ?") else "",
                "usage_count": round(payload["usage_count"], 3) if machine.startswith(("?섏쭅", "?ъ씤??, "?곕떇", "?묐㈃")) else "",
                "start_date": start_date or "-",
            }
        )

    sync_time = extract_sync_time_from_text(worksheet_name or st.session_state.get("auto_sheet_name", "") or sheet_url)
    if grouped:
        synced_label = ", ".join(f"{machine}/{blade}" if blade else machine for machine, blade in grouped.keys())
    else:
        detected = sorted({record["machine"] for record in records if record["machine"]})
        detected_label = ", ".join(detected[:8]) if detected else "?쎌? ?ㅻ퉬 ?놁쓬"
        synced_label = f"諛섏쁺 ?ㅻ퉬 ?놁쓬 ({detected_label})"
    history_entries = [
        {
            "諛섏쁺?쒓컖": sync_time,
            "???: effective_target_label,
            "?ㅻ퉬": detail["machine"],
            "?좊Ъ紐?: detail["blade_name"],
            "諛섏쁺 ?ъ슜??m)": detail["usage_m"],
            "諛섏쁺 ?ъ슜????": detail["usage_count"],
            "?쒖옉??: detail["start_date"],
        }
        for detail in sync_details
    ]
    should_replace_boring_history = False
    if history_entries and (not is_duplicate_sync or should_replace_boring_history):
        previous_history = st.session_state.get("sheet_sync_history", [])
        if should_replace_boring_history:
            replacement_keys = {
                (
                    str(entry.get("諛섏쁺?쒓컖", "")).strip(),
                    str(entry.get("???, "")).strip(),
                    str(entry.get("?ㅻ퉬", "")).strip(),
                    str(entry.get("?좊Ъ紐?, "")).strip(),
                    str(entry.get("?쒖옉??, "")).strip(),
                )
                for entry in history_entries
            }
            previous_history = [
                entry
                for entry in previous_history
                if (
                    str(entry.get("諛섏쁺?쒓컖", "")).strip(),
                    str(entry.get("???, "")).strip(),
                    str(entry.get("?ㅻ퉬", "")).strip(),
                    str(entry.get("?좊Ъ紐?, "")).strip(),
                    str(entry.get("?쒖옉??, "")).strip(),
                )
                not in replacement_keys
            ]
        merged_history = merge_sheet_sync_history(previous_history, history_entries)
        st.session_state.sheet_sync_history = merged_history
        save_sheet_sync_history(merged_history)
        new_keys = {
            (
                str(entry.get("???, "")).strip(),
                str(entry.get("?ㅻ퉬", "")).strip(),
                str(entry.get("?좊Ъ紐?, "")).strip(),
                str(entry.get("諛섏쁺 ?ъ슜??m)", "")).strip(),
                str(entry.get("諛섏쁺 ?ъ슜????", "")).strip(),
                str(entry.get("?쒖옉??, "")).strip(),
            )
            for entry in history_entries
        }
        matching_times = [
            str(entry.get("諛섏쁺?쒓컖", "")).strip()
            for entry in merged_history
            if (
                str(entry.get("???, "")).strip(),
                str(entry.get("?ㅻ퉬", "")).strip(),
                str(entry.get("?좊Ъ紐?, "")).strip(),
                str(entry.get("諛섏쁺 ?ъ슜??m)", "")).strip(),
                str(entry.get("諛섏쁺 ?ъ슜????", "")).strip(),
                str(entry.get("?쒖옉??, "")).strip(),
            )
            in new_keys
        ]
        st.session_state.last_sheet_sync_at = min(matching_times) if matching_times else sync_time
    elif is_duplicate_sync:
        existing_history = st.session_state.get("sheet_sync_history", [])
        if existing_history:
            st.session_state.last_sheet_sync_at = str(existing_history[0].get("諛섏쁺?쒓컖", st.session_state.get("last_sheet_sync_at", ""))).strip()
    st.session_state.last_sheet_sync_details = normalize_last_sheet_sync_details(sync_details)
    if not is_duplicate_sync:
        updated_hashes = [*existing_hashes, sync_hash]
        st.session_state.sheet_sync_hashes[hash_bucket_key] = updated_hashes[-50:]
    if not silent:
        if is_duplicate_sync:
            st.session_state.send_result = f"媛숈? ?곗씠?곕씪 ?ъ슜?됱? ?좎??섍퀬 諛섏쁺 寃곌낵留?媛깆떊?덉뒿?덈떎: {synced_label}"
        else:
            st.session_state.send_result = f"援ш? ?ㅽ봽?덈뱶?쒗듃 ?먮룞 諛섏쁺 ?꾨즺: {synced_label}"
    save_dashboard_state()


def handle_action(row_id: int) -> None:
    selected_item = next((item for item in st.session_state.equipment_data if item["id"] == row_id), None)
    if selected_item is None:
        return
    if parse_numeric_value(selected_item.get("usage", 0)) <= 0 or selected_item.get("rate", 0) < 1:
        st.session_state.send_result = f"{selected_item['machine']} ??? ?? ?? ??? ????."
        save_dashboard_state()
        return

    today = date.today().isoformat()
    selected_machine = selected_item["machine"]
    completed_usage = parse_numeric_value(selected_item.get("usage", 0))
    completed_usage_label = format_cycle_value(selected_item, completed_usage)
    completed_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.equipment_data = [
        {
            **item,
            "usage": 0 if item["machine"] == selected_machine else item["usage"],
            "quality": 0 if item["machine"] == selected_machine else item["quality"],
            "installDate": today if item["machine"] == selected_machine else item.get("installDate", ""),
            "actionStep": "" if item["machine"] == selected_machine else item.get("actionStep", ""),
        }
        for item in st.session_state.equipment_data
    ]
    st.session_state.replace_alert_history.pop(selected_machine, None)
    machine_reset_at = st.session_state.get("machine_reset_at", {})
    machine_reset_at[selected_machine] = completed_at
    st.session_state.machine_reset_at = machine_reset_at
    completion_entry = {
        "??": completed_at,
        "??": selected_item["machine"],
        "???": get_display_blade_name(selected_item),
        "?? ?? ???": completed_usage_label,
    }
    st.session_state.completion_history = [completion_entry, *st.session_state.get("completion_history", [])]
    message = f"{selected_item['machine']} ?? ?? ???."
    try:
        send_teams_complete_alert(selected_item)
        message += " Teams ??? ??."
    except Exception as exc:
        message += f" Teams ?? ?? ??: {exc}"
    st.session_state.send_result = message
    save_completion_history(st.session_state.get("completion_history", []))
    save_dashboard_state()

def get_action_label(row: dict[str, Any]) -> str:
    if row.get("rate", 0) >= 1:
        return "援먯껜"
    return "?뺤긽"


def render_kpis(enriched: list[dict[str, Any]]) -> None:
    replace_now = len([d for d in enriched if d["status"] == "replace"])
    due_soon = len([d for d in enriched if d["remainDays"] <= 3])
    avg_rate = round(sum(d["rate"] for d in enriched) / len(enriched) * 100) if enriched else 0
    cards = [
        ("愿由??좊Ъ", f"{len(enriched)} EA", "?ㅼ떆媛?愿由????),
        ("利됱떆 援먯껜", f"{replace_now} 嫄?, "?ъ슜瑜?湲곗?"),
        ("3????援먯껜?덉젙", f"{due_soon} 嫄?, "?좎“???꾩슂"),
        ("?됯퇏 ?ъ슜瑜?, f"{avg_rate}%", "?쇱씤 ?됯퇏"),
    ]
    cols = st.columns(4)
    for col, card in zip(cols, cards):
        title, value, sub = card
        col.metric(title, value, sub)


def render_status_badge(status: str) -> str:
    meta = STATUS_META[status]
    styles = STATUS_STYLES[status]
    return (
        f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
        f"background:{styles['badge_bg']};color:{styles['badge_text']};"
        f"border:1px solid {styles['badge_border']};font-weight:700;font-size:12px;'>"
        f"{meta['label']}</span>"
    )


def render_action_badge(status: str) -> str:
    if status == "replace":
        label = "援먯껜?꾩슂"
    elif status == "caution":
        label = "二쇱쓽"
    else:
        label = "遺덊븘??
    styles = STATUS_STYLES[status]
    return (
        f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
        f"background:{styles['badge_bg']};color:{styles['badge_text']};"
        f"border:1px solid {styles['badge_border']};font-weight:700;font-size:12px;'>"
        f"{label}</span>"
    )


def render_usage_bar(rate: float, status: str) -> str:
    if rate >= 1:
        styles = STATUS_STYLES["replace"]
    elif rate >= 0.6:
        styles = STATUS_STYLES["caution"]
    else:
        styles = STATUS_STYLES["normal"]
    width = max(0, min(100, round(rate * 100)))
    return (
        "<div style='display:flex;align-items:center;gap:12px;'>"
        "<div style='width:98px;height:12px;background:#e2e8f0;border-radius:999px;overflow:hidden;'>"
        f"<div style='width:{width}%;height:100%;background:{styles['bar']};border-radius:999px;'></div>"
        "</div>"
        f"<span style='font-weight:700;color:#0f172a;'>{width}%</span>"
        "</div>"
    )


def render_equipment_table(rows: list[dict[str, Any]]) -> None:
    st.markdown(
        """
        <style>
        [class*="st-key-table_action_"] div[data-testid="stButton"] > button[kind="secondary"] {
            background: #ecfdf5 !important;
            color: #047857 !important;
            border: 1px solid #a7f3d0 !important;
            border-radius: 999px !important;
            font-weight: 700 !important;
        }
        [class*="st-key-table_action_"] div[data-testid="stButton"] > button[kind="secondary"]:hover {
            background: #d1fae5 !important;
            color: #065f46 !important;
            border: 1px solid #6ee7b7 !important;
        }
        [class*="st-key-table_action_"] div[data-testid="stButton"] > button[kind="primary"] {
            background: #fff1f2 !important;
            color: #be123c !important;
            border: 1px solid #fda4af !important;
            border-radius: 999px !important;
            font-weight: 700 !important;
        }
        [class*="st-key-table_action_"] div[data-testid="stButton"] > button[kind="primary"]:hover {
            background: #ffe4e6 !important;
            color: #9f1239 !important;
            border: 1px solid #fb7185 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with st.container(border=True):
        st.markdown(
            """
            <div style="padding:14px 18px;border:1px solid #e2e8f0;border-radius:18px;background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);margin-bottom:14px;box-shadow:0 8px 24px rgba(15,23,42,0.04);">
              <div style="display:grid;grid-template-columns:0.8fr 1.2fr 1.4fr 1fr 1.2fr 1.1fr 1fr 1fr;gap:16px;font-size:13px;font-weight:700;color:#64748b;">
                <div>?쇱씤</div>
                <div>?ㅻ퉬</div>
                <div>?좊Ъ紐?/div>
                <div>湲곗?媛?/div>
                <div>?ъ슜瑜?/div>
                <div>?붿뿬?ъ슜??/div>
                <div>?덉륫援먯껜</div>
                <div>援먯껜?곹깭</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        for row in rows:
            line_label = "?ｌ?" if row["line"] == "?ｌ?" else "蹂대쭅"
            with st.container(border=True):
                row_cols = st.columns([0.8, 1.2, 1.4, 1.0, 1.2, 1.1, 1.0, 1.0])
                row_cols[0].markdown(
                    f"<span style='display:inline-block;padding:5px 10px;border-radius:999px;background:#eef2ff;color:#334155;font-size:12px;font-weight:700;'>{line_label}</span>",
                    unsafe_allow_html=True,
                )
                row_cols[1].markdown(f"**{row['machine']}**")
                row_cols[2].write(row["displayBladeName"])
                row_cols[3].markdown(f"**{row['displayStandard']}**")
                row_cols[4].markdown(render_usage_bar(row["rate"], row["status"]), unsafe_allow_html=True)
                row_cols[5].markdown(f"**{row['displayRemaining']}**")
                row_cols[6].write(row["predictedDate"])
                if get_action_label(row) == "援먯껜":
                    button_type = "primary"
                else:
                    button_type = "secondary"
                if row_cols[7].button(get_action_label(row), key=f"table_action_{row['id']}", use_container_width=True, type=button_type):
                    handle_action(row["id"])
                    st.rerun()


def main() -> None:
    init_state()
    latest_info = refresh_auto_sheet_target()
    auto_sheet_url = st.session_state.auto_sheet_url
    auto_sheet_name = st.session_state.auto_sheet_name
    auto_sheet_updated_at = st.session_state.auto_sheet_updated_at
    dataset_type = str(latest_info.get("dataset_type", "")).strip()
    current_snapshot_key = f"{auto_sheet_name}|{auto_sheet_updated_at}|{dataset_type}"
    has_sync_result = bool(st.session_state.get("last_sheet_sync_details")) and bool(st.session_state.get("last_sheet_sync_at"))
    if dataset_type == "蹂대쭅" and auto_sheet_updated_at and (
        st.session_state.get("boring_snapshot_loaded_key", "") != current_snapshot_key
        or not has_boring_history_rows(st.session_state.get("sheet_sync_history", []))
        or not any(
            is_boring_machine(str(row.get("machine", ""))) and parse_numeric_value(row.get("usage", 0)) > 0
            for row in st.session_state.get("equipment_data", [])
        )
    ):
        try:
            sync_from_google_sheet(
                auto_sheet_url,
                "auto",
                worksheet_name=auto_sheet_name or None,
                worksheet_gid=st.session_state.get("auto_sheet_gid") or None,
                silent=True,
            )
            st.session_state.boring_snapshot_loaded_key = current_snapshot_key
            st.session_state.last_applied_upload_at = auto_sheet_updated_at
            st.session_state.last_snapshot_sync_key = current_snapshot_key
            save_dashboard_state()
        except Exception:
            pass
    if auto_sheet_updated_at and (
        auto_sheet_updated_at != st.session_state.get("last_applied_upload_at", "")
        or not has_sync_result
        or (dataset_type == "蹂대쭅" and st.session_state.get("last_snapshot_sync_key", "") != current_snapshot_key)
    ):
        try:
            sync_from_google_sheet(
                auto_sheet_url,
                "auto",
                worksheet_name=auto_sheet_name or None,
                worksheet_gid=st.session_state.get("auto_sheet_gid") or None,
                silent=True,
            )
            st.session_state.last_applied_upload_at = auto_sheet_updated_at
            if dataset_type == "蹂대쭅":
                st.session_state.last_snapshot_sync_key = current_snapshot_key
            save_dashboard_state()
        except Exception:
            pass
    auto_sync_fragment()
    # Use the persisted sync history as the single source of truth for both
    # edge and boring so previous days' history does not get rebuilt away.
    effective_history = normalize_sheet_sync_history(
        st.session_state.get("sheet_sync_history", [])
    )
    if effective_history != st.session_state.get("sheet_sync_history", []):
        st.session_state.sheet_sync_history = effective_history
        save_sheet_sync_history(effective_history)
        save_dashboard_state()
    st.session_state.equipment_data = reconcile_edge_usage_from_history(
        st.session_state.equipment_data,
        effective_history,
        st.session_state.get("usage_reset_at", ""),
    )
    st.session_state.equipment_data = reconcile_boring_usage_from_history(
        st.session_state.equipment_data,
        effective_history,
        st.session_state.get("usage_reset_at", ""),
    )
    enriched = enrich_data(st.session_state.equipment_data)
    process_replace_alerts(enriched)

    st.title("?좊Ъ 援먯껜愿由???쒕낫??)
    st.caption("FURSYS 쨌 異⑹＜ 怨듭옣 쨌 ?덉쭏蹂댁쬆?")

    render_kpis(enriched)

    with st.sidebar:
        st.subheader("?꾪꽣")
        status_filter = st.selectbox("?곹깭", ["all", "normal", "caution", "replace"], format_func=lambda x: {"all": "?꾩껜 ?곹깭", "normal": "?뺤긽", "caution": "二쇱쓽", "replace": "援먯껜"}[x])
        search = st.text_input("?ㅻ퉬紐?寃??)

        st.divider()
        st.subheader("援ш? ?ㅽ봽?덈뱶?쒗듃 諛섏쁺")
        sheet_url = st.text_input("援ш? ?쒗듃 留곹겕", value=auto_sheet_url or DEFAULT_GOOGLE_SHEET_URL)
        all_machines = sorted({item["machine"] for item in st.session_state.equipment_data})
        target_options = ["?ｌ? ?꾩껜", "蹂대쭅 ?꾩껜", *all_machines]
        target_machine = st.selectbox("湲곕낯 ????ㅻ퉬", target_options, key="sheet_target_machine")
        if st.button("吏湲?諛섏쁺", use_container_width=True):
            try:
                sync_from_google_sheet(
                    sheet_url,
                    target_machine,
                    worksheet_name=auto_sheet_name or None,
                    worksheet_gid=st.session_state.get("auto_sheet_gid") or None,
                )
                st.rerun()
            except Exception as exc:
                st.session_state.send_result = f"援ш? ?ㅽ봽?덈뱶?쒗듃 ?숆린???ㅽ뙣: {exc}"
        st.caption(f"理쒓렐 ?숆린?? {st.session_state.last_sheet_sync_at or '?꾩쭅 ?놁쓬'}")
        if auto_sheet_name:
            st.caption(f"?먮룞 ?곌껐 ?쒗듃: {auto_sheet_name}")
        if auto_sheet_updated_at:
            st.caption(f"?먮룞 ?곌껐 媛깆떊: {auto_sheet_updated_at}")
        st.text_input("Teams Webhook URL", key="teams_webhook_url")
        if st.button("?ъ슜瑜?由ъ뀑", use_container_width=True):
            reset_all_usage_data()
            st.rerun()
        if st.button("?곗씠??諛섏쁺 ?대젰 由ъ뀑", use_container_width=True):
            reset_sheet_sync_history_data()
            st.rerun()
        if st.button("援먯껜?꾨즺 ?쒖젏 由ъ뀑", use_container_width=True):
            reset_completion_history_data()
            st.rerun()

    if st.session_state.send_result:
        st.info(st.session_state.send_result)

    if st.session_state.upload_summary:
        summary = st.session_state.upload_summary
        st.caption(
            f"理쒓렐 諛섏쁺: {summary['fileName']} / {summary['targetMachine']} / {summary['startDate']} ~ {summary['endDate']} / "
            f"{summary['periodDays']}??湲곗? / {summary['totalUsageM']:.3f} m"
        )

    filtered = [
        row
        for row in enriched
        if (status_filter == "all" or row["status"] == status_filter)
        and (not search.strip() or any(search.lower() in str(row[key]).lower() for key in ["machine", "bladeName", "line"]))
    ]
    top_priority = sorted(enriched, key=lambda row: (row["rate"] * 100 + row["quality"] * 10), reverse=True)[:5]

    left, right = st.columns([3.2, 1.2])
    with left:
        st.subheader("?ㅻ퉬蹂?援먯껜 ?꾪솴")
        available_lines = [line for line in LINE_FILTER_ORDER if any(row["line"] == line for row in enriched)]
        line_button_cols = st.columns(len(available_lines) + 1)
        if line_button_cols[0].button("?꾩껜", key="line_toggle_all", use_container_width=True, type="primary" if st.session_state.get("line_filter_toggle", "all") == "all" else "secondary"):
            st.session_state.line_filter_toggle = "all"
            st.session_state.line_machine_filter = "?꾩껜"
            st.rerun()
        for idx, line_name in enumerate(available_lines, start=1):
            active = st.session_state.get("line_filter_toggle", "all") == line_name
            if line_button_cols[idx].button(line_name, key=f"line_toggle_{line_name}", use_container_width=True, type="primary" if active else "secondary"):
                st.session_state.line_filter_toggle = line_name
                st.session_state.line_machine_filter = "?꾩껜"
                st.rerun()
        active_line_filter = st.session_state.get("line_filter_toggle", "all")
        if active_line_filter != "all":
            machine_options = ["?꾩껜", *[machine for machine in LINE_MACHINE_OPTIONS.get(active_line_filter, []) if any(row["machine"] == machine for row in enriched)]]
            st.selectbox(
                f"{active_line_filter} ?몃? ?좏깮",
                machine_options,
                key="line_machine_filter",
            )
        filtered = [
            row
            for row in filtered
            if active_line_filter == "all" or row["line"] == active_line_filter
        ]
        active_machine_filter = st.session_state.get("line_machine_filter", "?꾩껜")
        if active_line_filter != "all" and active_machine_filter != "?꾩껜":
            filtered = [row for row in filtered if row["machine"] == active_machine_filter]
        st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)
        render_equipment_table(filtered)

        st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)
        st.caption("?곗씠??諛섏쁺 ?대젰")
        if st.session_state.sheet_sync_history:
            history_df = pd.DataFrame(st.session_state.sheet_sync_history)
            ordered_columns = ["諛섏쁺?쒓컖", "?ㅻ퉬", "?좊Ъ紐?, "諛섏쁺 ?ъ슜??m)", "諛섏쁺 ?ъ슜????", "?곗씠??湲곗??쇱옄"]
            history_df = history_df.rename(columns={"?쒖옉??: "?곗씠??湲곗??쇱옄"})
            history_df = history_df[[column for column in ordered_columns if column in history_df.columns]]
            history_df["?ㅻ퉬"] = history_df["?ㅻ퉬"].map(normalize_machine_name)
            history_df["_line"] = history_df["?ㅻ퉬"].map(infer_line_from_machine)
            history_df = history_df[
                history_df["?ㅻ퉬"].apply(lambda machine: (active_line_filter == "all" or infer_line_from_machine(machine) == active_line_filter))
            ]
            if active_line_filter != "all" and active_machine_filter != "?꾩껜":
                history_df = history_df[history_df["?ㅻ퉬"] == active_machine_filter]
            history_filter_cols = st.columns(3)
            history_df = apply_date_dropdown_filter(history_df, "諛섏쁺?쒓컖", "history", history_filter_cols[0])
            history_df = expand_history_rows_by_blade(history_df)
            machine_options = ["?꾩껜", *sorted([value for value in history_df["?ㅻ퉬"].dropna().astype(str).unique() if value.strip()])]
            selected_history_machine = history_filter_cols[1].selectbox("?ㅻ퉬", machine_options, key="history_machine_filter")
            if selected_history_machine != "?꾩껜":
                history_df = history_df[history_df["?ㅻ퉬"] == selected_history_machine]
            blade_options = ["?꾩껜", *sorted([value for value in history_df["?좊Ъ紐?].dropna().astype(str).unique() if value.strip()])]
            selected_history_blade = history_filter_cols[2].selectbox("?좊Ъ紐?, blade_options, key="history_blade_filter")
            if selected_history_blade != "?꾩껜":
                history_df = history_df[history_df["?좊Ъ紐?] == selected_history_blade]
            history_df = aggregate_history_rows(history_df)
            history_df["_sort_time"] = pd.to_datetime(history_df["諛섏쁺?쒓컖"], errors="coerce")
            history_df["_machine_sort"] = history_df["?ㅻ퉬"].apply(get_machine_sort_key)
            history_df["_blade_sort"] = history_df["?좊Ъ紐?].apply(get_blade_sort_key)
            history_df = (
                history_df
                .sort_values(
                    by=["_sort_time", "_machine_sort", "_blade_sort", "?ㅻ퉬", "?좊Ъ紐?],
                    ascending=[False, True, True, True, True],
                    na_position="last",
                )
                .reset_index(drop=True)
            )
            for column in ["諛섏쁺 ?ъ슜??m)", "諛섏쁺 ?ъ슜????"]:
                if column in history_df.columns:
                    history_df[column] = history_df[column].where(history_df[column].notna(), "")
            history_df = history_df.drop(columns=["_line", "_sort_time", "_machine_sort", "_blade_sort"], errors="ignore")
            if not history_df.empty:
                st.dataframe(format_sync_display_dataframe(history_df), use_container_width=True)
            else:
                st.info("議곌굔??留욌뒗 諛섏쁺 ?대젰???놁뒿?덈떎.")
        else:
            st.info("?꾩쭅 諛섏쁺 ?대젰???놁뒿?덈떎.")

        st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)
        st.caption("援먯껜?꾨즺 ?쒖젏")
        if st.session_state.get("completion_history"):
            completion_df = pd.DataFrame(st.session_state.get("completion_history", []))
            ordered_columns = ["援먯껜?꾨즺?쒓컖", "?ㅻ퉬", "?좊Ъ紐?, "援먯껜 ?쒖젏 ?ъ슜??]
            completion_df = completion_df[[column for column in ordered_columns if column in completion_df.columns]]
            if "援먯껜 ?쒖젏 ?ъ슜?? in completion_df.columns:
                completion_df["援먯껜 ?쒖젏 ?ъ슜??] = completion_df["援먯껜 ?쒖젏 ?ъ슜??].where(completion_df["援먯껜 ?쒖젏 ?ъ슜??].notna(), "")
                completion_df["援먯껜 ?쒖젏 ?ъ슜??] = completion_df["援먯껜 ?쒖젏 ?ъ슜??].replace("None", "")
            completion_df["?ㅻ퉬"] = completion_df["?ㅻ퉬"].apply(normalize_machine_name)
            completion_df = completion_df[
                completion_df["?ㅻ퉬"].apply(lambda machine: (active_line_filter == "all" or infer_line_from_machine(machine) == active_line_filter))
            ]
            if active_line_filter != "all" and active_machine_filter != "?꾩껜":
                completion_df = completion_df[completion_df["?ㅻ퉬"] == active_machine_filter]
            completion_filter_cols = st.columns(3)
            completion_df = apply_date_dropdown_filter(completion_df, "援먯껜?꾨즺?쒓컖", "completion", completion_filter_cols[0])
            completion_machine_options = ["?꾩껜", *sorted([value for value in completion_df["?ㅻ퉬"].dropna().astype(str).unique() if value.strip()])]
            selected_completion_machine = completion_filter_cols[1].selectbox("?ㅻ퉬", completion_machine_options, key="completion_machine_filter")
            if selected_completion_machine != "?꾩껜":
                completion_df = completion_df[completion_df["?ㅻ퉬"] == selected_completion_machine]
            completion_blade_options = ["?꾩껜", *sorted([value for value in completion_df["?좊Ъ紐?].dropna().astype(str).unique() if value.strip()])]
            selected_completion_blade = completion_filter_cols[2].selectbox("?좊Ъ紐?, completion_blade_options, key="completion_blade_filter")
            if selected_completion_blade != "?꾩껜":
                completion_df = completion_df[completion_df["?좊Ъ紐?] == selected_completion_blade]
            if not completion_df.empty:
                st.dataframe(format_sync_display_dataframe(completion_df), use_container_width=True)
            else:
                st.info("議곌굔??留욌뒗 援먯껜?꾨즺 ?대젰???놁뒿?덈떎.")
        else:
            st.info("?꾩쭅 援먯껜?꾨즺 ?대젰???놁뒿?덈떎.")

    with right:
        st.subheader("援먯껜 ?곗꽑?쒖쐞 TOP 5")
        for index, row in enumerate(top_priority, start=1):
            with st.container(border=True):
                st.caption(f"#{index} 쨌 {row['line']}")
                st.markdown(f"**{row['machine']}**")
                st.write(row["displayBladeName"])
                st.write(f"湲곗?媛?{row['displayStandard']}")
                st.write(f"?ъ슜瑜?{round(row['rate'] * 100)}% 쨌 {row['predictedDate']}")
                st.write(f"?붿뿬 {row['displayRemaining']}")


if __name__ == "__main__":
    main()
