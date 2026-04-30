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

try:
    import gspread
except Exception:
    gspread = None





st.set_page_config(page_title="날물 교체관리 대시보드", layout="wide")
APP_DATA_VERSION = "2026-04-28-boring-history-refresh"





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

COMPLETION_HISTORY_WORKSHEET_NAME = "DASHBOARD_COMPLETION_HISTORY"

PERSIST_STATE_WORKSHEET_NAME = "DASHBOARD_PERSIST_STATE"

KST = ZoneInfo("Asia/Seoul")

STREAMLIT_APP_REVISION = "2026-04-27 16:05"

BORING_WORKSHEET_GID_BY_SYNC_TIME = {

    "2026-04-23 10:15:08": "1062250441",

    "2026-04-24 10:56:07": "1321320597",

}





BORING_MACHINE_CONFIG = [

    {"line": "수직", "machine": "수직 #1", "installDate": "2026-03-01"},

    {"line": "수직", "machine": "수직 #2", "installDate": "2026-03-04"},

    {"line": "수직", "machine": "수직 #3", "installDate": "2026-03-02"},

    {"line": "포인트", "machine": "포인트 #3", "installDate": "2026-02-28", "actionStep": ""},

    {"line": "양면", "machine": "양면 #26", "installDate": "2026-03-03"},

    {"line": "양면", "machine": "양면 #27", "installDate": "2026-03-05"},

    {"line": "런닝", "machine": "런닝 #19", "installDate": "2026-02-22"},

    {"line": "런닝", "machine": "런닝 #20", "installDate": "2026-03-06"},

    {"line": "런닝", "machine": "런닝 #21", "installDate": "2026-03-01"},

    {"line": "런닝", "machine": "런닝 #22", "installDate": "2026-03-04"},

    {"line": "런닝", "machine": "런닝 #23", "installDate": "2026-03-05"},

    {"line": "런닝", "machine": "런닝 #24", "installDate": "2026-03-02"},

]



BORING_BLADE_SPECS = [

    {"suffix": "035", "bladeName": "Φ35 날물", "standard": 10000, "avg7d": 420, "quality": 0, "spindle": "H1"},

    {"suffix": "020", "bladeName": "Φ20 날물", "standard": 10000, "avg7d": 320, "quality": 0, "spindle": "H2"},

    {"suffix": "012", "bladeName": "Φ12(관통) 날물", "standard": 10000, "avg7d": 410, "quality": 0, "spindle": "H3"},

    {"suffix": "008", "bladeName": "Φ8(관통) 날물", "standard": 10000, "avg7d": 355, "quality": 0, "spindle": "MAIN"},

    {"suffix": "015", "bladeName": "Φ15 날물", "standard": 10000, "avg7d": 300, "quality": 0, "spindle": "H4"},

    {"suffix": "005", "bladeName": "Φ5(관통) 날물", "standard": 10000, "avg7d": 280, "quality": 0, "spindle": "H5"},

]



BORING_HISTORY_BLADE_COLUMNS = [

    "Φ5(관통) 날물",

    "Φ8(관통) 날물",

    "Φ12(관통) 날물",

    "Φ15 날물",

    "Φ20 날물",

    "Φ35 날물",

]



BORING_HISTORY_BLADE_NAMES = [

    "Φ5(관통) 날물",

    "Φ8(관통) 날물",

    "Φ12(관통) 날물",

    "Φ15 날물",

    "Φ20 날물",

    "Φ35 날물",

]



EDGE_MACHINE_DEFAULTS = [

    {"line": "엣지", "machine": "엣지 #1", "spindle": "H1", "bladeCode": "AT-013-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-03", "usage": 0, "standard": 15000, "avg7d": 2000, "quality": 0},

    {"line": "엣지", "machine": "엣지 #2", "spindle": "H2", "bladeCode": "AT-014-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-05", "usage": 0, "standard": 15000, "avg7d": 2000, "quality": 0},

    {"line": "엣지", "machine": "엣지 #3,4", "spindle": "H1/H3", "bladeCode": "AT-015-016-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-06", "usage": 0, "standard": 90000, "avg7d": 15000, "quality": 0},

    {"line": "엣지", "machine": "엣지 #5", "spindle": "H2", "bladeCode": "AT-017-B", "bladeName": "AT 날물(후면)", "installDate": "2026-02-27", "usage": 0, "standard": 15000, "avg7d": 2125, "quality": 0},

    {"line": "엣지", "machine": "엣지 #6", "spindle": "MAIN-F", "bladeCode": "AT-018-F", "bladeName": "AT 날물(전면)", "installDate": "2026-03-26", "usage": 0, "standard": 75000, "avg7d": 10000, "quality": 0, "actionStep": ""},

    {"line": "엣지", "machine": "엣지 #6", "spindle": "MAIN-B", "bladeCode": "AT-018-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-26", "usage": 0, "standard": 75000, "avg7d": 10000, "quality": 0, "actionStep": ""},

]





def build_initial_raw_data() -> list[dict[str, Any]]:

    rows: list[dict[str, Any]] = []

    row_id = 1

    line_prefix = {"수직": "V", "포인트": "P", "양면": "D", "런닝": "R"}

    for machine_config in BORING_MACHINE_CONFIG:

        for blade_spec in BORING_BLADE_SPECS:

            standard = blade_spec["standard"]

            if machine_config["line"] == "런닝" and blade_spec["bladeName"] == "Φ5(관통) 날물":

                standard = 50000

            row = {

                "id": row_id,

                "plant": "충주",

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

        rows.append({"id": row_id, "plant": "충주", **edge_config})

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

    legacy_edge_group_machines = {"엣지 #3", "엣지 #4"}

    existing_rows = [

        row

        for row in data

        if (

            isinstance(row, dict)

            and str(row.get("machine", "")).strip() not in legacy_edge_group_machines

            and not (

                str(row.get("line", "")).strip() == "엣지"

                and str(row.get("bladeName", "")).strip() == "AT 날물"

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

    st.session_state.send_result = "설비 사용률을 모두 리셋했습니다."

    save_dashboard_state()





def reset_last_sheet_sync_result() -> None:

    st.session_state.last_sheet_sync_details = []

    st.session_state.last_sheet_sync_at = ""

    st.session_state.send_result = "데이터 반영 이력을 리셋했습니다."

    save_dashboard_state()





def reset_sheet_sync_history_data() -> None:

    st.session_state.sheet_sync_history = []

    save_sheet_sync_history([])

    st.session_state.send_result = "데이터 반영 이력을 리셋했습니다."

    save_dashboard_state()





def reset_completion_history_data() -> None:

    st.session_state.completion_history = []

    save_completion_history([])

    st.session_state.send_result = "교체완료 시점을 리셋했습니다."

    save_dashboard_state()





EDGE_UPLOAD_RULES = {

    "엣지 #1": {"periodDays": 15},

    "엣지 #2": {"periodDays": 15},

    "엣지 #3,4": {"periodDays": 7},

    "엣지 #5": {"periodDays": 15},

    "엣지 #6": {"periodDays": 7},

}



MACHINE_GROUPS = {

    "엣지 전체": ["엣지 #1", "엣지 #2", "엣지 #3,4", "엣지 #5", "엣지 #6"],

    "보링 전체": [

        "수직 #1",

        "수직 #2",

        "수직 #3",

        "포인트 #3",

        "양면 #26",

        "양면 #27",

        "런닝 #19",

        "런닝 #20",

        "런닝 #21",

        "런닝 #22",

        "런닝 #23",

        "런닝 #24",

    ],

}



EDGE_FIXED_STANDARDS = {

    "엣지 #1": 15000,

    "엣지 #2": 15000,

    "엣지 #3,4": 90000,

    "엣지 #5": 15000,

    "엣지 #6": 75000,

}



STATUS_META = {

    "normal": {"label": "정상", "color": "green"},

    "caution": {"label": "주의", "color": "orange"},

    "replace": {"label": "교체", "color": "red"},

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



LINE_FILTER_ORDER = ["엣지", "런닝", "양면", "포인트", "수직"]

LINE_MACHINE_OPTIONS = {

    "엣지": ["엣지 #1", "엣지 #2", "엣지 #3,4", "엣지 #5", "엣지 #6"],

    "런닝": ["런닝 #19", "런닝 #20", "런닝 #21", "런닝 #22", "런닝 #23", "런닝 #24"],

    "양면": ["양면 #26", "양면 #27"],

    "포인트": ["포인트 #3"],

    "수직": ["수직 #1", "수직 #2", "수직 #3"],

}





def init_state() -> None:

    saved_state = load_dashboard_state()

    if "equipment_data" not in st.session_state:

        raw_equipment = saved_state.get("equipment_data", INITIAL_RAW_DATA.copy())

        st.session_state.equipment_data = ensure_default_equipment_rows(raw_equipment if isinstance(raw_equipment, list) else INITIAL_RAW_DATA.copy())

    if "send_result" not in st.session_state:

        st.session_state.send_result = ""

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

    else:

        persisted_history = load_sheet_sync_history()

        if persisted_history:

            st.session_state.sheet_sync_history = normalize_sheet_sync_history(persisted_history)

    if "completion_history" not in st.session_state:

        raw_completion = merge_completion_history(
            saved_state.get("completion_history", []),
            load_completion_history(),
        )

        st.session_state.completion_history = normalize_completion_history(raw_completion if isinstance(raw_completion, list) else [])

    else:

        persisted_completion = load_completion_history()

        if persisted_completion:

            st.session_state.completion_history = merge_completion_history(

                st.session_state.get("completion_history", []),

                persisted_completion,

            )

    if "machine_reset_at" not in st.session_state:

        raw_machine_reset_at = saved_state.get("machine_reset_at", {})

        st.session_state.machine_reset_at = raw_machine_reset_at if isinstance(raw_machine_reset_at, dict) else {}

    if "blade_reset_at" not in st.session_state:

        raw_blade_reset_at = saved_state.get("blade_reset_at", {})

        st.session_state.blade_reset_at = raw_blade_reset_at if isinstance(raw_blade_reset_at, dict) else {}

    st.session_state.blade_reset_at = rebuild_blade_reset_at_from_completion_history(
        st.session_state.get("blade_reset_at", {}),
        st.session_state.get("completion_history", []),
    )

    if "replacement_assignees" not in st.session_state:

        raw_replacement_assignees = saved_state.get("replacement_assignees", {})

        st.session_state.replacement_assignees = raw_replacement_assignees if isinstance(raw_replacement_assignees, dict) else {}

    if "assignee_widget_reset_versions" not in st.session_state:

        raw_assignee_widget_reset_versions = saved_state.get("assignee_widget_reset_versions", {})

        st.session_state.assignee_widget_reset_versions = raw_assignee_widget_reset_versions if isinstance(raw_assignee_widget_reset_versions, dict) else {}

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

        st.session_state.line_machine_filter = saved_state.get("line_machine_filter", "전체")

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

    if rate >= 0.7:

        return "caution"

    return "normal"





def days_left(remaining: float, avg7d: float) -> int:

    if avg7d <= 0:

        return 999

    return max(0, math.ceil(remaining / avg7d))





def format_cycle_value(row: dict[str, Any], value: float) -> str:

    if row["line"] == "엣지":

        return f"{round(value):,}m"

    return f"{value:,.0f} 회"





def get_display_blade_name(row: dict[str, Any]) -> str:

    edge = "엣지"

    gwantong = "(관통)"

    nalmul = " 날물"

    if row["line"] == edge:

        blade_name = str(row.get("bladeName", "")).strip()

        if blade_name:

            return blade_name

        return "AT 날물(후면)"

    if gwantong in row["bladeName"]:

        return row["bladeName"]

    if any(token in row["bladeName"] for token in ["Φ5", "Φ8", "Φ12"]):

        return row["bladeName"].replace(nalmul, "") + gwantong + nalmul

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

    if normalized_machine.startswith(("수직", "양면", "포인트", "런닝")):

        return [

            "Φ35 날물",

            "Φ20 날물",

            "Φ12(관통) 날물",

            "Φ8(관통) 날물",

            "Φ15 날물",

            "Φ5(관통) 날물",

        ]

    if normalized_machine == "엣지 #6":

        return ["AT 날물(전면)", "AT 날물(후면)"]

    if normalized_machine.startswith("엣지"):

        return ["AT 날물(후면)"]

    return []





def get_machine_sort_key(machine: str) -> tuple[int, int]:

    normalized_machine = normalize_machine_name(machine)

    line_order = {"엣지": 0, "런닝": 1, "양면": 2, "포인트": 3, "수직": 4}

    line_name = infer_line_from_machine(normalized_machine)

    digits = "".join(ch for ch in normalized_machine if ch.isdigit())

    machine_no = int(digits) if digits else 999

    return line_order.get(line_name, 99), machine_no





def get_blade_sort_key(blade_name: str) -> int:

    preferred_order = [

        "Φ35 날물",

        "Φ20 날물",

        "Φ12(관통) 날물",

        "Φ8(관통) 날물",

        "Φ15 날물",

        "Φ5(관통) 날물",

        "AT 날물(전면)",

        "AT 날물(후면)",

    ]

    try:

        return preferred_order.index(str(blade_name).strip())

    except ValueError:

        return len(preferred_order)





def normalize_edge_blade_name(machine: str, blade_name: Any) -> str:

    normalized_machine = normalize_machine_name(machine)

    raw_blade_name = str(blade_name or "").strip()

    if normalized_machine == "엣지 #6":

        if "전면" in raw_blade_name:

            return "AT 날물(전면)"

        if "후면" in raw_blade_name:

            return "AT 날물(후면)"

        return raw_blade_name

    if normalized_machine.startswith("엣지"):

        return "AT 날물(후면)"

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

    save_remote_sheet_sync_history(normalized)


def save_remote_sheet_sync_history(history: list[dict[str, Any]]) -> None:

    spreadsheet = get_google_spreadsheet()

    if spreadsheet is None:

        return

    try:

        normalized = normalize_sheet_sync_history(history)

        columns = ["반영시각", "대상", "설비", "날물명", "데이터 기준일자", "반영 사용량(m)", "반영 사용량(회)"]

        rows = [

            [

                str(entry.get(column, entry.get("시작일", "") if column == "데이터 기준일자" else "")).strip()

                for column in columns

            ]

            for entry in normalized

        ]

        worksheet = get_or_create_worksheet(

            spreadsheet,

            SYNC_HISTORY_WORKSHEET_NAME,

            rows=max(len(rows) + 100, 1000),

            cols=len(columns) + 2,

        )

        worksheet.clear()

        worksheet.update([columns, *rows], "A1", value_input_option="RAW")

    except Exception:

        return





def load_completion_history() -> list[dict[str, Any]]:

    remote_history = load_remote_completion_history()

    if not COMPLETION_HISTORY_PATH.exists():

        return remote_history

    try:

        data = json.loads(COMPLETION_HISTORY_PATH.read_text(encoding="utf-8"))

        local_history = normalize_completion_history(data if isinstance(data, list) else [])

        return merge_completion_history(local_history, remote_history)

    except Exception:

        return remote_history





def normalize_completion_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:

    normalized: list[dict[str, Any]] = []

    for entry in history:

        if not isinstance(entry, dict):

            continue

        completed_at = str(entry.get("교체완료시각", entry.get("??????", ""))).strip()

        machine = str(entry.get("설비", entry.get("??", ""))).strip()

        blade_name = str(entry.get("날물명", entry.get("???", ""))).strip()

        usage_label = str(entry.get("교체 시점 사용량", entry.get("?? ?? ???", ""))).strip()

        assignee = str(entry.get("담당자", entry.get("담당", ""))).strip()

        note = str(entry.get("비고", entry.get("원인", ""))).strip()

        if not completed_at and not machine and not blade_name:

            continue

        normalized.append(

            {

                "교체완료시각": completed_at,

                "설비": normalize_machine_name(machine),

                "날물명": blade_name,

                "교체 시점 사용량": usage_label,

                "담당자": assignee,

                "비고": note,

            }

        )

    return normalized



def load_sheet_config() -> dict[str, str]:

    config_path = WORK_DIR / "sheet_config.json"

    if not config_path.exists():

        return {}

    try:

        config = json.loads(config_path.read_text(encoding="utf-8"))

        return config if isinstance(config, dict) else {}

    except Exception:

        return {}



def get_google_spreadsheet():

    if gspread is None:

        return None

    config = load_sheet_config()

    spreadsheet_name = str(config.get("spreadsheet_name", "")).strip() or "엣지 ERP 자동업로드"

    credentials_path = Path(str(config.get("credentials_path", "")).strip())

    if not credentials_path.exists():

        credentials_path = WORK_DIR / "google-service-account.json"

    if not credentials_path.exists():

        return None

    try:

        client = gspread.service_account(filename=str(credentials_path))

        return client.open(spreadsheet_name)

    except Exception:

        return None



def get_or_create_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 20):

    try:

        return spreadsheet.worksheet(title)

    except Exception:

        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))



def load_remote_completion_history() -> list[dict[str, Any]]:

    spreadsheet = get_google_spreadsheet()

    if spreadsheet is None:

        return []

    try:

        worksheet = spreadsheet.worksheet(COMPLETION_HISTORY_WORKSHEET_NAME)

        return normalize_completion_history(worksheet.get_all_records())

    except Exception:

        return []



def save_remote_completion_history(history: list[dict[str, Any]]) -> None:

    spreadsheet = get_google_spreadsheet()

    if spreadsheet is None:

        return

    try:

        normalized = normalize_completion_history(history)

        columns = ["교체완료시각", "설비", "날물명", "교체 시점 사용량", "담당자", "비고"]

        rows = [
            [
                str(entry.get(column, "")).strip()
                for column in columns
            ]
            for entry in normalized
        ]

        worksheet = get_or_create_worksheet(
            spreadsheet,
            COMPLETION_HISTORY_WORKSHEET_NAME,
            rows=max(len(rows) + 100, 1000),
            cols=len(columns) + 2,
        )

        worksheet.clear()

        worksheet.update([columns, *rows], "A1", value_input_option="RAW")

    except Exception:

        return



def decode_persist_value(value: Any) -> Any:

    text = str(value or "").strip()

    if not text:

        return ""

    try:

        return json.loads(text)

    except Exception:

        return text



def load_remote_dashboard_state() -> dict[str, Any]:

    spreadsheet = get_google_spreadsheet()

    if spreadsheet is None:

        return {}

    try:

        worksheet = spreadsheet.worksheet(PERSIST_STATE_WORKSHEET_NAME)

        records = worksheet.get_all_records()

        state: dict[str, Any] = {}

        for record in records:

            key = str(record.get("key", "")).strip()

            if not key:

                continue

            state[key] = decode_persist_value(record.get("value", ""))

        return state

    except Exception:

        return {}



def save_remote_dashboard_state(data: dict[str, Any]) -> None:

    spreadsheet = get_google_spreadsheet()

    if spreadsheet is None:

        return

    persist_keys = [
        "usage_reset_at",
        "machine_reset_at",
        "blade_reset_at",
        "replacement_assignees",
        "assignee_widget_reset_versions",
        "replace_alert_history",
        "last_applied_upload_at",
        "last_snapshot_sync_key",
        "boring_snapshot_loaded_key",
        "line_filter_toggle",
        "line_machine_filter",
    ]

    try:

        existing_state = load_remote_dashboard_state()

        for merge_key in ["machine_reset_at", "blade_reset_at", "replace_alert_history"]:

            existing_value = existing_state.get(merge_key, {})

            next_value = data.get(merge_key, {})

            if isinstance(existing_value, dict) and isinstance(next_value, dict):

                data[merge_key] = {**existing_value, **next_value}

        rows = [
            [
                key,
                json.dumps(data.get(key, ""), ensure_ascii=False),
            ]
            for key in persist_keys
        ]

        worksheet = get_or_create_worksheet(
            spreadsheet,
            PERSIST_STATE_WORKSHEET_NAME,
            rows=max(len(rows) + 100, 1000),
            cols=4,
        )

        worksheet.clear()

        worksheet.update([["key", "value"], *rows], "A1", value_input_option="RAW")

    except Exception:

        return



def merge_completion_history(*history_lists: list[dict[str, Any]]) -> list[dict[str, Any]]:

    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for history in history_lists:

        for entry in normalize_completion_history(history):

            key = (
                str(entry.get("교체완료시각", "")).strip(),
                str(entry.get("설비", "")).strip(),
                str(entry.get("날물명", "")).strip(),
                str(entry.get("교체 시점 사용량", "")).strip(),
            )

            merged[key] = entry

    return sorted(merged.values(), key=lambda row: str(row.get("교체완료시각", "")), reverse=True)



def rebuild_blade_reset_at_from_completion_history(
    blade_reset_at: dict[str, str] | None,
    completion_history: list[dict[str, Any]],
) -> dict[str, str]:

    rebuilt = dict(blade_reset_at or {})

    for entry in normalize_completion_history(completion_history):

        completed_at = str(entry.get("교체완료시각", "")).strip()

        machine = normalize_machine_name(str(entry.get("설비", "")).strip())

        blade_name = str(entry.get("날물명", "")).strip()

        if not completed_at or not machine or not blade_name:

            continue

        key = f"{machine}|{blade_name}"

        if completed_at > str(rebuilt.get(key, "")).strip():

            rebuilt[key] = completed_at

    return rebuilt



def save_completion_history(history: list[dict[str, Any]]) -> None:

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    normalized = merge_completion_history(normalize_completion_history(history), load_remote_completion_history())

    COMPLETION_HISTORY_PATH.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")

    save_remote_completion_history(normalized)





def normalize_sheet_sync_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:

    blade_map = {
        "?5(??) ??": "Φ5(관통) 날물",
        "?8(??) ??": "Φ8(관통) 날물",
        "?12(??) ??": "Φ12(관통) 날물",
        "?15 ??": "Φ15 날물",
        "?20 ??": "Φ20 날물",
        "?35 ??": "Φ35 날물",
    }

    def pick(entry: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in entry:
                return entry.get(key)
        return ""

    def is_date_like(value: str) -> bool:
        return len(value) == 10 and value[4:5] == "-" and value[7:8] == "-"

    def infer_legacy_machine(raw_machine: str, raw_blade: str) -> str:
        compact = raw_machine.replace(" ", "")
        digits = ''.join(ch for ch in compact if ch.isdigit())
        if not digits:
            return normalize_machine_name(raw_machine)

        if raw_blade in blade_map or raw_blade.startswith("Φ"):
            if "??" in raw_machine or "수직" in raw_machine:
                return f"수직 #{digits[:1]}"
            if "???" in raw_machine or "포인트" in raw_machine:
                return "포인트 #3"
            if digits in {"26", "27"}:
                return f"양면 #{digits}"
            if digits in {"19", "20", "21", "22", "23", "24"}:
                return f"런닝 #{digits}"
            if digits in {"1", "2"}:
                return f"수직 #{digits}"
            if digits == "3":
                return "포인트 #3"

        if is_date_like(raw_blade):
            if digits == "1":
                return "엣지 #1"
            if digits == "2":
                return "엣지 #2"
            if digits == "3" or digits == "4":
                return "엣지 #3,4"
            if digits == "5":
                return "엣지 #5"
            if digits == "6":
                return "엣지 #6"

        return normalize_machine_name(raw_machine)

    normalized: list[dict[str, Any]] = []

    for entry in history:
        if not isinstance(entry, dict):
            continue

        sync_at = str(pick(entry, "반영시각", "????", "??????")).strip()
        raw_machine = str(pick(entry, "설비", "??", "???")).strip()
        raw_blade = str(pick(entry, "날물명", "???", "?????")).strip()
        target = str(pick(entry, "대상", "????", "????")).strip()
        usage_m = pick(entry, "반영 사용량(m)", "?? ???(m)", "??? ?????m)")
        usage_count = pick(entry, "반영 사용량(회)", "?? ???(?)", "??? ???????")
        start_date = str(pick(entry, "데이터 기준일자", "시작일", "??? ????", "?????")).strip()
        if not start_date and len(sync_at) >= 10:
            start_date = sync_at[:10]

        machine = infer_legacy_machine(raw_machine, raw_blade)
        blade_name = blade_map.get(raw_blade, raw_blade)

        if not machine:
            continue

        is_boring = machine.startswith(("수직", "포인트", "런닝", "양면"))
        is_edge = machine.startswith("엣지")

        if is_boring:
            target = "보링 전체"
            usage_m = ""
            blade_name = normalize_boring_blade_name(blade_name)
        elif is_edge:
            target = "엣지 전체"
            usage_count = ""
            if is_date_like(blade_name):
                blade_name = ""
            blade_name = normalize_edge_blade_name(machine, blade_name)
            if not str(blade_name).strip():
                blade_name = get_machine_blade_summary(machine, INITIAL_RAW_DATA)
        else:
            continue

        normalized.append({
            "반영시각": sync_at,
            "대상": target,
            "설비": machine,
            "날물명": blade_name,
            "반영 사용량(m)": usage_m,
            "반영 사용량(회)": usage_count,
            "데이터 기준일자": start_date,
        })

    return normalized


def merge_sheet_sync_history(existing_history: list[dict[str, Any]], new_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:

    existing_rows = normalize_sheet_sync_history(existing_history)

    new_rows = normalize_sheet_sync_history(new_entries)

    if not existing_rows and not new_rows:

        return []



    key_columns = ["반영시각", "대상", "설비", "날물명"]

    if new_rows:

        replacement_keys = {

            tuple(str(row.get(column, "")).strip() for column in key_columns)

            for row in new_rows

        }

        existing_rows = [

            row

            for row in existing_rows

            if tuple(str(row.get(column, "")).strip() for column in key_columns) not in replacement_keys

        ]



    history_df = pd.DataFrame([*existing_rows, *new_rows])

    if history_df.empty:

        return []



    history_df["_sort_time"] = pd.to_datetime(history_df["반영시각"], errors="coerce")

    history_df = history_df.sort_values(by=["_sort_time", "반영시각", "설비", "날물명"], ascending=[True, True, True, True], na_position="last")

    history_df = history_df.drop(columns=["_sort_time"], errors="ignore")

    return history_df.to_dict(orient="records")





def normalize_last_sheet_sync_details(details: list[dict[str, Any]]) -> list[dict[str, Any]]:

    normalized: list[dict[str, Any]] = []

    for entry in details:

        if not isinstance(entry, dict):

            continue

        machine = normalize_machine_name(str(entry.get("machine", entry.get("설비", entry.get("?ㅻ퉬", "")))).strip())

        blade_name = entry.get("blade_name", entry.get("날물명", entry.get("?좊Ъ紐?", "")))

        usage_m = entry.get("usage_m", entry.get("반영 사용량(m)", entry.get("諛섏쁺 ?ъ슜??m)", "")))

        usage_count = entry.get("usage_count", entry.get("반영 사용량(회)", entry.get("諛섏쁺 ?ъ슜????", "")))

        start_date = entry.get("start_date", entry.get("시작일", entry.get("?쒖옉??", "")))



        is_boring = machine.startswith(("수직", "포인트", "런닝", "양면", "?섏쭅", "?ъ씤??", "?곕떇", "?묐㈃"))

        is_edge = machine.startswith(("엣지", "?ｌ?"))



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



    latest_sync_at = normalize_display_timestamp(history[0].get("반영시각", ""))

    if not latest_sync_at:

        return False



    latest_entries: list[dict[str, Any]] = []

    for entry in history:

        if str(entry.get("반영시각", "")).strip() != latest_sync_at:

            break

        latest_entries.append(entry)



    if not latest_entries:

        return False



    st.session_state.last_sheet_sync_at = latest_sync_at

    st.session_state.last_sheet_sync_details = normalize_last_sheet_sync_details(

        [

            {

                "machine": entry.get("설비", ""),

                "blade_name": entry.get("날물명", ""),

                "usage_m": entry.get("반영 사용량(m)", ""),

                "usage_count": entry.get("반영 사용량(회)", ""),

                "start_date": entry.get("시작일", ""),

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

    if "반영 사용량(m)" in display_df.columns:

        display_df["반영 사용량(m)"] = display_df["반영 사용량(m)"].apply(

            lambda value: "" if value in ("", None) or pd.isna(value) else f"{float(value):.2f}".rstrip("0").rstrip(".")

        )

    if "반영 사용량(회)" in display_df.columns:

        display_df["반영 사용량(회)"] = display_df["반영 사용량(회)"].apply(

            lambda value: "" if value in ("", None) or pd.isna(value) else str(int(round(float(value))))

        )

    return display_df





def load_dashboard_state() -> dict[str, Any]:

    remote_state = load_remote_dashboard_state()

    local_state: dict[str, Any] = {}

    if not DASHBOARD_STATE_PATH.exists():

        return remote_state

    try:

        data = json.loads(DASHBOARD_STATE_PATH.read_text(encoding="utf-8"))

        local_state = data if isinstance(data, dict) else {}

    except Exception:

        local_state = {}

    if remote_state:

        local_state.update(remote_state)

    return local_state





def save_dashboard_state() -> None:

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    completion_history = normalize_completion_history(st.session_state.get("completion_history", []))

    blade_reset_at = rebuild_blade_reset_at_from_completion_history(
        st.session_state.get("blade_reset_at", {}),
        completion_history,
    )

    st.session_state.blade_reset_at = blade_reset_at

    data = {

        "equipment_data": st.session_state.get("equipment_data", INITIAL_RAW_DATA),

        "send_result": st.session_state.get("send_result", ""),

        "replace_alert_history": st.session_state.get("replace_alert_history", {}),

        "last_sheet_sync_at": st.session_state.get("last_sheet_sync_at", ""),

        "last_sheet_sync_details": normalize_last_sheet_sync_details(st.session_state.get("last_sheet_sync_details", [])),

        "sheet_sync_history": normalize_sheet_sync_history(st.session_state.get("sheet_sync_history", [])),

        "completion_history": completion_history,

        "replacement_assignees": st.session_state.get("replacement_assignees", {}),

        "assignee_widget_reset_versions": st.session_state.get("assignee_widget_reset_versions", {}),

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

        "blade_reset_at": blade_reset_at,

        "line_filter_toggle": st.session_state.get("line_filter_toggle", "all"),

        "line_machine_filter": st.session_state.get("line_machine_filter", "전체"),

    }

    DASHBOARD_STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    save_remote_dashboard_state(data)





def reconcile_edge_usage_from_history(data: list[dict[str, Any]], history: list[dict[str, Any]], reset_at: str = "") -> list[dict[str, Any]]:

    aggregated: dict[tuple[str, str], dict[str, Any]] = {}

    machine_reset_at = st.session_state.get("machine_reset_at", {})

    blade_reset_at = st.session_state.get("blade_reset_at", {})

    for entry in normalize_sheet_sync_history(history):

        sync_at = str(entry.get("\ubc18\uc601\uc2dc\uac01", "")).strip()

        if reset_at and sync_at and sync_at <= reset_at:

            continue

        machine = str(entry.get("\uc124\ube44", "")).strip()

        machine_cutoff = str(machine_reset_at.get(machine, "")).strip()

        if machine_cutoff and sync_at and sync_at <= machine_cutoff:

            continue

        blade_name = str(entry.get("\ub0a0\ubb3c\uba85", "")).strip()

        blade_cutoff = str(blade_reset_at.get(f"{machine}|{blade_name}", "")).strip()

        if blade_cutoff and sync_at and sync_at <= blade_cutoff:

            continue

        usage_m = parse_numeric_value(entry.get("\ubc18\uc601 \uc0ac\uc6a9\ub7c9(m)", 0))

        start_date = str(entry.get("\ub370\uc774\ud130 \uae30\uc900\uc77c\uc790", entry.get("\uc2dc\uc791\uc77c", ""))).strip()

        if not machine.startswith("\uc5e3\uc9c0") or not blade_name or usage_m <= 0:

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

        if str(item.get("line", "")).strip() != "\uc5e3\uc9c0":

            next_rows.append(item)

            continue

        blade_reset_key = f"{key[0]}|{key[1]}"

        if key not in aggregated:

            if str(blade_reset_at.get(blade_reset_key, "")).strip():

                next_rows.append(

                    {

                        **item,

                        "usage": 0,

                        "quality": 0,

                        "standard": EDGE_FIXED_STANDARDS.get(item["machine"], item["standard"]),

                        "actionStep": "completed",

                    }

                )

            else:

                next_rows.append(item)

            continue

        total_usage = round(aggregated[key]["usage"], 3)

        period_days = EDGE_UPLOAD_RULES.get(item["machine"], {"periodDays": 7})["periodDays"]

        action_step = "" if total_usage > 0 else item.get("actionStep", "")

        next_rows.append(

            {

                **item,

                "usage": total_usage,

                "standard": EDGE_FIXED_STANDARDS.get(item["machine"], item["standard"]),

                "avg7d": max(1, round(total_usage / period_days, 3)),

                "installDate": aggregated[key]["start_date"] or item.get("installDate", ""),

                "actionStep": action_step,

            }

        )

    return next_rows


def reconcile_boring_usage_from_history(data: list[dict[str, Any]], history: list[dict[str, Any]], reset_at: str = "") -> list[dict[str, Any]]:

    aggregated: dict[tuple[str, str], dict[str, Any]] = {}

    machine_reset_at = st.session_state.get("machine_reset_at", {})

    blade_reset_at = st.session_state.get("blade_reset_at", {})

    for entry in normalize_sheet_sync_history(history):

        sync_at = str(entry.get("\ubc18\uc601\uc2dc\uac01", "")).strip()

        if reset_at and sync_at and sync_at <= reset_at:

            continue

        machine = str(entry.get("\uc124\ube44", "")).strip()

        machine_cutoff = str(machine_reset_at.get(machine, "")).strip()

        if machine_cutoff and sync_at and sync_at <= machine_cutoff:

            continue

        if not machine.startswith(("\uc218\uc9c1", "\ud3ec\uc778\ud2b8", "\ub7f0\ub2dd", "\uc591\uba74")):

            continue

        blade_name = normalize_boring_blade_name(str(entry.get("\ub0a0\ubb3c\uba85", "")).strip())

        blade_cutoff = str(blade_reset_at.get(f"{machine}|{blade_name}", "")).strip()

        if blade_cutoff and sync_at and sync_at <= blade_cutoff:

            continue

        usage_count = parse_numeric_value(entry.get("\ubc18\uc601 \uc0ac\uc6a9\ub7c9(\ud68c)", 0))

        start_date = str(entry.get("\ub370\uc774\ud130 \uae30\uc900\uc77c\uc790", entry.get("\uc2dc\uc791\uc77c", ""))).strip()

        if not blade_name:

            continue

        key = (machine, blade_name)

        aggregated.setdefault(key, {"usage": 0.0, "start_date": ""})

        aggregated[key]["usage"] += usage_count

        if start_date:

            current_start = aggregated[key]["start_date"]

            aggregated[key]["start_date"] = min(current_start, start_date) if current_start else start_date

    if not aggregated:

        return data

    next_rows: list[dict[str, Any]] = []

    for item in data:

        machine = str(item.get("machine", "")).strip()

        if not machine.startswith(("\uc218\uc9c1", "\ud3ec\uc778\ud2b8", "\ub7f0\ub2dd", "\uc591\uba74")):

            next_rows.append(item)

            continue

        blade_name = normalize_boring_blade_name(get_display_blade_name(item))

        key = (machine, blade_name)

        total_usage = round(aggregated.get(key, {"usage": 0.0})["usage"], 3)

        blade_reset_key = f"{machine}|{blade_name}"

        has_blade_reset = bool(str(blade_reset_at.get(blade_reset_key, "")).strip())

        action_step = "" if total_usage > 0 else ("completed" if has_blade_reset else item.get("actionStep", ""))

        next_rows.append(

            {

                **item,

                "usage": total_usage,

                "standard": get_boring_standard(machine, blade_name),

                "avg7d": max(0, round(total_usage / 7, 3)),

                "installDate": aggregated.get(key, {}).get("start_date") or item.get("installDate", ""),

                "actionStep": action_step,

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

        remaining = 0 if standard and row["usage"] >= standard else max(0, standard - row["usage"])

        remain_days = days_left(remaining, row["avg7d"])

        enriched.append(

            {

                **row,

                "rate": rate,

                "remaining": remaining,

                "remainDays": remain_days,

                "predictedDate": "-" if remain_days == 999 else f"{remain_days}일 후",

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

        "엣지밴더#1": "엣지 #1",

        "엣지밴더#2": "엣지 #2",

        "엣지#3,4": "엣지 #3,4",

        "엣지#3": "엣지 #3,4",

        "엣지#4": "엣지 #3,4",

        "신규엣지밴더#3": "엣지 #3,4",

        "신규엣지밴더#4": "엣지 #3,4",

        "신규엣지밴더#5": "엣지 #5",

        "더블엣지밴더#6": "엣지 #6",

    }

    if compact in edge_aliases:

        return edge_aliases[compact]

    boring_aliases = {

        "NC보링기수직#1": "수직 #1",

        "NC보링기수직#2": "수직 #2",

        "NC보링기수직#3": "수직 #3",

        "NC보링기#3(포인트보링기)": "포인트 #3",

        "NC보링기#19": "런닝 #19",

        "NC보링기#20": "런닝 #20",

        "NC보링기#21": "런닝 #21",

        "NC보링기#22": "런닝 #22",

        "NC보링기#23": "런닝 #23",

        "NC보링기#24": "런닝 #24",

        "NC보링기#26(신규양면보링기)": "양면 #26",

        "NC보링기#27(신규양면보링기)": "양면 #27",

    }

    if compact in boring_aliases:

        return boring_aliases[compact]

    if compact.startswith("NC보링기수직#"):

        digits = "".join(ch for ch in compact if ch.isdigit())

        if digits:

            return f"수직 #{digits[-1]}"

    if "A동" in raw or "A동" in compact:

        return raw

    if compact.startswith("NC보링기#17"):

        return raw

    if compact.startswith("NC보링기#3(") and "A동" not in compact:

        return "포인트 #3"

    if compact.startswith("NC보링기#26"):

        return "양면 #26"

    if compact.startswith("NC보링기#27"):

        return "양면 #27"

    for running_no in ["19", "20", "21", "22", "23", "24"]:

        if compact.startswith(f"NC보링기#{running_no}"):

            return f"런닝 #{running_no}"

    digits = "".join(ch for ch in raw if ch.isdigit())

    if digits and digits[0] in "123456" and "엣지" in raw:

        return f"엣지 #{digits[0]}"

    if "수직" in raw and digits:

        return f"수직 #{digits[0]}"

    if "포인트" in raw and digits:

        return f"포인트 #{digits[0]}"

    if "양면" in raw and digits:

        return f"양면 #{digits[0:2] if digits.startswith('2') and len(digits) > 1 else digits[0]}"

    if "런닝" in raw and digits:

        return f"런닝 #{digits}"

    if "NC보링기" in compact and digits:

        machine_no = digits

        if compact.startswith("NC보링기수직"):

            return f"수직 #{machine_no[0]}"

        if machine_no == "3":

            return "포인트 #3"

        if machine_no in {"26", "27"}:

            return f"양면 #{machine_no}"

        if machine_no in {"19", "20", "21", "22", "23", "24"}:

            return f"런닝 #{machine_no}"

    return raw





def machine_matches_target(machine: str, target_machine: str) -> bool:

    if target_machine == "auto":

        return True

    if target_machine in MACHINE_GROUPS:

        return machine in MACHINE_GROUPS[target_machine]

    return machine == target_machine





def infer_line_from_machine(machine: str) -> str:

    normalized = normalize_machine_name(machine)

    if normalized.startswith("엣지"):

        return "엣지"

    if normalized.startswith("런닝"):

        return "런닝"

    if normalized.startswith("양면"):

        return "양면"

    if normalized.startswith("포인트"):

        return "포인트"

    if normalized.startswith("수직"):

        return "수직"

    return ""





def is_edge_machine(machine: str) -> bool:

    return infer_line_from_machine(machine) == "?ｌ?"





def is_boring_machine(machine: str) -> bool:

    line_name = infer_line_from_machine(machine)

    return bool(line_name) and line_name != "?ｌ?"





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

    return ["전체", *unique_dates]





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

    return ["전체", *years], months_by_year, days_by_year_month





def apply_date_dropdown_filter(df: pd.DataFrame, column: str, prefix: str, container) -> pd.DataFrame:

    parsed_dates = [parsed for parsed in (parse_date_only(value) for value in df[column].tolist()) if parsed is not None]

    if not parsed_dates:

        container.date_input("날짜", value=None, key=f"{prefix}_date_filter", format="YYYY-MM-DD")

        return df



    selected_date = container.date_input(

        "날짜",

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

        machine = str(row_dict.get("설비", "")).strip()

        blade_name = str(row_dict.get("날물명", "")).strip()

        if blade_name:

            blade_parts = [part.strip() for part in blade_name.split(",") if part.strip()]

            if blade_parts:

                for blade in blade_parts:

                    copied = dict(row_dict)

                    copied["날물명"] = blade

                    expanded_rows.append(copied)

            else:

                expanded_rows.append(row_dict)

            continue

        blade_list = get_history_blade_list(machine)

        if blade_list:

            for blade in blade_list:

                copied = dict(row_dict)

                copied["날물명"] = blade

                expanded_rows.append(copied)

        else:

            expanded_rows.append(row_dict)

    return pd.DataFrame(expanded_rows)





def remove_redundant_boring_summary_rows(history_df: pd.DataFrame) -> pd.DataFrame:

    if history_df.empty:

        return history_df



    normalized_df = history_df.copy()

    machine_col = next((col for col in ["설비", "?ㅻ퉬"] if col in normalized_df.columns), None)

    blade_col = next((col for col in ["날물명", "?좊Ъ紐?"] if col in normalized_df.columns), None)

    time_col = next((col for col in ["반영시각", "諛섏쁺?쒓컖"] if col in normalized_df.columns), None)

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

    for column in ["반영 사용량(m)", "반영 사용량(회)"]:

        if column in normalized_df.columns:

            normalized_df[column] = pd.to_numeric(normalized_df[column], errors="coerce").fillna(0)



    group_columns = [column for column in ["반영시각", "설비", "날물명"] if column in normalized_df.columns]

    if not group_columns:

        return normalized_df



    aggregation_map: dict[str, Any] = {}

    if "반영 사용량(m)" in normalized_df.columns:

        aggregation_map["반영 사용량(m)"] = "sum"

    if "반영 사용량(회)" in normalized_df.columns:

        aggregation_map["반영 사용량(회)"] = "sum"

    if "데이터 기준일자" in normalized_df.columns:

        aggregation_map["데이터 기준일자"] = lambda values: min(

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

        raise ValueError("Teams Webhook URL이 설정되지 않았습니다.")

    assignee = str(row.get("assignee", row.get("담당자", ""))).strip()

    facts = [

        {"title": "설비", "value": row["machine"]},

        {"title": "날물", "value": row["bladeName"]},

        {"title": "교체 시점 사용량", "value": format_cycle_value(row, parse_numeric_value(row.get("usage", 0)))},

        {"title": "조치", "value": "교체완료"},

        {"title": "처리일", "value": date.today().isoformat()},

    ]

    if assignee:

        facts.append({"title": "담당자", "value": assignee})

    note = str(row.get("note", row.get("비고", ""))).strip()

    if note:

        facts.append({"title": "비고", "value": note})



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

                        {"type": "TextBlock", "size": "Large", "weight": "Bolder", "text": "날물 교체 완료"},

                        {"type": "TextBlock", "wrap": True, "text": f"{row['line']} / {row['machine']} / {row['spindle']}"},

                        {

                            "type": "FactSet",

                            "facts": facts,

                        },

                    ],

                },

            }

        ],

    }



    response = requests.post(webhook_url, json=payload, timeout=30)

    if not response.ok:

        raise RuntimeError(f"Teams 알림 실패: HTTP {response.status_code}")





def send_teams_replace_alert(row: dict[str, Any]) -> None:

    webhook_url = st.session_state.teams_webhook_url.strip()

    if not webhook_url:

        raise ValueError("Teams Webhook URL이 설정되지 않았습니다.")

    assignee = str(row.get("assignee", row.get("담당자", ""))).strip()

    facts = [

        {"title": "설비", "value": row["machine"]},

        {"title": "날물", "value": row["displayBladeName"]},

        {"title": "사용률", "value": f"{round(row['rate'] * 100)}%"},

        {"title": "잔여사용량", "value": row["displayRemaining"]},

        {"title": "예측교체", "value": row["predictedDate"]},

    ]

    if assignee:

        facts.append({"title": "담당자", "value": assignee})



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

                        {"type": "TextBlock", "size": "Large", "weight": "Bolder", "text": "날물 교체 알림"},

                        {"type": "TextBlock", "wrap": True, "text": f"{row['line']} / {row['machine']} / {row['spindle']}"},

                        {

                            "type": "FactSet",

                            "facts": facts,

                        },

                    ],

                },

            }

        ],

    }



    response = requests.post(webhook_url, json=payload, timeout=30)

    if not response.ok:

        raise RuntimeError(f"Teams 알림 실패: HTTP {response.status_code}")





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

    active_alert_keys = {
        f"{str(row.get('machine', '')).strip()}|{get_display_blade_name(row)}"
        for row in enriched
        if row.get("status") == "replace"
    }

    next_history = {key: signature for key, signature in alert_history.items() if key in active_alert_keys}

    latest_message = ""

    for row in enriched:

        machine = str(row.get("machine", "")).strip()

        if not machine or row.get("status") != "replace":

            continue

        alert_key = f"{machine}|{get_display_blade_name(row)}"

        if next_history.get(alert_key) == "sent":

            continue

        try:

            send_teams_replace_alert(row)

            next_history[alert_key] = "sent"

            latest_message = f"{machine} · {get_display_blade_name(row)} 날물 교체 알림을 전송했습니다."

        except Exception as exc:

            latest_message = f"{machine} · {get_display_blade_name(row)} 날물 교체 알림 전송 실패: {exc}"

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

            next_standard = EDGE_FIXED_STANDARDS.get(item["machine"], item["standard"]) if item["line"] == "엣지" else item["standard"]

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

        "5": "Φ5(관통) 날물",

        "8": "Φ8(관통) 날물",

        "12": "Φ12(관통) 날물",

        "15": "Φ15 날물",

        "20": "Φ20 날물",

        "35": "Φ35 날물",

    }

    return blade_map.get(blade_code, str(value or "").strip())





def get_boring_standard(machine: Any, blade_name: Any) -> int:

    normalized_machine = str(machine or "").strip()

    normalized_blade = normalize_boring_blade_name(blade_name)

    if normalized_blade == "Φ5(관통) 날물":

        if normalized_machine.startswith("런닝"):

            return 100000

        if normalized_machine.startswith(("양면", "수직")):

            return 50000

        if normalized_machine.startswith("포인트"):

            return 75000

    if normalized_blade == "Φ20 날물" and normalized_machine.startswith("런닝"):

        return 30000

    return 10000


def build_boring_history_entries_from_dataframe(df: pd.DataFrame, sync_time: str) -> list[dict[str, Any]]:

    df.columns = [str(col).replace("\ufeff", "").strip() for col in df.columns]

    machine_col = next((c for c in ["설비명", "설비", "설비명▼"] if c in df.columns), None)

    date_col = next((c for c in ["생산일", "작업일", "date"] if c in df.columns), None)

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

                    "반영시각": sync_time,

                    "대상": "보링 전체",

                    "설비": machine,

                    "날물명": blade_name,

                    "반영 사용량(m)": "",

                    "반영 사용량(회)": round(float(payload["usage_count"]), 3),

                    "시작일": payload["start_date"] or machine_start_dates.get(machine, ""),

                }

            )

    return entries





def load_latest_boring_snapshot_entries() -> list[dict[str, Any]]:

    latest_info = load_latest_upload_info()

    if str(latest_info.get("dataset_type", "")).strip() != "보링":

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

        return f"보링_grd_List_{digits[:14]}"

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

            str(entry.get("대상", "")).strip() == "보링 전체"

            or is_boring_machine(str(entry.get("설비", "")).strip())

        )

    ]

    boring_sync_times = sorted(

        {

            str(entry.get("반영시각", "")).strip()

            for entry in normalized_history

            if (str(entry.get("대상", "")).strip() == "보링 전체" or is_boring_machine(str(entry.get("설비", "")).strip()))

            and str(entry.get("반영시각", "")).strip()

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

                        (str(entry.get("대상", "")).strip() == "보링 전체" or is_boring_machine(str(entry.get("설비", "")).strip()))

                        and str(entry.get("반영시각", "")).strip() == sync_time

                    )

                ]

            )

    return merge_sheet_sync_history(preserved_history, rebuilt_entries)





def overlay_latest_boring_snapshot_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:

    latest_boring_entries = load_latest_boring_snapshot_entries()

    if not latest_boring_entries:

        return history



    sync_time = str(latest_boring_entries[0].get("반영시각", "")).strip()

    preserved_history = [

        entry

        for entry in normalize_sheet_sync_history(history)

        if not (

            str(entry.get("대상", "")).strip() == "보링 전체"

            and str(entry.get("반영시각", "")).strip() == sync_time

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

        machine = normalize_machine_name(str(entry.get("설비", entry.get("?ㅻ퉬", ""))).strip())

        if is_boring_machine(machine):

            return True

    return False





def handle_excel_upload(uploaded_file, target_machine: str) -> None:

    if uploaded_file is None:

        return

    df = pd.read_excel(uploaded_file)

    usage_col = "엣지사용량(m)" if "엣지사용량(m)" in df.columns else "총엣지사용량(m)"

    if usage_col not in df.columns:

        st.session_state.send_result = "엑셀에 엣지사용량(m) 또는 총엣지사용량(m) 열이 없습니다."

        return



    valid_rows = df[df[usage_col].notna()].copy()

    total_usage_m = float(valid_rows[usage_col].apply(parse_numeric_value).sum())

    date_candidates = pd.to_datetime(valid_rows["생산일"], errors="coerce") if "생산일" in valid_rows.columns else pd.Series(dtype="datetime64[ns]")

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

    st.session_state.send_result = f"엑셀 업로드 완료: {uploaded_file.name} / {target_machine} / {total_usage_m:.3f} m 반영"





def sync_from_google_sheet(

    sheet_url: str,

    target_machine: str,

    worksheet_name: str | None = None,

    worksheet_gid: str | None = None,

    silent: bool = False,

) -> None:

    if not sheet_url.strip():

        if not silent:

            st.session_state.send_result = "구글 스프레드시트 링크를 입력해 주세요."

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

        (candidate for candidate in ["엣지사용량(m)", "총엣지사용량(m)", "usage_m", "엣지사용량", "총엣지사용량"] if candidate in df.columns),

        None,

    )

    quantity_col = next(

        (candidate for candidate in ["생산량", "qty", "quantity"] if candidate in df.columns),

        None,

    )

    if usage_col is None and quantity_col is None:

        raise ValueError("시트에 엣지사용량(m) 또는 생산량 열이 없습니다.")



    machine_col = next(

        (c for c in ["설비", "설비명", "설비명▼", "호기", "machine", "machine_name"] if c in df.columns),

        None,

    )

    date_col = next((c for c in ["생산일", "date", "작업일"] if c in df.columns), None)

    material_col = next((c for c in ["재질", "재질▲", "material"] if c in df.columns), None)



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

        if machine.startswith(("수직", "포인트", "런닝", "양면")):

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

        if machine == "엣지 #6":

            front_count, back_count = parse_edge_material_counts(row[material_col]) if material_col else (0, 0)

            total_count = front_count + back_count

            if total_count <= 0:

                records.append(

                    {

                        "machine": machine,

                        "blade_name": "AT 날물(후면)",

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

                        "blade_name": "AT 날물(전면)",

                        "usageM": front_usage,

                        "usageCount": 0.0,

                        "prodDate": prod_date,

                    }

                )

            if back_usage > 0:

                records.append(

                    {

                        "machine": machine,

                        "blade_name": "AT 날물(후면)",

                        "usageM": back_usage,

                        "usageCount": 0.0,

                        "prodDate": prod_date,

                    }

                )

            continue



        blade_name = "AT 날물(후면)" if machine.startswith("엣지") else ""

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

    edge_count = sum(1 for machine in grouped_machines if machine.startswith("엣지"))

    boring_count = sum(1 for machine in grouped_machines if machine.startswith(("수직", "포인트", "런닝", "양면")))

    if target_machine == "auto":

        if edge_count and not boring_count:

            effective_target_label = "엣지 전체"

        elif boring_count and not edge_count:

            effective_target_label = "보링 전체"

        elif edge_count and boring_count:

            effective_target_label = "전체"

        else:

            effective_target_label = "자동"

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

                "usage_m": round(payload["total"], 3) if machine.startswith("엣지") else "",

                "usage_count": round(payload["usage_count"], 3) if machine.startswith(("수직", "포인트", "런닝", "양면")) else "",

                "start_date": start_date or "-",

            }

        )



    sync_time = extract_sync_time_from_text(worksheet_name or st.session_state.get("auto_sheet_name", "") or sheet_url)

    if grouped:

        synced_label = ", ".join(f"{machine}/{blade}" if blade else machine for machine, blade in grouped.keys())

    else:

        detected = sorted({record["machine"] for record in records if record["machine"]})

        detected_label = ", ".join(detected[:8]) if detected else "읽은 설비 없음"

        synced_label = f"반영 설비 없음 ({detected_label})"

    history_entries = [

        {

            "반영시각": sync_time,

            "대상": effective_target_label,

            "설비": detail["machine"],

            "날물명": detail["blade_name"],

            "반영 사용량(m)": detail["usage_m"],

            "반영 사용량(회)": detail["usage_count"],

            "시작일": detail["start_date"],

        }

        for detail in sync_details

    ]

    should_replace_boring_history = False

    if history_entries and (not is_duplicate_sync or should_replace_boring_history):

        previous_history = st.session_state.get("sheet_sync_history", [])

        if should_replace_boring_history:

            replacement_keys = {

                (

                    str(entry.get("반영시각", "")).strip(),

                    str(entry.get("대상", "")).strip(),

                    str(entry.get("설비", "")).strip(),

                    str(entry.get("날물명", "")).strip(),

                    str(entry.get("시작일", "")).strip(),

                )

                for entry in history_entries

            }

            previous_history = [

                entry

                for entry in previous_history

                if (

                    str(entry.get("반영시각", "")).strip(),

                    str(entry.get("대상", "")).strip(),

                    str(entry.get("설비", "")).strip(),

                    str(entry.get("날물명", "")).strip(),

                    str(entry.get("시작일", "")).strip(),

                )

                not in replacement_keys

            ]

        merged_history = merge_sheet_sync_history(previous_history, history_entries)

        st.session_state.sheet_sync_history = merged_history

        save_sheet_sync_history(merged_history)

        new_keys = {

            (

                str(entry.get("대상", "")).strip(),

                str(entry.get("설비", "")).strip(),

                str(entry.get("날물명", "")).strip(),

                str(entry.get("반영 사용량(m)", "")).strip(),

                str(entry.get("반영 사용량(회)", "")).strip(),

                str(entry.get("시작일", "")).strip(),

            )

            for entry in history_entries

        }

        matching_times = [

            str(entry.get("반영시각", "")).strip()

            for entry in merged_history

            if (

                str(entry.get("대상", "")).strip(),

                str(entry.get("설비", "")).strip(),

                str(entry.get("날물명", "")).strip(),

                str(entry.get("반영 사용량(m)", "")).strip(),

                str(entry.get("반영 사용량(회)", "")).strip(),

                str(entry.get("시작일", "")).strip(),

            )

            in new_keys

        ]

        st.session_state.last_sheet_sync_at = min(matching_times) if matching_times else sync_time

    elif is_duplicate_sync:

        existing_history = st.session_state.get("sheet_sync_history", [])

        if existing_history:

            st.session_state.last_sheet_sync_at = str(existing_history[0].get("반영시각", st.session_state.get("last_sheet_sync_at", ""))).strip()

    st.session_state.last_sheet_sync_details = normalize_last_sheet_sync_details(sync_details)

    if not is_duplicate_sync:

        updated_hashes = [*existing_hashes, sync_hash]

        st.session_state.sheet_sync_hashes[hash_bucket_key] = updated_hashes[-50:]

    if not silent:

        if is_duplicate_sync:

            st.session_state.send_result = f"같은 데이터라 사용량은 유지하고 반영 결과만 갱신했습니다: {synced_label}"

        else:

            st.session_state.send_result = f"구글 스프레드시트 자동 반영 완료: {synced_label}"

    save_dashboard_state()





def handle_action(row_id: int, assignee: str = "", note: str = "") -> None:

    selected_item = next((item for item in st.session_state.equipment_data if item["id"] == row_id), None)

    if selected_item is None:

        return

    selected_machine = str(selected_item.get("machine", "")).strip()

    selected_blade = get_display_blade_name(selected_item)

    completed_usage = parse_numeric_value(selected_item.get("usage", 0))

    completed_usage_label = format_cycle_value(selected_item, completed_usage)

    completed_at = now_kst().strftime("%Y-%m-%d %H:%M:%S")

    today = date.today().isoformat()

    selected_standard = parse_numeric_value(selected_item.get("standard", 0))

    selected_rate = completed_usage / selected_standard if selected_standard else 0

    was_replace = selected_rate >= 1

    completion_usage_label = completed_usage_label

    assignee = str(assignee or "").strip()

    note = str(note or "").strip()

    if was_replace and not assignee:

        st.session_state.send_result = f"{selected_machine} · {selected_blade} 담당자를 입력한 뒤 교체필요를 눌러주세요."

        return

    if not was_replace and not assignee:

        st.session_state.send_result = f"{selected_machine} · {selected_blade} 조기 교체 담당자를 입력해 주세요."

        return

    if not was_replace and not note:

        st.session_state.send_result = f"{selected_machine} · {selected_blade} 교체 이유를 입력해 주세요."

        return



    def is_selected_row(item: dict[str, Any]) -> bool:

        return str(item.get("machine", "")).strip() == selected_machine and get_display_blade_name(item) == selected_blade



    st.session_state.equipment_data = [

        {

            **item,

            "usage": 0 if is_selected_row(item) else item.get("usage", 0),

            "quality": 0 if is_selected_row(item) else item.get("quality", 0),

            "installDate": today if is_selected_row(item) else item.get("installDate", ""),

            "actionStep": "completed" if is_selected_row(item) and was_replace else ("" if is_selected_row(item) else item.get("actionStep", "")),

        }

        for item in st.session_state.equipment_data

    ]



    st.session_state.replace_alert_history.pop(selected_machine, None)

    st.session_state.replace_alert_history.pop(f"{selected_machine}|{selected_blade}", None)

    blade_reset_at = st.session_state.get("blade_reset_at", {})

    blade_reset_at[f"{selected_machine}|{selected_blade}"] = completed_at

    st.session_state.blade_reset_at = blade_reset_at



    completion_entry = {

        "교체완료시각": completed_at,

        "설비": selected_machine,

        "날물명": selected_blade,

        "교체 시점 사용량": completion_usage_label,

        "담당자": assignee,

        "비고": note,

    }

    st.session_state.completion_history = normalize_completion_history([completion_entry, *st.session_state.get("completion_history", [])])



    if was_replace:

        message = f"{selected_machine} · {selected_blade} 교체 완료 처리되었습니다."
    else:

        message = f"{selected_machine} · {selected_blade} 조기 교체 처리되었습니다."

    try:

        remaining = max(0, selected_standard - completed_usage)

        remain_days = days_left(remaining, parse_numeric_value(selected_item.get("avg7d", 0)))

        alert_row = {

            **selected_item,

            "rate": selected_rate,

            "bladeName": selected_blade,

            "displayBladeName": selected_blade,

            "displayRemaining": format_cycle_value(selected_item, remaining),

            "predictedDate": "-" if remain_days == 999 else f"{remain_days}일 후",

            "assignee": assignee,

            "담당자": assignee,

            "note": note,

            "비고": note,

        }

        send_teams_complete_alert(alert_row)

        message += " Teams 알림도 전송했습니다."

    except Exception as exc:

        message += f" Teams 알림 전송 실패: {exc}"



    st.session_state.send_result = message

    save_completion_history(st.session_state.get("completion_history", []))

    save_dashboard_state()



def get_action_label(row: dict[str, Any]) -> str:

    if row.get("rate", 0) >= 1:

        return "교체필요"

    if row.get("actionStep") == "completed" and parse_numeric_value(row.get("usage", 0)) <= 0:

        return "교체완료"

    return "정상"





def render_kpis(enriched: list[dict[str, Any]]) -> None:

    replace_now = len([d for d in enriched if d.get("rate", 0) >= 1])

    due_soon = len([d for d in enriched if 0.7 <= d.get("rate", 0) < 1])

    avg_rate = round(sum(d["rate"] for d in enriched) / len(enriched) * 100) if enriched else 0

    cards = [

        ("\uad00\ub9ac \ub0a0\ubb3c", f"{len(enriched)} EA", "\uc2e4\uc2dc\uac04 \uad00\ub9ac \ub300\uc0c1"),

        ("\uc989\uc2dc \uad50\uccb4", f"{replace_now} \uac74", "\uc0ac\uc6a9\ub960 \uae30\uc900"),

        ("\uc0ac\uc6a9\ub960 70% \uc774\uc0c1", f"{due_soon} \uac74", "70~99% \uad6c\uac04"),

        ("\ud3c9\uade0 \uc0ac\uc6a9\ub960", f"{avg_rate}%", "\ub77c\uc778 \ud3c9\uade0"),

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

        label = "\uad50\uccb4\ud544\uc694"

    elif status == "caution":

        label = "\uc8fc\uc758"

    else:

        label = "\ubd88\ud544\uc694"

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

    elif rate >= 0.7:

        styles = STATUS_STYLES["caution"]

    else:

        styles = STATUS_STYLES["normal"]

    width = 100 if rate >= 1 else max(0, min(99, int(rate * 100)))

    return (

        "<div style='display:flex;align-items:center;gap:12px;'>"

        "<div style='width:98px;height:12px;background:#e2e8f0;border-radius:999px;overflow:hidden;'>"

        f"<div style='width:{width}%;height:100%;background:{styles['bar']};border-radius:999px;'></div>"

        "</div>"

        f"<span style='font-weight:700;color:#0f172a;'>{width}%</span>"

        "</div>"

    )





def render_normal_replacement_prompt() -> None:

    prompt = st.session_state.get("normal_replacement_prompt")

    if not isinstance(prompt, dict):

        return

    row_id = prompt.get("row_id")

    assignee = str(prompt.get("assignee", "")).strip()

    selected_item = next((item for item in st.session_state.equipment_data if item.get("id") == row_id), None)

    if selected_item is None:

        st.session_state.pop("normal_replacement_prompt", None)

        return

    title = "교체 이유 입력"

    description = f"{selected_item.get('machine', '')} · {get_display_blade_name(selected_item)}"

    def render_form() -> None:

        st.write(description)

        with st.form(f"normal_replacement_reason_form_{row_id}"):

            form_assignee = st.text_input("담당자", value=assignee, key=f"normal_replacement_assignee_{row_id}", placeholder="담당자 이름")

            reason = st.text_area("교체 이유", key=f"normal_replacement_reason_{row_id}", placeholder="예: 깨짐, 불량 발생, 날물 이상 등")

            form_cols = st.columns(2)

            submitted = form_cols[0].form_submit_button("교체 처리", use_container_width=True)

            cancelled = form_cols[1].form_submit_button("취소", use_container_width=True)

            if submitted:

                if not form_assignee.strip():

                    st.warning("담당자를 입력해 주세요.")

                    return

                if not reason.strip():

                    st.warning("교체 이유를 입력해 주세요.")

                    return

                handle_action(int(row_id), form_assignee.strip(), reason.strip())

                st.session_state.pop("normal_replacement_prompt", None)

                st.rerun()

            if cancelled:

                st.session_state.pop("normal_replacement_prompt", None)

                st.rerun()

    if hasattr(st, "dialog"):

        @st.dialog(title)
        def reason_dialog() -> None:

            render_form()

        reason_dialog()

    else:

        with st.container(border=True):

            st.subheader(title)

            render_form()


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

        [class*="st-key-table_action_completed_"] div[data-testid="stButton"] > button[kind="secondary"] {

            background: #fff7ed !important;

            color: #c2410c !important;

            border: 1px solid #fdba74 !important;

            border-radius: 999px !important;

            font-weight: 700 !important;

        }

        [class*="st-key-table_action_completed_"] div[data-testid="stButton"] > button[kind="secondary"]:hover {

            background: #ffedd5 !important;

            color: #9a3412 !important;

            border: 1px solid #fb923c !important;

        }

        </style>

        """,

        unsafe_allow_html=True,

    )

    with st.container(border=True):

        header_cols = st.columns([0.7, 1.1, 1.35, 0.9, 1.1, 1.0, 0.9, 1.0, 1.0])
        header_labels = ["\ub77c\uc778", "\uc124\ube44", "\ub0a0\ubb3c\uba85", "\uae30\uc900\uac12", "\uc0ac\uc6a9\ub960", "\uc794\uc5ec\uc0ac\uc6a9\ub7c9", "\uc608\uce21\uad50\uccb4", "\ub2f4\ub2f9\uc790", "\uad50\uccb4\uc0c1\ud0dc"]
        for col, label in zip(header_cols, header_labels):
            col.caption(label)
        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)




        for row in rows:

            line_label = "\uc5e3\uc9c0" if row["line"] == "\uc5e3\uc9c0" else "\ubcf4\ub9c1"

            with st.container(border=True):

                row_cols = st.columns([0.7, 1.1, 1.35, 0.9, 1.1, 1.0, 0.9, 1.0, 1.0])

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

                action_label = get_action_label(row)

                assignee_record_key = f"{row['machine']}|{row['displayBladeName']}"

                assignee_reset_version = st.session_state.get("assignee_widget_reset_versions", {}).get(assignee_record_key, 0)

                assignee_widget_key = f"replacement_assignee_{row['id']}_{assignee_reset_version}"

                assignee_defaults = st.session_state.get("replacement_assignees", {})

                pending_clear_assignees = set(st.session_state.get("clear_assignee_record_keys", []))

                if action_label != "교체필요" or assignee_record_key in pending_clear_assignees:

                    assignee_defaults.pop(assignee_record_key, None)

                    st.session_state.replacement_assignees = assignee_defaults

                    if assignee_widget_key not in st.session_state or st.session_state.get(assignee_widget_key):

                        st.session_state[assignee_widget_key] = ""

                    if assignee_record_key in pending_clear_assignees:

                        pending_clear_assignees.remove(assignee_record_key)

                        st.session_state.clear_assignee_record_keys = list(pending_clear_assignees)

                if assignee_widget_key not in st.session_state:

                    st.session_state[assignee_widget_key] = assignee_defaults.get(assignee_record_key, "")

                row_cols[7].text_input(

                    "담당자",

                    key=assignee_widget_key,

                    label_visibility="collapsed",

                    placeholder="담당자",

                )

                if action_label == "교체필요":

                    button_type = "primary"

                else:

                    button_type = "secondary"

                action_key_prefix = "completed" if action_label == "교체완료" else ("replace" if action_label == "교체필요" else "normal")

                if row_cols[8].button(action_label, key=f"table_action_{action_key_prefix}_{row['id']}", use_container_width=True, type=button_type):

                    assignee = str(st.session_state.get(assignee_widget_key, "")).strip()

                    replacement_assignees = st.session_state.get("replacement_assignees", {})

                    replacement_assignees[assignee_record_key] = assignee

                    st.session_state.replacement_assignees = replacement_assignees

                    if action_label == "정상":

                        st.session_state.normal_replacement_prompt = {"row_id": row["id"], "assignee": ""}

                    else:

                        st.session_state.pop("normal_replacement_prompt", None)

                        handle_action(row["id"], assignee)

                        if action_label == "교체필요" and st.session_state.send_result and "담당자를 입력" not in st.session_state.send_result:

                            replacement_assignees.pop(assignee_record_key, None)

                            st.session_state.replacement_assignees = replacement_assignees

                            pending_clear_assignees = set(st.session_state.get("clear_assignee_record_keys", []))

                            pending_clear_assignees.add(assignee_record_key)

                            st.session_state.clear_assignee_record_keys = list(pending_clear_assignees)

                            reset_versions = st.session_state.get("assignee_widget_reset_versions", {})

                            reset_versions[assignee_record_key] = reset_versions.get(assignee_record_key, 0) + 1

                            st.session_state.assignee_widget_reset_versions = reset_versions

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

    if dataset_type == "보링" and auto_sheet_updated_at and (

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

        or (dataset_type == "보링" and st.session_state.get("last_snapshot_sync_key", "") != current_snapshot_key)

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

            if dataset_type == "보링":

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



    st.title("\ub0a0\ubb3c \uad50\uccb4\uad00\ub9ac \ub300\uc2dc\ubcf4\ub4dc")

    st.caption("FURSYS · \ucda9\uc8fc \uacf5\uc7a5 · \ud488\uc9c8\ubcf4\uc99d\ud300")



    render_kpis(enriched)



    with st.sidebar:

        st.subheader("필터")

        status_filter = st.selectbox("상태", ["all", "normal", "caution", "replace"], format_func=lambda x: {"all": "전체 상태", "normal": "정상", "caution": "주의", "replace": "교체"}[x])

        search = st.text_input("설비명 검색")



        st.divider()

        st.subheader("구글 스프레드시트 반영")

        sheet_url = st.text_input("구글 시트 링크", value=auto_sheet_url or DEFAULT_GOOGLE_SHEET_URL)

        all_machines = sorted({item["machine"] for item in st.session_state.equipment_data})

        target_options = ["엣지 전체", "보링 전체", *all_machines]

        target_machine = st.selectbox("기본 대상 설비", target_options, key="sheet_target_machine")

        if st.button("지금 반영", use_container_width=True):

            try:

                sync_from_google_sheet(

                    sheet_url,

                    target_machine,

                    worksheet_name=auto_sheet_name or None,

                    worksheet_gid=st.session_state.get("auto_sheet_gid") or None,

                )

                st.rerun()

            except Exception as exc:

                st.session_state.send_result = f"구글 스프레드시트 동기화 실패: {exc}"

        st.caption(f"최근 동기화: {st.session_state.last_sheet_sync_at or '아직 없음'}")

        if auto_sheet_name:

            st.caption(f"자동 연결 시트: {auto_sheet_name}")

        if auto_sheet_updated_at:

            st.caption(f"자동 연결 갱신: {auto_sheet_updated_at}")

        st.text_input("Teams Webhook URL", key="teams_webhook_url")

        if st.button("사용률 리셋", use_container_width=True):

            reset_all_usage_data()

            st.rerun()

        if st.button("데이터 반영 이력 리셋", use_container_width=True):

            reset_sheet_sync_history_data()

            st.rerun()

        if st.button("교체완료 시점 리셋", use_container_width=True):

            reset_completion_history_data()

            st.rerun()



    if st.session_state.send_result:

        st.info(st.session_state.send_result)



    if st.session_state.upload_summary:

        summary = st.session_state.upload_summary

        st.caption(

            f"최근 반영: {summary['fileName']} / {summary['targetMachine']} / {summary['startDate']} ~ {summary['endDate']} / "

            f"{summary['periodDays']}일 기준 / {summary['totalUsageM']:.3f} m"

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

        st.subheader("설비별 교체 현황")

        available_lines = [line for line in LINE_FILTER_ORDER if any(row["line"] == line for row in enriched)]

        line_button_cols = st.columns(len(available_lines) + 1)

        if line_button_cols[0].button("전체", key="line_toggle_all", use_container_width=True, type="primary" if st.session_state.get("line_filter_toggle", "all") == "all" else "secondary"):

            st.session_state.pop("normal_replacement_prompt", None)

            st.session_state.line_filter_toggle = "all"

            st.session_state.line_machine_filter = "전체"

            st.rerun()

        for idx, line_name in enumerate(available_lines, start=1):

            active = st.session_state.get("line_filter_toggle", "all") == line_name

            if line_button_cols[idx].button(line_name, key=f"line_toggle_{line_name}", use_container_width=True, type="primary" if active else "secondary"):

                st.session_state.pop("normal_replacement_prompt", None)

                st.session_state.line_filter_toggle = line_name

                st.session_state.line_machine_filter = "전체"

                st.rerun()

        active_line_filter = st.session_state.get("line_filter_toggle", "all")

        if active_line_filter != "all":

            machine_options = ["전체", *[machine for machine in LINE_MACHINE_OPTIONS.get(active_line_filter, []) if any(row["machine"] == machine for row in enriched)]]

            previous_machine_filter = st.session_state.get("line_machine_filter", "전체")

            st.selectbox(

                f"{active_line_filter} 세부 선택",

                machine_options,

                key="line_machine_filter",

            )

            if st.session_state.get("line_machine_filter", "전체") != previous_machine_filter:

                st.session_state.pop("normal_replacement_prompt", None)

                st.rerun()

        filtered = [

            row

            for row in filtered

            if active_line_filter == "all" or row["line"] == active_line_filter

        ]

        active_machine_filter = st.session_state.get("line_machine_filter", "전체")

        if active_line_filter != "all" and active_machine_filter != "전체":

            filtered = [row for row in filtered if row["machine"] == active_machine_filter]

        st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

        render_equipment_table(filtered)

        render_normal_replacement_prompt()



        st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)

        st.caption("데이터 반영 이력")

        if st.session_state.sheet_sync_history:

            history_df = pd.DataFrame(st.session_state.sheet_sync_history)

            ordered_columns = ["반영시각", "설비", "날물명", "반영 사용량(m)", "반영 사용량(회)", "데이터 기준일자"]

            history_df = history_df.rename(columns={"시작일": "데이터 기준일자"})

            history_df = history_df[[column for column in ordered_columns if column in history_df.columns]]

            history_df["설비"] = history_df["설비"].map(normalize_machine_name)

            history_df["_line"] = history_df["설비"].map(infer_line_from_machine)

            history_df = history_df[

                history_df["설비"].apply(lambda machine: (active_line_filter == "all" or infer_line_from_machine(machine) == active_line_filter))

            ]

            if active_line_filter != "all" and active_machine_filter != "전체":

                history_df = history_df[history_df["설비"] == active_machine_filter]

            history_filter_cols = st.columns(3)

            history_df = apply_date_dropdown_filter(history_df, "반영시각", "history", history_filter_cols[0])

            history_df = expand_history_rows_by_blade(history_df)

            machine_options = ["전체", *sorted([value for value in history_df["설비"].dropna().astype(str).unique() if value.strip()])]

            selected_history_machine = history_filter_cols[1].selectbox("설비", machine_options, key="history_machine_filter")

            if selected_history_machine != "전체":

                history_df = history_df[history_df["설비"] == selected_history_machine]

            blade_options = ["전체", *sorted([value for value in history_df["날물명"].dropna().astype(str).unique() if value.strip()])]

            selected_history_blade = history_filter_cols[2].selectbox("날물명", blade_options, key="history_blade_filter")

            if selected_history_blade != "전체":

                history_df = history_df[history_df["날물명"] == selected_history_blade]

            history_df = aggregate_history_rows(history_df)

            history_df["_sort_time"] = pd.to_datetime(history_df["반영시각"], errors="coerce")

            history_df["_machine_sort"] = history_df["설비"].apply(get_machine_sort_key)

            history_df["_blade_sort"] = history_df["날물명"].apply(get_blade_sort_key)

            history_df = (

                history_df

                .sort_values(

                    by=["_sort_time", "_machine_sort", "_blade_sort", "설비", "날물명"],

                    ascending=[False, True, True, True, True],

                    na_position="last",

                )

                .reset_index(drop=True)

            )

            for column in ["반영 사용량(m)", "반영 사용량(회)"]:

                if column in history_df.columns:

                    history_df[column] = history_df[column].where(history_df[column].notna(), "")

            history_df = history_df.drop(columns=["_line", "_sort_time", "_machine_sort", "_blade_sort"], errors="ignore")

            if not history_df.empty:

                st.dataframe(format_sync_display_dataframe(history_df), use_container_width=True, hide_index=True)

            else:

                st.info("조건에 맞는 반영 이력이 없습니다.")

        else:

            st.info("아직 반영 이력이 없습니다.")



        st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)

        st.caption("교체완료 시점")

        if st.session_state.get("completion_history"):

            completion_df = pd.DataFrame(normalize_completion_history(st.session_state.get("completion_history", [])))

            ordered_columns = ["교체완료시각", "설비", "날물명", "교체 시점 사용량", "담당자", "비고"]

            completion_df = completion_df[[column for column in ordered_columns if column in completion_df.columns]]

            if "교체 시점 사용량" in completion_df.columns:

                completion_df["교체 시점 사용량"] = completion_df["교체 시점 사용량"].where(completion_df["교체 시점 사용량"].notna(), "")

                completion_df["교체 시점 사용량"] = completion_df["교체 시점 사용량"].replace("None", "")

            completion_df["설비"] = completion_df["설비"].apply(normalize_machine_name)

            completion_df = completion_df[

                completion_df["설비"].apply(lambda machine: (active_line_filter == "all" or infer_line_from_machine(machine) == active_line_filter))

            ]

            if active_line_filter != "all" and active_machine_filter != "전체":

                completion_df = completion_df[completion_df["설비"] == active_machine_filter]

            completion_filter_cols = st.columns(3)

            completion_df = apply_date_dropdown_filter(completion_df, "교체완료시각", "completion", completion_filter_cols[0])

            completion_machine_options = ["전체", *sorted([value for value in completion_df["설비"].dropna().astype(str).unique() if value.strip()])]

            selected_completion_machine = completion_filter_cols[1].selectbox("설비", completion_machine_options, key="completion_machine_filter")

            if selected_completion_machine != "전체":

                completion_df = completion_df[completion_df["설비"] == selected_completion_machine]

            completion_blade_options = ["전체", *sorted([value for value in completion_df["날물명"].dropna().astype(str).unique() if value.strip()])]

            selected_completion_blade = completion_filter_cols[2].selectbox("날물명", completion_blade_options, key="completion_blade_filter")

            if selected_completion_blade != "전체":

                completion_df = completion_df[completion_df["날물명"] == selected_completion_blade]

            if not completion_df.empty:

                st.dataframe(format_sync_display_dataframe(completion_df), use_container_width=True, hide_index=True)

            else:

                st.info("조건에 맞는 교체완료 이력이 없습니다.")

        else:

            st.info("아직 교체완료 이력이 없습니다.")



    with right:

        st.subheader("교체 우선순위 TOP 5")

        for index, row in enumerate(top_priority, start=1):

            with st.container(border=True):

                st.caption(f"#{index} · {row['line']}")

                st.markdown(f"**{row['machine']}**")

                st.write(row["displayBladeName"])

                st.write(f"기준값 {row['displayStandard']}")

                st.write(f"사용률 {round(row['rate'] * 100)}% · {row['predictedDate']}")

                st.write(f"잔여 {row['displayRemaining']}")





if __name__ == "__main__":

    main()

