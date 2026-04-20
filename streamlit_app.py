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


st.set_page_config(page_title="날물 교체관리 대시보드", layout="wide")

TEAMS_DEFAULT_WEBHOOK = "https://defaulte2d70a05f3524e9d8c182194f1d9ef.31.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/98f10010be974d57a6f4065239b83ca4/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=tVbGSnsTMHbildXcbLsoBQj_WXrvSSOLnqktQNSDFBM"
DEFAULT_GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1KmsdyvfHJEOXnZvGtUl1TEhWz0JzJ3NWkg3Amb8l91U/edit"
WORK_DIR = Path(__file__).resolve().parent
LOG_DIR = WORK_DIR / "logs"
LATEST_UPLOAD_INFO_PATH = LOG_DIR / "latest_sheet_upload.json"
SHEET_SYNC_HISTORY_PATH = LOG_DIR / "sheet_sync_history.json"
DASHBOARD_STATE_PATH = LOG_DIR / "dashboard_state.json"
UPLOAD_INFO_WORKSHEET_NAME = "DASHBOARD_UPLOAD_INFO"
KST = ZoneInfo("Asia/Seoul")


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
    {"suffix": "020", "bladeName": "Φ20 날물", "standard": 9000, "avg7d": 320, "quality": 0, "spindle": "H2"},
    {"suffix": "012", "bladeName": "Φ12(관통) 날물", "standard": 10500, "avg7d": 410, "quality": 0, "spindle": "H3"},
    {"suffix": "008", "bladeName": "Φ8(관통) 날물", "standard": 9800, "avg7d": 355, "quality": 0, "spindle": "MAIN"},
    {"suffix": "015", "bladeName": "Φ15 날물", "standard": 9500, "avg7d": 300, "quality": 0, "spindle": "H4"},
    {"suffix": "005", "bladeName": "Φ5(관통) 날물", "standard": 9200, "avg7d": 280, "quality": 0, "spindle": "H5"},
]

EDGE_MACHINE_DEFAULTS = [
    {"line": "엣지", "machine": "엣지 #1", "spindle": "H1", "bladeCode": "AT-013-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-03", "usage": 0, "standard": 8000, "avg7d": 2000, "quality": 0},
    {"line": "엣지", "machine": "엣지 #2", "spindle": "H2", "bladeCode": "AT-014-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-05", "usage": 0, "standard": 8000, "avg7d": 2000, "quality": 0},
    {"line": "엣지", "machine": "엣지 #3,4", "spindle": "H1/H3", "bladeCode": "AT-015-016-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-06", "usage": 0, "standard": 60000, "avg7d": 15000, "quality": 0},
    {"line": "엣지", "machine": "엣지 #5", "spindle": "H2", "bladeCode": "AT-017-B", "bladeName": "AT 날물(후면)", "installDate": "2026-02-27", "usage": 0, "standard": 8500, "avg7d": 2125, "quality": 0},
    {"line": "엣지", "machine": "엣지 #6", "spindle": "MAIN-F", "bladeCode": "AT-018-F", "bladeName": "AT 날물(전면)", "installDate": "2026-03-26", "usage": 0, "standard": 40000, "avg7d": 10000, "quality": 0, "actionStep": ""},
    {"line": "엣지", "machine": "엣지 #6", "spindle": "MAIN-B", "bladeCode": "AT-018-B", "bladeName": "AT 날물(후면)", "installDate": "2026-03-26", "usage": 0, "standard": 40000, "avg7d": 10000, "quality": 0, "actionStep": ""},
]


def build_initial_raw_data() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    row_id = 1
    line_prefix = {"수직": "V", "포인트": "P", "양면": "D", "런닝": "R"}
    for machine_config in BORING_MACHINE_CONFIG:
        for blade_spec in BORING_BLADE_SPECS:
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
                "standard": blade_spec["standard"],
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


def normalize_display_timestamp(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
            if parsed.hour <= 3:
                parsed = parsed.replace(tzinfo=ZoneInfo("UTC")).astimezone(KST)
            else:
                parsed = parsed.replace(tzinfo=KST)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    try:
        parsed_iso = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed_iso.tzinfo is None:
            if parsed_iso.hour <= 3:
                parsed_iso = parsed_iso.replace(tzinfo=ZoneInfo("UTC"))
            else:
                parsed_iso = parsed_iso.replace(tzinfo=KST)
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
        merged_rows.append({**default_row, **existing_map.get(key, {})})

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
    st.session_state.upload_summary = None
    st.session_state.send_result = "설비 사용률을 모두 리셋했습니다."
    save_dashboard_state()


def reset_last_sheet_sync_result() -> None:
    st.session_state.last_sheet_sync_details = []
    st.session_state.last_sheet_sync_at = ""
    st.session_state.send_result = "최근 구글 스프레드시트 반영 결과를 리셋했습니다."
    save_dashboard_state()


def reset_sheet_sync_history_data() -> None:
    st.session_state.sheet_sync_history = []
    save_sheet_sync_history([])
    st.session_state.send_result = "구글 스프레드시트 반영 이력을 리셋했습니다."
    save_dashboard_state()


def reset_completion_history_data() -> None:
    st.session_state.completion_history = []
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
    "엣지 #1": 8000,
    "엣지 #2": 8000,
    "엣지 #3,4": 60000,
    "엣지 #5": 8500,
    "엣지 #6": 40000,
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
        raw_history = saved_state.get("sheet_sync_history", load_sheet_sync_history())
        st.session_state.sheet_sync_history = normalize_sheet_sync_history(raw_history if isinstance(raw_history, list) else [])
    if "completion_history" not in st.session_state:
        raw_completion = saved_state.get("completion_history", [])
        st.session_state.completion_history = raw_completion if isinstance(raw_completion, list) else []
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
    if not SHEET_SYNC_HISTORY_PATH.exists():
        return []
    try:
        data = json.loads(SHEET_SYNC_HISTORY_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return []
        normalized = normalize_sheet_sync_history(data)
        if normalized != data:
            save_sheet_sync_history(normalized)
        return normalized
    except Exception:
        return []


def save_sheet_sync_history(history: list[dict[str, Any]]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    normalized = normalize_sheet_sync_history(history)
    SHEET_SYNC_HISTORY_PATH.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_sheet_sync_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for entry in history:
        if not isinstance(entry, dict):
            continue
        machine = normalize_machine_name(str(entry.get("설비", entry.get("?ㅻ퉬", ""))).strip())
        target = str(entry.get("대상", entry.get("???", ""))).strip()
        usage_m_key = "반영 사용량(m)" if "반영 사용량(m)" in entry else "諛섏쁺 ?ъ슜??m)"
        usage_count_key = "반영 사용량(회)" if "반영 사용량(회)" in entry else "諛섏쁺 ?ъ슜????"
        sync_at = entry.get("반영시각", entry.get("諛섏쁺?쒓컖", ""))
        start_date = entry.get("시작일", entry.get("?쒖옉??", ""))
        blade_name = entry.get("날물명", entry.get("?좊Ъ紐?", ""))
        usage_m = entry.get(usage_m_key, "")
        usage_count = entry.get(usage_count_key, "")

        is_boring = machine.startswith(("수직", "포인트", "런닝", "양면", "?섏쭅", "?ъ씤??", "?곕떇", "?묐㈃"))
        is_edge = machine.startswith(("엣지", "?ｌ?"))

        if is_boring:
            target = "보링 전체"
            usage_m = ""
            blade_name = ""
        elif is_edge:
            target = "엣지 전체"
            usage_count = ""
            blade_name = normalize_edge_blade_name(machine, blade_name)
        if target == "보링 전체":
            blade_name = ""
        if is_edge and not str(blade_name).strip():
            blade_name = get_machine_blade_summary(machine, INITIAL_RAW_DATA)

        normalized.append(
            {
                "반영시각": normalize_display_timestamp(sync_at),
                "대상": target,
                "설비": machine,
                "날물명": blade_name,
                "반영 사용량(m)": usage_m,
                "반영 사용량(회)": usage_count,
                "시작일": start_date,
            }
        )
    return normalized


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
            blade_name = ""
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
        "usage_reset_at": st.session_state.get("usage_reset_at", ""),
        "line_filter_toggle": st.session_state.get("line_filter_toggle", "all"),
        "line_machine_filter": st.session_state.get("line_machine_filter", "전체"),
    }
    DASHBOARD_STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def reconcile_edge_usage_from_history(data: list[dict[str, Any]], history: list[dict[str, Any]], reset_at: str = "") -> list[dict[str, Any]]:
    aggregated: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in history:
        sync_at = str(entry.get("반영시각", "")).strip()
        if reset_at and sync_at and sync_at <= reset_at:
            continue
        machine = normalize_machine_name(str(entry.get("설비", "")).strip())
        blade_name = str(entry.get("날물명", "")).strip()
        usage_m = parse_numeric_value(entry.get("반영 사용량(m)", 0))
        start_date = str(entry.get("시작일", "")).strip()
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
        if item.get("line") != "엣지" or key not in aggregated:
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
        st.rerun()
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


def expand_history_rows_by_blade(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return history_df

    expanded_rows: list[dict[str, Any]] = []
    for _, history_row in history_df.iterrows():
        row_dict = history_row.to_dict()
        machine = str(row_dict.get("설비", "")).strip()
        blade_name = str(row_dict.get("날물명", "")).strip()
        if blade_name:
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


def send_teams_complete_alert(row: dict[str, Any]) -> None:
    webhook_url = st.session_state.teams_webhook_url.strip()
    if not webhook_url:
        raise ValueError("Teams Webhook URL이 설정되지 않았습니다.")

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
                            "facts": [
                                {"title": "설비", "value": row["machine"]},
                                {"title": "날물", "value": row["bladeName"]},
                                {"title": "교체 시점 사용량", "value": format_cycle_value(row, parse_numeric_value(row.get("usage", 0)))},
                                {"title": "조치", "value": "교체완료"},
                                {"title": "처리일", "value": date.today().isoformat()},
                            ],
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
                            "facts": [
                                {"title": "설비", "value": row["machine"]},
                                {"title": "날물", "value": row["displayBladeName"]},
                                {"title": "사용률", "value": f"{round(row['rate'] * 100)}%"},
                                {"title": "잔여사용량", "value": row["displayRemaining"]},
                                {"title": "예측교체", "value": row["predictedDate"]},
                            ],
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
            latest_message = f"{machine} 설비 날물 교체 알림을 전송했습니다."
        except Exception as exc:
            latest_message = f"{machine} 설비 날물 교체 알림 전송 실패: {exc}"

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
        if machine.startswith(("수직", "포인트", "런닝", "양면")) and quantity_col:
            usage_count = parsed_quantity
            usage_m = parsed_quantity
        prod_date = row[date_col] if date_col else None
        if not machine or usage_m <= 0:
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
    for (machine, blade_name), payload in grouped.items():
        dates = pd.to_datetime(pd.Series(payload["dates"]), errors="coerce").dropna().sort_values()
        start_date = dates.iloc[0].date().isoformat() if not dates.empty else ""
        period_days = EDGE_UPLOAD_RULES.get(machine, {"periodDays": 7})["periodDays"]
        if not is_duplicate_sync:
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

    sync_time = now_kst().strftime("%Y-%m-%d %H:%M:%S")
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
    if history_entries and not is_duplicate_sync:
        st.session_state.sheet_sync_history = history_entries + st.session_state.sheet_sync_history
        save_sheet_sync_history(st.session_state.sheet_sync_history)
    st.session_state.last_sheet_sync_at = normalize_display_timestamp(sync_time)
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


def handle_action(row_id: int) -> None:
    selected_item = next((item for item in st.session_state.equipment_data if item["id"] == row_id), None)
    if selected_item is None:
        return

    today = date.today().isoformat()
    selected_machine = selected_item["machine"]
    completed_usage = parse_numeric_value(selected_item.get("usage", 0))
    completed_usage_label = format_cycle_value(selected_item, completed_usage)
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
    st.session_state.sheet_sync_history = [
        entry
        for entry in st.session_state.get("sheet_sync_history", [])
        if normalize_machine_name(str(entry.get("설비", entry.get("?ㅻ퉬", ""))).strip()) != selected_machine
    ]
    save_sheet_sync_history(st.session_state.sheet_sync_history)
    st.session_state.last_sheet_sync_details = [
        detail
        for detail in st.session_state.get("last_sheet_sync_details", [])
        if normalize_machine_name(str(detail.get("machine", detail.get("설비", ""))).strip()) != selected_machine
    ]
    completion_entry = {
        "교체완료시각": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "설비": selected_item["machine"],
        "날물명": get_display_blade_name(selected_item),
        "교체 시점 사용량": completed_usage_label,
    }
    st.session_state.completion_history = [completion_entry, *st.session_state.get("completion_history", [])]
    message = f"{selected_item['machine']} 교체 완료 처리되었습니다."
    try:
        send_teams_complete_alert(selected_item)
        message += " Teams 알림도 전송했습니다."
    except Exception as exc:
        message += f" Teams 알림 전송 실패: {exc}"
    st.session_state.send_result = message
    save_dashboard_state()


def get_action_label(row: dict[str, Any]) -> str:
    if row.get("rate", 0) >= 1:
        return "교체"
    return "정상"


def render_kpis(enriched: list[dict[str, Any]]) -> None:
    replace_now = len([d for d in enriched if d["status"] == "replace"])
    due_soon = len([d for d in enriched if d["remainDays"] <= 3])
    avg_rate = round(sum(d["rate"] for d in enriched) / len(enriched) * 100) if enriched else 0
    cards = [
        ("관리 날물", f"{len(enriched)} EA", "실시간 관리 대상"),
        ("즉시 교체", f"{replace_now} 건", "사용률 기준"),
        ("3일 내 교체예정", f"{due_soon} 건", "선조달 필요"),
        ("평균 사용률", f"{avg_rate}%", "라인 평균"),
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
        label = "교체필요"
    elif status == "caution":
        label = "주의"
    else:
        label = "불필요"
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
                <div>라인</div>
                <div>설비</div>
                <div>날물명</div>
                <div>기준값</div>
                <div>사용률</div>
                <div>잔여사용량</div>
                <div>예측교체</div>
                <div>교체상태</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        for row in rows:
            line_label = "엣지" if row["line"] == "엣지" else "보링"
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
                if get_action_label(row) == "교체":
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
    has_sync_result = bool(st.session_state.get("last_sheet_sync_details")) and bool(st.session_state.get("last_sheet_sync_at"))
    if auto_sheet_updated_at and (
        auto_sheet_updated_at != st.session_state.get("last_applied_upload_at", "")
        or not has_sync_result
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
            save_dashboard_state()
        except Exception:
            pass
    auto_sync_fragment()
    enriched = enrich_data(st.session_state.equipment_data)
    process_replace_alerts(enriched)

    st.title("날물 교체관리 대시보드")
    st.caption("FURSYS · 충주 공장 · 품질보증팀")

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
        if st.button("최근 반영 결과 리셋", use_container_width=True):
            reset_last_sheet_sync_result()
            st.rerun()
        if st.button("반영 이력 리셋", use_container_width=True):
            reset_sheet_sync_history_data()
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
            st.session_state.line_filter_toggle = "all"
            st.session_state.line_machine_filter = "전체"
            st.rerun()
        for idx, line_name in enumerate(available_lines, start=1):
            active = st.session_state.get("line_filter_toggle", "all") == line_name
            if line_button_cols[idx].button(line_name, key=f"line_toggle_{line_name}", use_container_width=True, type="primary" if active else "secondary"):
                st.session_state.line_filter_toggle = line_name
                st.session_state.line_machine_filter = "전체"
                st.rerun()
        active_line_filter = st.session_state.get("line_filter_toggle", "all")
        if active_line_filter != "all":
            machine_options = ["전체", *[machine for machine in LINE_MACHINE_OPTIONS.get(active_line_filter, []) if any(row["machine"] == machine for row in enriched)]]
            st.selectbox(
                f"{active_line_filter} 세부 선택",
                machine_options,
                key="line_machine_filter",
            )
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

        st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)
        st.caption("데이터 반영 이력")
        if st.session_state.sheet_sync_history:
            history_df = pd.DataFrame(st.session_state.sheet_sync_history)
            history_date_options = build_date_filter_options(history_df["반영시각"].tolist())
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
            selected_history_date = history_filter_cols[0].selectbox("날짜", history_date_options, key="history_date_filter")
            if selected_history_date != "전체":
                history_df = history_df[
                    history_df["반영시각"].apply(lambda value: (parsed := parse_date_only(value)) is not None and parsed.isoformat() == selected_history_date)
                ]
            history_df = expand_history_rows_by_blade(history_df)
            machine_options = ["전체", *sorted([value for value in history_df["설비"].dropna().astype(str).unique() if value.strip()])]
            selected_history_machine = history_filter_cols[1].selectbox("설비", machine_options, key="history_machine_filter")
            if selected_history_machine != "전체":
                history_df = history_df[history_df["설비"] == selected_history_machine]
            blade_options = ["전체", *sorted([value for value in history_df["날물명"].dropna().astype(str).unique() if value.strip()])]
            selected_history_blade = history_filter_cols[2].selectbox("날물명", blade_options, key="history_blade_filter")
            if selected_history_blade != "전체":
                history_df = history_df[history_df["날물명"] == selected_history_blade]
            history_df["_sort_time"] = pd.to_datetime(history_df["반영시각"], errors="coerce")
            dedupe_columns = [column for column in ["설비", "날물명", "반영 사용량(m)", "반영 사용량(회)", "데이터 기준일자"] if column in history_df.columns]
            if dedupe_columns:
                history_df = (
                    history_df
                    .sort_values(by="_sort_time", ascending=False, na_position="last")
                    .drop_duplicates(subset=dedupe_columns, keep="first")
                    .sort_values(by="_sort_time", ascending=False, na_position="last")
                )
            for column in ["반영 사용량(m)", "반영 사용량(회)"]:
                if column in history_df.columns:
                    history_df[column] = history_df[column].where(history_df[column].notna(), "")
            history_df = history_df.drop(columns=["_line", "_sort_time"], errors="ignore")
            if not history_df.empty:
                st.dataframe(format_sync_display_dataframe(history_df), use_container_width=True)
            else:
                st.info("조건에 맞는 반영 이력이 없습니다.")
        else:
            st.info("아직 반영 이력이 없습니다.")

        st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)
        st.caption("교체완료 시점")
        if st.session_state.get("completion_history"):
            completion_df = pd.DataFrame(st.session_state.get("completion_history", []))
            completion_date_options = build_date_filter_options(completion_df["교체완료시각"].tolist())
            ordered_columns = ["교체완료시각", "설비", "날물명", "교체 시점 사용량"]
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
            selected_completion_date = completion_filter_cols[0].selectbox("날짜", completion_date_options, key="completion_date_filter")
            if selected_completion_date != "전체":
                completion_df = completion_df[
                    completion_df["교체완료시각"].apply(lambda value: (parsed := parse_date_only(value)) is not None and parsed.isoformat() == selected_completion_date)
                ]
            completion_machine_options = ["전체", *sorted([value for value in completion_df["설비"].dropna().astype(str).unique() if value.strip()])]
            selected_completion_machine = completion_filter_cols[1].selectbox("설비", completion_machine_options, key="completion_machine_filter")
            if selected_completion_machine != "전체":
                completion_df = completion_df[completion_df["설비"] == selected_completion_machine]
            completion_blade_options = ["전체", *sorted([value for value in completion_df["날물명"].dropna().astype(str).unique() if value.strip()])]
            selected_completion_blade = completion_filter_cols[2].selectbox("날물명", completion_blade_options, key="completion_blade_filter")
            if selected_completion_blade != "전체":
                completion_df = completion_df[completion_df["날물명"] == selected_completion_blade]
            if not completion_df.empty:
                st.dataframe(format_sync_display_dataframe(completion_df), use_container_width=True)
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
