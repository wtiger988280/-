from __future__ import annotations

import argparse
import shutil
import json
import logging
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

try:
    import gspread
except ImportError:  # pragma: no cover
    gspread = None


DESKTOP_DIR = Path.home() / "Desktop"
WORK_DIR = Path.cwd()
DEFAULT_MAPPING = WORK_DIR / "edge_mapping.xlsx"
DEFAULT_BORING_MACRO_PATHS = [
    DESKTOP_DIR / "mpr 추출 매크로 보링.xlsm",
    DESKTOP_DIR / "mpr 추출 매크로 (2).xlsm",
    DESKTOP_DIR / "mpr 추출 매크로 (3).xlsm",
    DESKTOP_DIR / "mpr 추출 매크로 (4).xlsm",
    DESKTOP_DIR / "mpr 추출 매크로 (5).xlsm",
]
OUTPUT_DIR = WORK_DIR / "output"
LOG_DIR = WORK_DIR / "logs"
DEBUG_LOG_PATH = LOG_DIR / "edge_pipeline.log"
LATEST_UPLOAD_INFO_PATH = LOG_DIR / "latest_sheet_upload.json"
KST = ZoneInfo("Asia/Seoul")
TEXT_EDGE = "\uc5e3\uc9c0"
TEXT_BORING = "\ubcf4\ub9c1"
COL_MACHINE = "\uc124\ube44\uba85"
COL_MACHINE_ALT = "\uc124\ube44\uba85\u25bc"
COL_VENDOR = "\uc2e4\uc801\ub4f1\ub85d\ucc98"
COL_PARTNER = "\ud611\ub825\uc0ac"
COL_DATE = "\uc0dd\uc0b0\uc77c"
COL_WORK_DATE = "\uc791\uc5c5\uc77c"
COL_MATERIAL = "\uc7ac\uc9c8"
COL_MATERIAL_ALT = "\uc7ac\uc9c8\u25b2"
COL_EDGE_USAGE = "\uc5e3\uc9c0\uc0ac\uc6a9\ub7c9(m)"
COL_EDGE_USAGE_TOTAL = "\ucd1d\uc5e3\uc9c0\uc0ac\uc6a9\ub7c9(m)"
H_SYNC = "\ubc18\uc601\uc2dc\uac01"
H_TARGET = "\ub300\uc0c1"
H_MACHINE = "\uc124\ube44"
H_BLADE = "\ub0a0\ubb3c\uba85"
H_USAGE_M = "\ubc18\uc601 \uc0ac\uc6a9\ub7c9(m)"
H_USAGE_C = "\ubc18\uc601 \uc0ac\uc6a9\ub7c9(\ud68c)"
H_BASE_DATE = "\uc2dc\uc791\uc77c"


def now_kst() -> datetime:
    return datetime.now(KST)


def extract_sync_time_from_text(value: object) -> str:
    text = "" if value is None else str(value)
    match = re.search(r"grd_List_(\d{14})", text, flags=re.IGNORECASE)
    if not match:
        return now_kst().strftime("%Y-%m-%d %H:%M:%S")


def normalize_history_date_value(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return text
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=KST)
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return now_kst().strftime("%Y-%m-%d %H:%M:%S")

ERP_REQUIRED_COLUMNS = {"부품코드", "색상", "부품명", "생산량"}
ERP_STRUCTURAL_COLUMNS = {"투입구분", "투입일", "생산일", "포장일자", "계획량", "투입량"}
ERP_STRONG_COLUMNS = {"부품이동카드번호", "포장라인", "재공평가금액"}
TRANSFORMED_REQUIRED_COLUMNS = {
    "번호",
    "보조번호",
    "작업자",
    "투입일",
    "생산일",
    "제품코드",
    "색상",
    "로트번호",
    "부품명",
    "생산량",
    "생산(분)",
    "설비명",
    "실적등록처",
    "건명",
    "공정명",
    "생산액",
    "계획량",
    "부품코드",
    "부품색상",
    "계획차수",
    "불량수량",
    "등록일자",
    "작업시작시간",
    "작업완료시간",
    "기준인원",
    "표준시간",
    "표준시간*생산량",
    "작업인원",
    "보정시간",
    "보정시간*생산량",
    "규격상세",
    "재질",
    "생산지시일",
    "다음공정",
    "포장라인",
    "재공평가금액",
    "재공평가합",
    "총작업시간",
    "표준시간.1",
    "특기사항",
}
TRANSFORMED_OPTIONAL_COLUMNS = {"엣지사용량(m)"}

BORING_BLADE_COLUMNS = [
    "Φ5(관통) 날물",
    "Φ8(관통) 날물",
    "Φ12(관통) 날물",
    "Φ15 날물",
    "Φ20 날물",
    "Φ35 날물",
]

ERP_COLUMNS = [
    "번호",
    "보조번호",
    "투입구분",
    "투입일",
    "생산일",
    "포장일자",
    "부품코드",
    "색상",
    "부품명",
    "계획량",
    "투입량",
    "생산량",
    "불량",
    "공정검사이력",
    "생산계획시작시간",
    "생산계획종료시간",
    "작업시작시간",
    "작업완료시간",
    "소요시간",
    "중량_소",
    "중량_대",
    "기중량_소",
    "기중량_대",
    "작업자",
    "청구자",
    "관리번호",
    "Lotno",
    "부품이동카드번호",
    "특기사항",
    "계획차수",
    "현수량",
    "협력사",
    "재료비",
    "가공비",
    "경비",
    "합계",
    "재공평가금액",
    "포장라인",
    "표준시간_초",
    "규격가로_mm",
    "규격세로_mm",
    "규격높이_mm",
    "ERP_엣지구분",
    "ERP_엣지가로_mm",
    "ERP_엣지세로_mm",
    "ERP_엣지두께_mm",
    "ERP_엣지사양A",
    "ERP_엣지사양B",
    "ERP_엣지사양C",
    "ERP_엣지사양D",
]

ERP_HEADER_ALIASES = {
    "Unnamed: 1": "보조번호",
    "0": "보조번호",
    0: "보조번호",
    "부품명▼": "부품명",
    "중량(小)": "중량_소",
    "중량(大)": "중량_대",
    "기중량(小)": "기중량_소",
    "기중량(大)": "기중량_대",
    "표준시간(초)": "표준시간_초",
    "규격가로(mm)": "규격가로_mm",
    "규격세로(mm)": "규격세로_mm",
    "규격높이(mm)": "규격높이_mm",
    "엣지 구분": "ERP_엣지구분",
    "엣지구분": "ERP_엣지구분",
    "엣지가로(mm)": "ERP_엣지가로_mm",
    "엣지 가로(mm)": "ERP_엣지가로_mm",
    "엣지세로(mm)": "ERP_엣지세로_mm",
    "엣지 세로(mm)": "ERP_엣지세로_mm",
    "엣지두께(mm)": "ERP_엣지두께_mm",
    "엣지 두께(mm)": "ERP_엣지두께_mm",
}

MAPPING_COLUMNS = [
    "자재코드",
    "색상",
    "부품명",
    "규격가로_mm",
    "규격세로_mm",
    "규격높이_mm",
    "기준_엣지구분",
    "기준_엣지가로_mm",
    "기준_엣지세로_mm",
    "기준_엣지두께_mm",
    "기준_엣지사양A",
    "기준_엣지사양B",
    "기준_엣지사양C",
    "기준_엣지사양D",
]

RESOLVED_EDGE_COLUMNS = [
    ("엣지구분", "ERP_엣지구분", "기준_엣지구분"),
    ("엣지가로_mm", "ERP_엣지가로_mm", "기준_엣지가로_mm"),
    ("엣지세로_mm", "ERP_엣지세로_mm", "기준_엣지세로_mm"),
    ("엣지두께_mm", "ERP_엣지두께_mm", "기준_엣지두께_mm"),
    ("엣지사양A", "ERP_엣지사양A", "기준_엣지사양A"),
    ("엣지사양B", "ERP_엣지사양B", "기준_엣지사양B"),
    ("엣지사양C", "ERP_엣지사양C", "기준_엣지사양C"),
    ("엣지사양D", "ERP_엣지사양D", "기준_엣지사양D"),
]


def setup_logging(debug: bool) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("edge_pipeline")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    file_handler = logging.FileHandler(DEBUG_LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)

    console_handler = SafeConsoleHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    logger.debug("logging_initialized")
    return logger


class SafeConsoleHandler(logging.StreamHandler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            safe_message = message.encode("cp949", errors="replace").decode("cp949", errors="replace")
            self.stream.write(safe_message + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ERP 데이터를 기준표와 병합해 엑셀/CSV 및 Google Sheets로 보냅니다.")
    parser.add_argument("--erp", help="ERP 원본 파일 경로(.xls/.xlsx/.xlsm)")
    parser.add_argument("--mapping", default=str(DEFAULT_MAPPING), help="기준표 경로")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="결과 저장 폴더")
    parser.add_argument("--sheet-config", help="Google Sheets 설정 JSON 경로")
    parser.add_argument("--debug", action="store_true", help="콘솔에도 디버그 로그를 표시")
    return parser.parse_args()


def normalize_part_name(value: object) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_color(value: object) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"\s+", "", text).strip().upper()


def normalize_mpr_key(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    text = text.replace("\\", "/").split("/")[-1].strip()
    text = re.sub(r"\.mpr$", "", text, flags=re.IGNORECASE)
    return text.upper()


def format_dimension(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return ""
    if float(numeric).is_integer():
        return str(int(numeric))
    return str(float(numeric)).rstrip("0").rstrip(".")


def parse_dimension_pair(detail: object) -> tuple[float, float]:
    text = "" if detail is None else str(detail)
    matches = re.findall(r"\d+(?:\.\d+)?", text)
    if len(matches) < 2:
        return 0.0, 0.0
    return float(matches[0]), float(matches[1])


def parse_material_flags(value: object) -> tuple[int, int, int, int]:
    text = "" if value is None else str(value)
    parts = [part.strip() for part in text.split("/")]
    while len(parts) < 4:
        parts.append("-")

    flags: list[int] = []
    for part in parts[:4]:
        if part.startswith("-"):
            flags.append(0)
            continue
        flags.append(1 if re.search(r"\d", part) else 0)
    return tuple(flags)


def pick_existing_column(columns: list[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def pick_column_by_keywords(columns: list[str], *keywords: str) -> str | None:
    for column in columns:
        text = "" if column is None else str(column)
        if all(keyword in text for keyword in keywords):
            return column
    return None


def load_boring_macro_table(path: Path, logger: logging.Logger) -> pd.DataFrame:
    logger.debug("load_boring_macro_table path=%s", path)
    raw = pd.read_excel(path, sheet_name=0, header=0)
    if "파일명" not in raw.columns:
        raise ValueError(f"보링 매크로 파일에 '파일명' 열이 없습니다: {path.name}")

    boring = raw.copy()
    boring["파일명"] = boring["파일명"].fillna("").astype(str).str.strip()
    boring = boring[boring["파일명"] != ""]

    numeric_sources = {
        "5_V": ["5_V", "5_H"],
        "8_V": ["8_V", "8_H"],
        "12_V": ["12_V"],
        "15_V": ["15_V"],
        "20_V": ["20_V"],
        "35_V": ["35_V"],
    }
    for required_col in {item for values in numeric_sources.values() for item in values}:
        if required_col not in boring.columns:
            boring[required_col] = 0

    boring["macro_key"] = boring["파일명"].map(normalize_mpr_key)
    boring["Φ5(관통) 날물"] = sum(pd.to_numeric(boring[col], errors="coerce").fillna(0) for col in numeric_sources["5_V"])
    boring["Φ8(관통) 날물"] = sum(pd.to_numeric(boring[col], errors="coerce").fillna(0) for col in numeric_sources["8_V"])
    boring["Φ12(관통) 날물"] = pd.to_numeric(boring["12_V"], errors="coerce").fillna(0)
    boring["Φ15 날물"] = pd.to_numeric(boring["15_V"], errors="coerce").fillna(0)
    boring["Φ20 날물"] = pd.to_numeric(boring["20_V"], errors="coerce").fillna(0)
    boring["Φ35 날물"] = pd.to_numeric(boring["35_V"], errors="coerce").fillna(0)

    boring = (
        boring.sort_values("macro_key")
        .drop_duplicates("macro_key", keep="first")
        [["macro_key", *BORING_BLADE_COLUMNS]]
    )
    logger.info("보링 매크로 로드 완료: %s행", len(boring))
    return boring


def load_combined_boring_macro_table(paths: list[Path], logger: logging.Logger) -> pd.DataFrame:
    tables: list[pd.DataFrame] = []
    existing_paths = [path for path in paths if path.exists()]
    if not existing_paths:
        missing = ", ".join(str(path) for path in paths)
        raise FileNotFoundError(f"보링 매크로 파일을 찾지 못했습니다: {missing}")

    for path in existing_paths:
        tables.append(load_boring_macro_table(path, logger))

    combined = pd.concat(tables, ignore_index=True)
    combined = (
        combined.sort_values("macro_key")
        .drop_duplicates("macro_key", keep="first")
        [["macro_key", *BORING_BLADE_COLUMNS]]
    )
    logger.info("보링 매크로 통합 완료: 파일 %s개, 총 %s행", len(existing_paths), len(combined))
    return combined


def apply_boring_macro_columns(merged: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    boring_macro = load_combined_boring_macro_table(DEFAULT_BORING_MACRO_PATHS, logger)
    boring_lookup = {
        row["macro_key"]: {column: row[column] for column in BORING_BLADE_COLUMNS}
        for _, row in boring_macro.iterrows()
        if row["macro_key"]
    }

    def build_candidates(row: pd.Series) -> list[str]:
        raw_values = [
            row.get("부품코드", ""),
            row.get("제품코드", ""),
            row.get("부품명", ""),
        ]
        candidates: list[str] = []
        for raw in raw_values:
            normalized = normalize_mpr_key(raw)
            if normalized and normalized not in candidates:
                candidates.append(normalized)
        return candidates

    match_keys: list[str] = []
    boring_values = {column: [] for column in BORING_BLADE_COLUMNS}
    for _, row in merged.iterrows():
        matched_key = ""
        matched_payload = {column: 0 for column in BORING_BLADE_COLUMNS}
        production_qty = pd.to_numeric(pd.Series([row.get("생산량", 0)]), errors="coerce").fillna(0).iloc[0]
        for candidate in build_candidates(row):
            if candidate in boring_lookup:
                matched_key = candidate
                matched_payload = boring_lookup[candidate]
                break
        match_keys.append(matched_key)
        for column in BORING_BLADE_COLUMNS:
            boring_values[column].append(float(matched_payload[column]) * float(production_qty))

    merged["보링매크로매칭키"] = match_keys
    for column in BORING_BLADE_COLUMNS:
        merged[column] = boring_values[column]

    matched_count = int(pd.Series(match_keys).astype(str).ne("").sum())
    logger.info("보링 매크로 매칭 완료: 총 %s행, 매칭 %s행", len(merged), matched_count)
    return merged



def build_boring_output_columns(merged: pd.DataFrame, output_columns: list[str]) -> list[str]:
    ordered_columns = [column for column in output_columns if column != "엣지사용량(m)"]
    for column in BORING_BLADE_COLUMNS:
        if column in merged.columns and column not in ordered_columns:
            ordered_columns.append(column)
    return ordered_columns


def convert_xls_to_xlsx(source: Path, destination_dir: Path, logger: logging.Logger) -> Path:
    destination = destination_dir / f"{source.stem}_converted.xlsx"
    temp_dir = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "Temp" / "edge_erp_convert"
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_id = uuid.uuid4().hex[:10]
    temp_source = temp_dir / f"input_{temp_id}.xls"
    temp_destination = temp_dir / f"output_{temp_id}.xlsx"
    last_error: Exception | None = None
    for _ in range(3):
        try:
            shutil.copy2(source, temp_source)
            last_error = None
            break
        except PermissionError as exc:
            last_error = exc
            time.sleep(1)
    if last_error is not None:
        raise RuntimeError(f"원본 ERP 파일이 다른 프로그램에서 사용 중입니다: {source.name}")
    logger.debug("convert_xls_to_xlsx source=%s temp_source=%s destination=%s", source, temp_source, destination)
    powershell_script = f"""
$ErrorActionPreference = 'Stop'
$source = '{str(temp_source)}'
$destination = '{str(temp_destination)}'
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
try {{
    $workbook = $excel.Workbooks.Open($source)
    $workbook.SaveAs($destination, 51)
    $workbook.Close($false)
}} finally {{
    if ($workbook) {{ [System.Runtime.Interopservices.Marshal]::ReleaseComObject($workbook) | Out-Null }}
    $excel.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
}}
"""
    result = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", powershell_script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        logger.error("xls_conversion_failed source=%s stderr=%s", source, result.stderr.strip())
        raise RuntimeError(f".xls 변환 실패: {source.name}")
    shutil.copy2(temp_destination, destination)
    return destination


def read_excel_file(path: Path, logger: logging.Logger) -> pd.DataFrame:
    logger.debug("read_excel_file path=%s", path)
    suffix = path.suffix.lower()
    actual_path = path
    if suffix == ".xls":
        actual_path = convert_xls_to_xlsx(path, OUTPUT_DIR, logger)
    return pd.read_excel(actual_path)


def rename_columns_by_position(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    if len(df.columns) < len(expected_columns):
        raise ValueError(f"예상 컬럼 수보다 적습니다. expected={len(expected_columns)}, actual={len(df.columns)}")
    renamed = df.copy()
    renamed.columns = expected_columns + list(df.columns[len(expected_columns):])
    return renamed


def standardize_erp_columns(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    renamed = df.rename(columns=ERP_HEADER_ALIASES).copy()
    columns = set(map(str, renamed.columns))
    output_columns = list(renamed.columns)

    if ERP_REQUIRED_COLUMNS.issubset(columns):
        logger.debug("standardize_erp_columns mode=header_based columns=%s", list(renamed.columns))
        for column in ERP_COLUMNS:
            if column not in renamed.columns:
                renamed[column] = pd.NA
        renamed.attrs["output_columns"] = output_columns
        return renamed

    logger.debug("standardize_erp_columns mode=position_based column_count=%s", len(df.columns))
    renamed = rename_columns_by_position(df, ERP_COLUMNS)
    renamed.attrs["output_columns"] = ERP_COLUMNS[: len(df.columns)]
    return renamed


def is_probable_erp_file(path: Path, logger: logging.Logger) -> bool:
    lower_name = path.name.lower()
    skip_patterns = [
        "엣지구분",
        "_merged",
        "_unmatched",
        "고객",
        "개선대책서",
        "검사일지",
        "기준",
        "~$",
        "7일데이터",
        "7일치 데이터",
        "10일치 데이터",
        "날물이력",
    ]
    if any(pattern in lower_name for pattern in skip_patterns):
        logger.debug("skip_candidate name=%s reason=name_pattern", path.name)
        return False

    try:
        sample_df = read_excel_file(path, logger).head(3)
    except Exception as exc:
        logger.debug("skip_candidate name=%s reason=read_error error=%s", path.name, exc)
        return False

    sample_columns = set(map(str, sample_df.rename(columns=ERP_HEADER_ALIASES).columns))
    structural_score = len(sample_columns & ERP_STRUCTURAL_COLUMNS)
    strong_score = len(sample_columns & ERP_STRONG_COLUMNS)
    column_count = len(sample_columns)
    transformed_columns = set(sample_columns)
    if "설비명▼" in transformed_columns:
        transformed_columns.add("설비명")
    if "재질▲" in transformed_columns:
        transformed_columns.add("재질")
    transformed_matches = TRANSFORMED_REQUIRED_COLUMNS.issubset(transformed_columns)
    logger.debug(
        "candidate_check name=%s columns=%s structural_score=%s strong_score=%s",
        path.name,
        column_count,
        structural_score,
        strong_score,
    )
    return (
        ERP_REQUIRED_COLUMNS.issubset(sample_columns)
        and transformed_matches
        and strong_score >= 2
        and 40 <= column_count <= 41
    )


def detect_latest_erp_file(logger: logging.Logger) -> Path:
    candidates = sorted(
        [path for path in DESKTOP_DIR.iterdir() if path.is_file() and path.suffix.lower() in {".xls", ".xlsx", ".xlsm"}],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    logger.debug("detect_latest_erp_file total_candidates=%s", len(candidates))

    latest_grd = [
        path
        for path in candidates
        if path.name.lower().startswith("grd_list")
    ]
    valid_latest_grd = [path for path in latest_grd if is_probable_erp_file(path, logger)]
    if valid_latest_grd:
        selected = valid_latest_grd[0]
        logger.info("자동선택 ERP 파일(grd 우선): %s", selected.name)
        return selected

    likely_names = [path for path in candidates if any(token in path.name.lower() for token in ["grd_list", "수주내역", "공정부적합", "erp"])]
    recent_others = [path for path in candidates if path not in likely_names][:6]
    search_pool = likely_names[:8] + recent_others
    probable = [path for path in search_pool if is_probable_erp_file(path, logger)]
    if not probable:
        raise FileNotFoundError("바탕화면에서 ERP 형식 파일을 찾지 못했습니다. --erp 옵션으로 직접 지정해 주세요.")

    selected = probable[0]
    logger.info("자동선택 ERP 파일: %s", selected.name)
    return selected


def build_mapping_tables(mapping_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mapping = rename_columns_by_position(mapping_df, MAPPING_COLUMNS)
    mapping["규격가로_mm_기준표"] = mapping["규격가로_mm"]
    mapping["규격세로_mm_기준표"] = mapping["규격세로_mm"]
    mapping["규격높이_mm_기준표"] = mapping["규격높이_mm"]
    mapping["part_name_norm"] = mapping["부품명"].map(normalize_part_name)
    mapping["color_norm"] = mapping["색상"].map(normalize_color)
    mapping["name_color_key"] = mapping["part_name_norm"] + "|" + mapping["color_norm"]

    key_mapping = (
        mapping.sort_values(["name_color_key", "자재코드"])
        .drop_duplicates("name_color_key", keep="first")
        [[
            "name_color_key",
            "기준_엣지구분",
            "기준_엣지가로_mm",
            "기준_엣지세로_mm",
            "기준_엣지두께_mm",
            "기준_엣지사양A",
            "기준_엣지사양B",
            "기준_엣지사양C",
            "기준_엣지사양D",
            "규격가로_mm_기준표",
            "규격세로_mm_기준표",
            "규격높이_mm_기준표",
        ]]
    )

    fallback_mapping = (
        mapping.groupby("part_name_norm", dropna=False)
        .agg(
            unique_key_count=("name_color_key", "nunique"),
            기준_엣지구분=("기준_엣지구분", "first"),
            기준_엣지가로_mm=("기준_엣지가로_mm", "first"),
            기준_엣지세로_mm=("기준_엣지세로_mm", "first"),
            기준_엣지두께_mm=("기준_엣지두께_mm", "first"),
            기준_엣지사양A=("기준_엣지사양A", "first"),
            기준_엣지사양B=("기준_엣지사양B", "first"),
            기준_엣지사양C=("기준_엣지사양C", "first"),
            기준_엣지사양D=("기준_엣지사양D", "first"),
            규격가로_mm_기준표=("규격가로_mm_기준표", "first"),
            규격세로_mm_기준표=("규격세로_mm_기준표", "first"),
            규격높이_mm_기준표=("규격높이_mm_기준표", "first"),
        )
        .reset_index()
    )
    fallback_mapping = fallback_mapping[fallback_mapping["unique_key_count"] == 1].drop(columns=["unique_key_count"])
    return key_mapping, fallback_mapping


def merge_erp_with_mapping(erp_df: pd.DataFrame, mapping_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    erp = standardize_erp_columns(erp_df, logger)
    output_columns = erp.attrs.get("output_columns", list(erp.columns))
    key_mapping, fallback_mapping = build_mapping_tables(mapping_df)

    erp["part_name_norm"] = erp["부품명"].map(normalize_part_name)
    erp["color_norm"] = erp["색상"].map(normalize_color)
    erp["name_color_key"] = erp["part_name_norm"] + "|" + erp["color_norm"]

    merged = erp.merge(key_mapping, on="name_color_key", how="left")
    fallback = erp.merge(fallback_mapping, on="part_name_norm", how="left", suffixes=("", "_fallback"))

    fill_columns = [
        "기준_엣지구분",
        "기준_엣지가로_mm",
        "기준_엣지세로_mm",
        "기준_엣지두께_mm",
        "기준_엣지사양A",
        "기준_엣지사양B",
        "기준_엣지사양C",
        "기준_엣지사양D",
        "규격가로_mm_기준표",
        "규격세로_mm_기준표",
        "규격높이_mm_기준표",
    ]
    for column in fill_columns:
        merged[column] = merged[column].combine_first(fallback[column])

    merged["규격가로_mm"] = merged["규격가로_mm"].combine_first(merged["규격가로_mm_기준표"])
    merged["규격세로_mm"] = merged["규격세로_mm"].combine_first(merged["규격세로_mm_기준표"])
    merged["규격높이_mm"] = merged["규격높이_mm"].combine_first(merged["규격높이_mm_기준표"])

    for output_col, erp_col, ref_col in RESOLVED_EDGE_COLUMNS:
        merged[output_col] = merged[ref_col].combine_first(merged[erp_col])

    quantity = pd.to_numeric(merged["생산량"], errors="coerce").fillna(0)

    material_source_column = pick_existing_column(list(merged.columns), "재질", "재질▲")

    if "규격상세" in merged.columns and material_source_column:
        parsed_dimensions = merged["규격상세"].map(parse_dimension_pair)
        spec_width = parsed_dimensions.map(lambda x: x[0])
        spec_height = parsed_dimensions.map(lambda x: x[1])
        parsed_materials = merged[material_source_column].map(parse_material_flags)
        width_count = parsed_materials.map(lambda x: x[0] + x[2])
        height_count = parsed_materials.map(lambda x: x[1] + x[3])
    else:
        spec_a = merged["엣지사양A"].fillna("").astype(str).str.strip().ne("")
        spec_b = merged["엣지사양B"].fillna("").astype(str).str.strip().ne("")
        spec_c = merged["엣지사양C"].fillna("").astype(str).str.strip().ne("")
        spec_d = merged["엣지사양D"].fillna("").astype(str).str.strip().ne("")
        width_count = spec_a.astype(int) + spec_c.astype(int)
        height_count = spec_b.astype(int) + spec_d.astype(int)
        spec_width = pd.to_numeric(merged["규격가로_mm"], errors="coerce").fillna(0)
        spec_height = pd.to_numeric(merged["규격세로_mm"], errors="coerce").fillna(0)

    equipment_source_column = (
        pick_existing_column(list(merged.columns), "설비명▼", "설비명", "협력사") or "협력사"
    )
    equipment_name = merged[equipment_source_column].fillna("").astype(str).str.replace(" ", "", regex=False)
    allowed_equipment = {
        "더블엣지밴더#6",
        "엣지밴더#1",
        "엣지밴더#2",
        "신규엣지밴더#3",
        "신규엣지밴더#4",
        "신규엣지밴더#5",
    }
    is_allowed_equipment = equipment_name.isin(allowed_equipment)
    edge_usage_mm = ((spec_width * width_count) + (spec_height * height_count)) * quantity
    merged["엣지사용량(m)"] = edge_usage_mm.where(is_allowed_equipment, 0) / 1000

    exact_match_count = int(merged["name_color_key"].isin(set(key_mapping["name_color_key"])).sum())
    logger.info("병합 완료: 총 %s행, 정확매칭 %s행", len(merged), exact_match_count)

    if detect_dataset_type(merged) == "보링":
        merged = apply_boring_macro_columns(merged, logger)
        return merged[build_boring_output_columns(merged, output_columns)]

    ordered_columns = [
        *output_columns,
        "엣지사용량(m)",
    ]
    return merged[ordered_columns]


def safe_write_excel(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_excel(path, index=False)
        return path
    except PermissionError:
        fallback = path.with_name(f"{path.stem}_{now_kst().strftime('%Y%m%d_%H%M%S')}{path.suffix}")
        df.to_excel(fallback, index=False)
        return fallback


def safe_write_csv(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path
    except PermissionError:
        fallback = path.with_name(f"{path.stem}_{now_kst().strftime('%Y%m%d_%H%M%S')}{path.suffix}")
        df.to_csv(fallback, index=False, encoding="utf-8-sig")
        return fallback


def write_outputs(result_df: pd.DataFrame, output_dir: Path, erp_source: Path, logger: logging.Logger) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    excel_output = safe_write_excel(result_df, output_dir / f"{erp_source.stem}_merged.xlsx")
    csv_output = safe_write_csv(result_df, output_dir / f"{erp_source.stem}_merged.csv")
    logger.info("엑셀 결과: %s", excel_output)
    logger.info("CSV 결과: %s", csv_output)
    return excel_output, csv_output


def load_sheet_target(config_path: Path) -> dict[str, str]:
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    return {
        "credentials_path": config["credentials_path"],
        "spreadsheet_name": config["spreadsheet_name"],
        "worksheet_name": config.get("worksheet_name", "ERP_MERGED"),
        "worksheet_mode": config.get("worksheet_mode", "new_sheet_per_upload"),
        "latest_worksheet_name": config.get("latest_worksheet_name", "DASHBOARD_LATEST"),
        "upload_info_worksheet_name": config.get("upload_info_worksheet_name", "DASHBOARD_UPLOAD_INFO"),
        "history_worksheet_name": config.get("history_worksheet_name", "DASHBOARD_SYNC_HISTORY"),
    }


def build_worksheet_title(erp_path: Path, requested_name: str | None = None) -> str:
    base = requested_name.strip() if requested_name and requested_name.strip() else erp_path.stem
    base = re.sub(r"[\[\]\*:/\\\?]", "_", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base[:80] if len(base) > 80 else base


def detect_dataset_type(df: pd.DataFrame) -> str:
    columns = list(df.columns)
    equipment_column = (
        pick_existing_column(columns, COL_MACHINE_ALT, COL_MACHINE, COL_PARTNER)
        or pick_column_by_keywords(columns, COL_MACHINE)
        or pick_column_by_keywords(columns, COL_PARTNER)
    )
    if not equipment_column:
        return "\ubbf8\ud655\uc778"
    equipment_series = df[equipment_column].fillna("").astype(str).str.replace(" ", "", regex=False)
    if equipment_series.str.contains("\uc5e3\uc9c0\ubc34\ub354|\uc2e0\uaddc\uc5e3\uc9c0\ubc34\ub354|\ub354\ube14\uc5e3\uc9c0\ubc34\ub354", regex=True).any():
        return TEXT_EDGE
    if equipment_series.str.contains(TEXT_BORING, regex=False).any():
        return TEXT_BORING
    return "\ubbf8\ud655\uc778"


def create_unique_worksheet(spreadsheet, base_title: str, row_count: int, col_count: int):
    existing_titles = {ws.title for ws in spreadsheet.worksheets()}
    title = base_title
    if title in existing_titles:
        title = f"{base_title[:68]}_{now_kst().strftime('%m%d_%H%M%S')}"
    return spreadsheet.add_worksheet(
        title=title,
        rows=str(max(row_count + 10, 1000)),
        cols=str(col_count + 5),
    )


def get_or_create_worksheet(spreadsheet, title: str, row_count: int, col_count: int):
    try:
        worksheet = spreadsheet.worksheet(title)
        worksheet.clear()
        return worksheet
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=title,
            rows=str(max(row_count + 10, 1000)),
            cols=str(col_count + 5),
        )


def write_dataframe_to_worksheet(worksheet, result_df: pd.DataFrame) -> None:
    serializable_df = result_df.copy()
    for column in serializable_df.columns:
        if pd.api.types.is_datetime64_any_dtype(serializable_df[column]):
            serializable_df[column] = serializable_df[column].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    serializable_df = serializable_df.where(pd.notna(serializable_df), "")
    records: list[list[object]] = [serializable_df.columns.tolist(), *serializable_df.values.tolist()]
    worksheet.update(records, "A1", value_input_option="USER_ENTERED")


def write_latest_upload_info(spreadsheet, worksheet, erp_path: Path, dataset_type: str) -> None:
    info = {
        "spreadsheet_name": spreadsheet.title,
        "spreadsheet_url": spreadsheet.url,
        "worksheet_title": worksheet.title,
        "worksheet_gid": str(getattr(worksheet, "id", "")),
        "erp_file_name": erp_path.name,
        "dataset_type": dataset_type,
        "updated_at": now_kst().isoformat(timespec="seconds"),
    }
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_UPLOAD_INFO_PATH.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    return info


def write_latest_upload_info_worksheet(spreadsheet, sheet_config: dict[str, str], info: dict[str, str]) -> None:
    worksheet = get_or_create_worksheet(
        spreadsheet,
        sheet_config.get("upload_info_worksheet_name", "DASHBOARD_UPLOAD_INFO"),
        2,
        len(info),
    )
    info_df = pd.DataFrame([info])
    write_dataframe_to_worksheet(worksheet, info_df)


def normalize_dashboard_machine_name(raw_value: object) -> str:
    raw = "" if raw_value is None else str(raw_value).strip()
    compact = raw.replace(" ", "")
    edge_aliases = {
        "엣지밴더#1": "엣지 #1",
        "엣지밴더#2": "엣지 #2",
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
    return raw


def build_sync_history_entries(result_df: pd.DataFrame, dataset_type: str, source_label: str = "") -> pd.DataFrame:
    sync_time = extract_sync_time_from_text(source_label)
    entries: list[dict[str, object]] = []
    columns = list(result_df.columns)
    equipment_column = (
        pick_existing_column(columns, COL_MACHINE_ALT, COL_MACHINE, COL_VENDOR)
        or pick_column_by_keywords(columns, COL_MACHINE)
        or pick_column_by_keywords(columns, COL_VENDOR)
        or (columns[11] if len(columns) > 11 else None)
    )
    date_column = (
        pick_existing_column(columns, COL_DATE, COL_WORK_DATE)
        or pick_column_by_keywords(columns, COL_DATE)
        or pick_column_by_keywords(columns, COL_WORK_DATE)
        or (columns[4] if len(columns) > 4 else None)
    )
    material_column = (
        pick_existing_column(columns, COL_MATERIAL, COL_MATERIAL_ALT)
        or pick_column_by_keywords(columns, COL_MATERIAL)
        or (columns[30] if len(columns) > 30 else None)
    )
    edge_usage_column = pick_existing_column(
        columns,
        COL_EDGE_USAGE,
        COL_EDGE_USAGE_TOTAL,
    ) or pick_column_by_keywords(columns, "\uc5e3\uc9c0\uc0ac\uc6a9\ub7c9")
    boring_blade_columns = [column for column in BORING_BLADE_COLUMNS if column in columns]
    if not boring_blade_columns and len(columns) >= 6:
        trailing_columns = columns[-6:]
        if all(str(column).startswith("Φ") for column in trailing_columns):
            boring_blade_columns = trailing_columns
    is_boring_sheet = len(boring_blade_columns) == 6

    if is_boring_sheet:
        if not equipment_column:
            return pd.DataFrame()
        boring_machines_seen: list[tuple[str, str]] = []
        aggregated: dict[tuple[str, str], dict[str, object]] = {}
        for _, row in result_df.iterrows():
            machine = normalize_dashboard_machine_name(row.get(equipment_column, ""))
            if not machine:
                continue
            base_date = str(row.get(date_column, "")).strip() if date_column else ""
            machine_key = (machine, base_date)
            if machine_key not in boring_machines_seen:
                boring_machines_seen.append(machine_key)
            for blade_column in boring_blade_columns:
                blade_usage = pd.to_numeric(pd.Series([row.get(blade_column, 0)]), errors="coerce").fillna(0).iloc[0]
                aggregate_key = (machine, blade_column, base_date)
                aggregated.setdefault(
                    aggregate_key,
                    {
                        H_SYNC: sync_time,
                        H_TARGET: "\ubcf4\ub9c1 \uc804\uccb4",
                        H_MACHINE: machine,
                        H_BLADE: blade_column,
                        H_USAGE_M: "",
                        H_USAGE_C: 0,
                        H_BASE_DATE: base_date,
                    },
                )
                aggregated[aggregate_key][H_USAGE_C] = int(round(float(aggregated[aggregate_key][H_USAGE_C]))) + int(round(float(blade_usage)))
        for machine, base_date in boring_machines_seen:
            for blade_column in boring_blade_columns:
                aggregate_key = (machine, blade_column, base_date)
                payload = aggregated.setdefault(
                    aggregate_key,
                    {
                        H_SYNC: sync_time,
                        H_TARGET: "\ubcf4\ub9c1 \uc804\uccb4",
                        H_MACHINE: machine,
                        H_BLADE: blade_column,
                        H_USAGE_M: "",
                        H_USAGE_C: 0,
                        H_BASE_DATE: base_date,
                    },
                )
                entries.append(payload)
    else:
        if not equipment_column or not edge_usage_column:
            return pd.DataFrame()
        for _, row in result_df.iterrows():
            machine = normalize_dashboard_machine_name(row.get(equipment_column, ""))
            usage_m = pd.to_numeric(pd.Series([row.get(edge_usage_column, 0)]), errors="coerce").fillna(0).iloc[0]
            if not machine or usage_m <= 0:
                continue
            base_date = str(row.get(date_column, "")).strip() if date_column else ""
            if machine == "엣지 #6":
                front_count, back_count = parse_material_flags(row.get(material_column, ""))[:2], parse_material_flags(row.get(material_column, ""))[2:]
                front_total = sum(front_count)
                back_total = sum(back_count)
                total = front_total + back_total
                if total > 0:
                    if front_total > 0:
                        entries.append(
                            {
                                H_SYNC: sync_time,
                                H_TARGET: "\uc5e3\uc9c0 \uc804\uccb4",
                                H_MACHINE: machine,
                                H_BLADE: "AT 날물(전면)",
                                H_USAGE_M: round(float(usage_m) * front_total / total, 3),
                                H_USAGE_C: "",
                                H_BASE_DATE: base_date,
                            }
                        )
                    if back_total > 0:
                        entries.append(
                            {
                                H_SYNC: sync_time,
                                H_TARGET: "\uc5e3\uc9c0 \uc804\uccb4",
                                H_MACHINE: machine,
                                H_BLADE: "AT 날물(후면)",
                                H_USAGE_M: round(float(usage_m) * back_total / total, 3),
                                H_USAGE_C: "",
                                H_BASE_DATE: base_date,
                            }
                        )
                    continue
            entries.append(
                {
                    H_SYNC: sync_time,
                    H_TARGET: "\uc5e3\uc9c0 \uc804\uccb4",
                    H_MACHINE: machine,
                    H_BLADE: "AT 날물(후면)",
                    H_USAGE_M: round(float(usage_m), 3),
                    H_USAGE_C: "",
                    H_BASE_DATE: base_date,
                }
            )

    if not entries:
        return pd.DataFrame()
    history_df = pd.DataFrame(entries)
    grouped = (
        history_df.groupby([H_SYNC, H_TARGET, H_MACHINE, H_BLADE], as_index=False)
        .agg(
            {
                H_USAGE_M: lambda values: round(sum(float(v) for v in values if str(v).strip() not in {"", "nan"}), 3)
                if any(str(v).strip() not in {"", "nan"} for v in values)
                else "",
                H_USAGE_C: lambda values: int(round(sum(float(v) for v in values if str(v).strip() not in {"", "nan"})))
                if any(str(v).strip() not in {"", "nan"} for v in values)
                else "",
                H_BASE_DATE: lambda values: min([str(v).strip() for v in values if str(v).strip()], default=""),
            }
        )
    )
    return grouped


def normalize_sync_history_dataframe(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame(
            columns=[
                H_SYNC,
                H_TARGET,
                H_MACHINE,
                H_BLADE,
                H_USAGE_M,
                H_USAGE_C,
                H_BASE_DATE,
            ]
        )

    normalized = history_df.copy()
    expected_columns = [
        H_SYNC,
        H_TARGET,
        H_MACHINE,
        H_BLADE,
        H_USAGE_M,
        H_USAGE_C,
        H_BASE_DATE,
    ]
    normalized = normalized.rename(
        columns={
            "諛섏쁺?쒓컖": H_SYNC,
            "???": H_TARGET,
            "?ㅻ퉬": H_MACHINE,
            "?좊Ъ紐?": H_BLADE,
            "諛섏쁺 ?ъ슜??m)": H_USAGE_M,
            "諛섏쁺 ?ъ슜????": H_USAGE_C,
            "?쒖옉??": H_BASE_DATE,
        }
    )
    normalized = normalized.loc[:, ~normalized.columns.duplicated()]
    for column in expected_columns:
        if column not in normalized.columns:
            normalized[column] = ""

    normalized = normalized[expected_columns]
    normalized[H_SYNC] = normalized[H_SYNC].fillna("").astype(str).str.strip()
    normalized[H_TARGET] = normalized[H_TARGET].fillna("").astype(str).str.strip()
    normalized[H_MACHINE] = normalized[H_MACHINE].fillna("").astype(str).str.strip()
    normalized[H_BLADE] = normalized[H_BLADE].fillna("").astype(str).str.strip()
    normalized[H_BASE_DATE] = normalized[H_BASE_DATE].fillna("").astype(str).str.strip().apply(normalize_history_date_value)
    normalized[H_USAGE_M] = pd.to_numeric(normalized[H_USAGE_M], errors="coerce")
    normalized[H_USAGE_C] = pd.to_numeric(normalized[H_USAGE_C], errors="coerce")
    normalized["_sort_time"] = pd.to_datetime(normalized[H_SYNC], errors="coerce")
    normalized = normalized.sort_values(by=["_sort_time", H_SYNC], ascending=[True, True], na_position="last")

    dedupe_keys = [H_SYNC, H_TARGET, H_MACHINE, H_BLADE, H_USAGE_M, H_USAGE_C, H_BASE_DATE]
    normalized = normalized.drop_duplicates(subset=dedupe_keys, keep="first")
    normalized = normalized.drop(columns=["_sort_time"], errors="ignore")
    normalized[H_USAGE_M] = normalized[H_USAGE_M].apply(
        lambda value: "" if pd.isna(value) else round(float(value), 3)
    )
    normalized[H_USAGE_C] = normalized[H_USAGE_C].apply(
        lambda value: "" if pd.isna(value) else int(round(float(value)))
    )
    return normalized.reset_index(drop=True)


def write_sync_history_worksheet(spreadsheet, sheet_config: dict[str, str], history_df: pd.DataFrame) -> None:
    if history_df.empty:
        return
    worksheet_title = sheet_config.get("history_worksheet_name", "DASHBOARD_SYNC_HISTORY")
    try:
        worksheet = spreadsheet.worksheet(worksheet_title)
        existing_records = worksheet.get_all_records()
        existing_df = pd.DataFrame(existing_records)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_title,
            rows=str(max(len(history_df) + 100, 1000)),
            cols=str(max(len(history_df.columns) + 5, 20)),
        )
        existing_df = pd.DataFrame()

    combined_df = pd.concat([existing_df, history_df], ignore_index=True, sort=False)
    combined_df = normalize_sync_history_dataframe(combined_df)
    write_dataframe_to_worksheet(worksheet, combined_df)


def upload_to_google_sheet(result_df: pd.DataFrame, sheet_config: dict[str, str], erp_path: Path, logger: logging.Logger) -> None:
    if gspread is None:
        raise RuntimeError("gspread가 설치되어 있지 않아 Google Sheets 업로드를 진행할 수 없습니다.")

    for proxy_key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        os.environ.pop(proxy_key, None)

    credentials_path = Path(sheet_config["credentials_path"])
    logger.debug("upload_start spreadsheet=%s worksheet=%s credentials=%s", sheet_config["spreadsheet_name"], sheet_config["worksheet_name"], credentials_path)
    client = gspread.service_account(filename=str(credentials_path))
    spreadsheet = client.open(sheet_config["spreadsheet_name"])
    worksheet_mode = sheet_config.get("worksheet_mode", "new_sheet_per_upload")
    dataset_type = detect_dataset_type(result_df)

    if worksheet_mode == "overwrite":
        worksheet = get_or_create_worksheet(
            spreadsheet,
            sheet_config["worksheet_name"],
            len(result_df),
            len(result_df.columns),
        )
    else:
        worksheet = create_unique_worksheet(
            spreadsheet,
            build_worksheet_title(erp_path, f"{dataset_type}_{erp_path.stem}"),
            len(result_df),
            len(result_df.columns),
        )

    write_dataframe_to_worksheet(worksheet, result_df)

    latest_info = write_latest_upload_info(spreadsheet, worksheet, erp_path, dataset_type)
    write_latest_upload_info_worksheet(spreadsheet, sheet_config, latest_info)
    write_sync_history_worksheet(spreadsheet, sheet_config, build_sync_history_entries(result_df, dataset_type, erp_path.stem))
    logger.info("Google Sheets 업로드 완료: %s / %s / %s", dataset_type, sheet_config["spreadsheet_name"], worksheet.title)


def main() -> int:
    args = parse_args()
    logger = setup_logging(args.debug)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        erp_path = Path(args.erp) if args.erp else detect_latest_erp_file(logger)
        mapping_path = Path(args.mapping)
        output_dir = Path(args.output_dir)
        logger.info("사용 ERP 파일: %s", erp_path)
        logger.debug("사용 기준표 파일: %s", mapping_path)

        if not erp_path.exists():
            raise FileNotFoundError(f"ERP 파일을 찾지 못했습니다: {erp_path}")
        if not mapping_path.exists():
            raise FileNotFoundError(f"기준표 파일을 찾지 못했습니다: {mapping_path}")

        erp_df = read_excel_file(erp_path, logger)
        mapping_df = read_excel_file(mapping_path, logger)
        result_df = merge_erp_with_mapping(erp_df, mapping_df, logger)
        excel_output, csv_output = write_outputs(result_df, output_dir, erp_path, logger)

        print(f"총 행 수: {len(result_df)}")
        print(f"엑셀 결과: {excel_output}")
        print(f"CSV 결과: {csv_output}")
        print(f"디버그 로그: {DEBUG_LOG_PATH}")

        if args.sheet_config:
            sheet_config = load_sheet_target(Path(args.sheet_config))
            upload_to_google_sheet(result_df, sheet_config, erp_path, logger)

        return 0
    except Exception as exc:
        logger.exception("pipeline_failed")
        print(f"오류: {exc}", file=sys.stderr)
        print(f"디버그 로그 확인: {DEBUG_LOG_PATH}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
