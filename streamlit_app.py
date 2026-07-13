from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


WORK_DIR = Path(__file__).resolve().parent
SYNC_HISTORY_SEED_PATH = WORK_DIR / "sheet_sync_history_seed.json"
COMPLETION_HISTORY_SEED_PATH = WORK_DIR / "completion_history_seed.json"


def _safe_page_config() -> None:
    try:
        st.set_page_config(page_title="날물 교체관리 대시보드", layout="wide")
    except Exception:
        pass


def _load_json_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _show_recovery_dashboard(error: BaseException | None = None) -> None:
    _safe_page_config()
    st.title("날물 교체관리 대시보드")
    st.caption("FURSYS · 충주 공장 · 품질보증팀")
    if error is None:
        st.info("대시보드가 빠르게 열리도록 안정 모드로 표시 중입니다.")
    else:
        st.warning("전체 대시보드 실행 중 오류가 발생해서 안정 모드로 열었습니다.")
        with st.expander("오류 내용", expanded=False):
            st.exception(error)

    history = _load_json_rows(SYNC_HISTORY_SEED_PATH)
    completion = _load_json_rows(COMPLETION_HISTORY_SEED_PATH)
    metric_cols = st.columns(2)
    metric_cols[0].metric("데이터 반영 이력", f"{len(history):,}개")
    metric_cols[1].metric("교체완료 시점", f"{len(completion):,}개")

    tab_history, tab_completion = st.tabs(["데이터 반영 이력", "교체완료 시점"])
    with tab_history:
        _show_table(
            history,
            ["반영시각", "대상", "설비", "날물명", "데이터 기준일자", "반영 사용량(m)", "반영 사용량(회)"],
            "반영시각",
        )
    with tab_completion:
        _show_table(
            completion,
            ["교체완료시각", "설비", "날물명", "기준값", "교체 시점 사용량", "담당자", "비고"],
            "교체완료시각",
        )


def _show_table(rows: list[dict[str, Any]], columns: list[str], time_column: str) -> None:
    if not rows:
        st.info("표시할 데이터가 없습니다.")
        return

    df = pd.DataFrame(rows).rename(columns={"시작일": "데이터 기준일자"})
    df = df[[column for column in columns if column in df.columns]]
    filter_cols = st.columns(3)
    machine_options = ["전체", *sorted(value for value in df["설비"].dropna().astype(str).unique() if value.strip())]
    blade_options = ["전체", *sorted(value for value in df["날물명"].dropna().astype(str).unique() if value.strip())]
    selected_machine = filter_cols[0].selectbox("설비", machine_options, key=f"{time_column}_machine")
    selected_blade = filter_cols[1].selectbox("날물명", blade_options, key=f"{time_column}_blade")
    max_rows = filter_cols[2].selectbox("표시 개수", [200, 500, 1000, "전체"], index=1, key=f"{time_column}_limit")

    if selected_machine != "전체":
        df = df[df["설비"].astype(str) == selected_machine]
    if selected_blade != "전체":
        df = df[df["날물명"].astype(str) == selected_blade]
    if time_column in df.columns:
        df["_sort_time"] = pd.to_datetime(df[time_column], errors="coerce")
        df = df.sort_values("_sort_time", ascending=False).drop(columns=["_sort_time"], errors="ignore")
    if max_rows != "전체":
        df = df.head(int(max_rows))
    st.dataframe(df, use_container_width=True, hide_index=True)


def main() -> None:
    if st.query_params.get("full") != "1":
        _show_recovery_dashboard()
        return

    try:
        import dashboard_app

        dashboard_app.main()
    except BaseException as exc:
        _show_recovery_dashboard(exc)


if __name__ == "__main__":
    main()
