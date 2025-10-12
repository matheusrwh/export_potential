import polars as pl
from pathlib import Path
import streamlit as st


def get_paths() -> dict:
    """Resolve key project paths regardless of current working dir."""
    current = Path(__file__).resolve()
    # project root is the parent of 'app' directory containing this file
    project_root = current.parent.parent
    return {
        "root": project_root,
        "app": project_root / "app",
        "data_raw": project_root / "data" / "raw",
        "data_processed": project_root / "data" / "processed",
        "data_interim": project_root / "data" / "interim",
        "references": project_root / "references",
    }


@st.cache_data(show_spinner=False)
def load_epi_data() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load EPI datasets used across pages, with caching."""
    paths = get_paths()
    df_epi_sh6 = pl.read_parquet(paths["app"] / "data" / "epi_scores_sh6.parquet")
    df_epi_countries = pl.read_parquet(paths["app"] / "data" / "epi_scores_countries.parquet")
    # Round as per original logic
    df_epi_sh6 = df_epi_sh6.with_columns(pl.col("epi_score_normalized").round(2))
    df_epi_countries = df_epi_countries.with_columns(pl.col("epi_score_normalized").round(2))
    return df_epi_sh6, df_epi_countries


@st.cache_data(show_spinner=False)
def load_munic_vp():
    """Load munic/vp options and return lists for filters."""
    paths = get_paths()
    df_munic_vp = pl.read_excel(paths["references"] / "munic_vp.xlsx")
    vp = sorted(df_munic_vp["vp"].unique().to_list())
    munic = sorted(df_munic_vp["munic"].unique().to_list())
    return vp, munic, df_munic_vp


# Backwards-compatible helper
def load_data():
    return load_epi_data()