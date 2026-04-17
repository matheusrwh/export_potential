
import pandas as pd
from pathlib import Path

TOLERANCE = 1e-10


def _normalize_to_fraction(series: pd.Series) -> pd.Series:
    """Normalize share scale to 0-1 when source is in 0-100."""
    max_val = series.max(skipna=True)
    if pd.isna(max_val):
        return series
    return series / 100.0 if max_val > 1.0 else series


def validate_sc_shares(project_root: Path) -> None:
    references = project_root / "references"
    processed = project_root / "data" / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    # Databricks source: all UFs, then filter SC.
    df_databricks = pd.read_csv(references / "share-ufs.csv")
    df_databricks_sc = (
        df_databricks.loc[df_databricks["sg_uf"] == "SC"].copy()
    )
    df_databricks_sc = df_databricks_sc.loc[df_databricks_sc["nr_ano"] < 2025].copy()

    # SC legacy source: wide format by year -> long format.
    df_shares_sc = pd.read_excel(references / "share_sc.xlsx")
    df_shares_sc = df_shares_sc.melt(
        id_vars=["sh6"],
        var_name="nr_ano",
        value_name="share_sc",
    )

    # Harmonize key types.
    df_databricks_sc["cd_sh6"] = pd.to_numeric(df_databricks_sc["cd_sh6"], errors="coerce")
    df_databricks_sc["nr_ano"] = pd.to_numeric(df_databricks_sc["nr_ano"], errors="coerce")
    df_databricks_sc["pct_participacao"] = pd.to_numeric(
        df_databricks_sc["pct_participacao"], errors="coerce"
    )

    df_shares_sc["sh6"] = pd.to_numeric(df_shares_sc["sh6"], errors="coerce")
    df_shares_sc["nr_ano"] = pd.to_numeric(df_shares_sc["nr_ano"], errors="coerce")
    df_shares_sc["share_sc"] = pd.to_numeric(df_shares_sc["share_sc"], errors="coerce")

    df_databricks_sc = df_databricks_sc.dropna(subset=["cd_sh6", "nr_ano", "pct_participacao"])
    df_shares_sc = df_shares_sc.dropna(subset=["sh6", "nr_ano", "share_sc"])

    df_databricks_sc[["cd_sh6", "nr_ano"]] = df_databricks_sc[["cd_sh6", "nr_ano"]].astype(int)
    df_shares_sc[["sh6", "nr_ano"]] = df_shares_sc[["sh6", "nr_ano"]].astype(int)

    df_all = df_databricks_sc.merge(
        df_shares_sc,
        left_on=["cd_sh6", "nr_ano"],
        right_on=["sh6", "nr_ano"],
        how="left",
        indicator=True,
    )

    # Compare on common 0-1 scale.
    df_all["pct_participacao_norm"] = _normalize_to_fraction(df_all["pct_participacao"])
    df_all["diff_abs"] = (df_all["pct_participacao_norm"] - df_all["share_sc"]).abs()

    missing_in_share_sc = df_all[df_all["share_sc"].isna()].copy()
    compared = df_all[df_all["share_sc"].notna()].copy()
    divergentes = compared[compared["diff_abs"] > TOLERANCE].copy()

    print("==== Validacao share SC ====")
    print(f"Linhas Databricks SC: {len(df_databricks_sc):,}")
    print(f"Linhas comparaveis (match): {len(compared):,}")
    print(f"Linhas sem match no share_sc.xlsx: {len(missing_in_share_sc):,}")
    print(f"Divergencias acima da tolerancia ({TOLERANCE}): {len(divergentes):,}")

    if len(compared) > 0:
        print(f"Diferenca absoluta maxima: {compared['diff_abs'].max():.12g}")
        print(f"Diferenca absoluta media: {compared['diff_abs'].mean():.12g}")

    if not divergentes.empty:
        cols = [
            "nr_ano",
            "cd_sh6",
            "sg_uf",
            "pct_participacao",
            "pct_participacao_norm",
            "share_sc",
            "diff_abs",
        ]
        print("Top 20 divergencias:")
        print(divergentes[cols].sort_values("diff_abs", ascending=False).head(20))

    divergentes.to_csv(processed / "share_sc_divergencias.csv", index=False)
    missing_in_share_sc.to_csv(processed / "share_sc_sem_match.csv", index=False)

    # Keep existing output expected by other routines.
    df_final_shares = df_databricks[["cd_sh6", "nr_ano", "sg_uf", "pct_participacao"]].copy()
    df_final_shares.to_csv(processed / "final_shares.csv", index=False)


if __name__ == "__main__":
    validate_sc_shares(Path(__file__).resolve().parents[1])
