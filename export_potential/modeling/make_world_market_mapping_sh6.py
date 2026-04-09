"""
Gera tabela de mapeamento do mercado mundial por SH6 para consumo em plataforma.

Saida principal por linha (sh6, pais importador), com colunas de exibicao:
- Posicao
- Pais
- Montante US$
- Market Share (%)
- CAGR 5 anos (%)
- Share Brasil (%)
- Share SC (%)
"""

from pathlib import Path

import polars as pl


######## Setting directories ########
project_root = Path(__file__).resolve().parents[2]
data_raw = project_root / "data" / "raw"
data_app = project_root / "data" / "app"
data_processed = project_root / "data" / "processed"
references = project_root / "references"


def format_money_ptbr(value: float) -> str:
    if value is None:
        return "0,0"
    if value >= 1e9:
        s = f"{value / 1e9:,.1f} bi"
    elif value >= 1e6:
        s = f"{value / 1e6:,.1f} mi"
    elif value >= 1e3:
        s = f"{value / 1e3:,.1f} mil"
    else:
        s = f"{value:,.1f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def format_pct_ptbr(value: float, decimals: int = 1) -> str:
    if value is None:
        return "0,0"
    return f"{value:.{decimals}f}".replace(".", ",")


######## Loading BACI ########
csv_files = sorted(data_raw.glob("baci_*.csv"))
df_list = [pl.read_csv(f) for f in csv_files]
df_raw = pl.concat(df_list)

df_raw = df_raw.rename(
    {"t": "year", "i": "exporter", "j": "importer", "k": "sh6", "v": "value"}
)

df_countries = pl.read_csv(references / "countries.csv")
df_countries_br = pl.read_csv(references / "countries_br.csv", encoding="latin1", separator=";")
df_products_br = pl.read_excel(references / "products_br_mdic.xlsx")
df_share_sc = pl.read_excel(references / "share_sc.xlsx")


######## Harmonizing keys ########
df_share_sc = df_share_sc.with_columns(pl.col("sh6").cast(pl.Utf8).str.zfill(6).alias("sh6"))
df_products_br = df_products_br.select(
    [
        pl.col("CO_SH6").cast(pl.Utf8).str.zfill(6).alias("sh6"),
        pl.col("NO_SH6_POR").alias("product_description_br"),
    ]
)

df = (
    df_raw.join(
        df_countries.select(
            [pl.col("country_code").alias("importer"), pl.col("country_iso3").alias("importer_iso3")]
        ),
        on="importer",
        how="left",
    )
    .join(
        df_countries.select(
            [pl.col("country_code").alias("exporter"), pl.col("country_iso3").alias("exporter_iso3")]
        ),
        on="exporter",
        how="left",
    )
    .select(
        [
            "year",
            "exporter_iso3",
            "importer_iso3",
            pl.col("sh6").cast(pl.Utf8).str.zfill(6).alias("sh6"),
            (pl.col("value") * 1000).alias("value_usd"),
        ]
    )
    .rename({"exporter_iso3": "exporter", "importer_iso3": "importer"})
    .drop_nulls(["exporter", "importer", "sh6"])
)


######## Defining horizon ########
base_year = int(df.select(pl.col("year").max()).item())
start_year = base_year - 5

share_year_col = str(base_year)
if share_year_col not in df_share_sc.columns:
    available_year_cols = [c for c in df_share_sc.columns if c != "sh6"]
    share_year_col = sorted(available_year_cols)[-1]


######## Core metrics per (importer, sh6) ########
df_base = df.filter(pl.col("year") == base_year)

df_import_market = df_base.group_by(["importer", "sh6"]).agg(
    pl.sum("value_usd").alias("import_value_usd")
)

df_world_total = df_import_market.group_by("sh6").agg(
    pl.sum("import_value_usd").alias("world_import_value_usd")
)

df_import_market = df_import_market.join(df_world_total, on="sh6", how="left").with_columns(
    (
        pl.when(pl.col("world_import_value_usd") > 0)
        .then((pl.col("import_value_usd") / pl.col("world_import_value_usd")) * 100)
        .otherwise(0.0)
    ).alias("market_share_pct")
)


######## CAGR 5 anos (%) ########
df_start = (
    df.filter(pl.col("year") == start_year)
    .group_by(["importer", "sh6"])
    .agg(pl.sum("value_usd").alias("value_start"))
)

df_end = (
    df.filter(pl.col("year") == base_year)
    .group_by(["importer", "sh6"])
    .agg(pl.sum("value_usd").alias("value_end"))
)

df_cagr = (
    df_end.join(df_start, on=["importer", "sh6"], how="left")
    .with_columns(
        pl.when((pl.col("value_start") > 0) & (pl.col("value_end") >= 0))
        .then((((pl.col("value_end") / pl.col("value_start")) ** (1 / 5)) - 1) * 100)
        .otherwise(None)
        .alias("cagr_5y_pct")
    )
    .select(["importer", "sh6", "cagr_5y_pct"])
)


######## Share Brasil e Share SC ########
df_brazil = (
    df_base.filter(pl.col("exporter") == "BRA")
    .group_by(["importer", "sh6"])
    .agg(pl.sum("value_usd").alias("brazil_exports_usd"))
)

df_sc_share_map = df_share_sc.select(
    [pl.col("sh6"), pl.col(share_year_col).cast(pl.Float64).fill_null(0.0).alias("share_sc_in_brazil")]
)

df_brazil_sc = (
    df_brazil.join(df_sc_share_map, on="sh6", how="left")
    .with_columns(
        [
            pl.col("share_sc_in_brazil").fill_null(0.0),
            (pl.col("brazil_exports_usd") * pl.col("share_sc_in_brazil")).alias("sc_exports_usd"),
        ]
    )
    .select(["importer", "sh6", "brazil_exports_usd", "sc_exports_usd"])
)


######## Joining all metrics ########
df_country_names = df_countries_br.select(
    [pl.col("CO_PAIS_ISOA3").alias("importer"), pl.col("NO_PAIS").alias("País")]
)

df_out = (
    df_import_market.join(df_cagr, on=["importer", "sh6"], how="left")
    .join(df_brazil_sc, on=["importer", "sh6"], how="left")
    .join(df_country_names, on="importer", how="left")
    .join(df_products_br, on="sh6", how="left")
    .with_columns(
        [
            pl.col("brazil_exports_usd").fill_null(0.0),
            pl.col("sc_exports_usd").fill_null(0.0),
            pl.col("cagr_5y_pct").fill_null(0.0),
            pl.coalesce([pl.col("País"), pl.col("importer")]).alias("País"),
            pl.when(pl.col("import_value_usd") > 0)
            .then((pl.col("brazil_exports_usd") / pl.col("import_value_usd")) * 100)
            .otherwise(0.0)
            .alias("share_brasil_pct"),
            pl.when(pl.col("import_value_usd") > 0)
            .then((pl.col("sc_exports_usd") / pl.col("import_value_usd")) * 100)
            .otherwise(0.0)
            .alias("share_sc_pct"),
        ]
    )
    .with_columns(
        pl.col("import_value_usd")
        .rank(method="ordinal", descending=True)
        .over("sh6")
        .cast(pl.Int64)
        .alias("Posição")
    )
    .sort(["sh6", "Posição"])
)


######## Final display columns ########
df_display = (
    df_out.with_columns(
        [
            pl.col("import_value_usd").map_elements(format_money_ptbr).alias("Montante US$"),
            pl.col("market_share_pct").map_elements(lambda x: format_pct_ptbr(x, 1)).alias("Market Share (%)"),
            pl.col("cagr_5y_pct").map_elements(lambda x: format_pct_ptbr(x, 1)).alias("CAGR 5 anos (%)"),
            pl.col("share_brasil_pct").fill_null(0.0).map_elements(lambda x: format_pct_ptbr(x, 1)).alias("Share Brasil (%)"),
            pl.col("share_sc_pct").fill_null(0.0).map_elements(lambda x: format_pct_ptbr(x, 1)).alias("Share SC (%)"),
        ]
    )
    .select(
        [
            "sh6",
            "product_description_br",
            "Posição",
            "País",
            "Montante US$",
            "Market Share (%)",
            "CAGR 5 anos (%)",
            "Share Brasil (%)",
            "Share SC (%)",
        ]
    )
)


######## Saving ########
data_app.mkdir(parents=True, exist_ok=True)
df_display.write_parquet(data_app / "global_market_sh6.parquet")
df_display.write_parquet(data_processed / "global_market_sh6.parquet")
