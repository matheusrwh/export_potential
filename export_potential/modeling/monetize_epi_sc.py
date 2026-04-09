"""
Monetiza o EPI para Santa Catarina sem depender de horizonte de mercado mundial.

Ideia central:
1) Usa o score bruto EPI (share de oferta * demanda projetada * ease) como peso de atratividade.
2) Aloca a oferta projetada de SC por SH6 entre mercados conforme esses pesos.
3) Limita o valor potencial por mercado ao teto de viabilidade (o proprio score bruto).
4) Deriva potencial nao realizado comparando potencial estimado com exportacao bilateral atual.
"""

from pathlib import Path

import numpy as np
import polars as pl
from sklearn.cluster import KMeans


######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_processed = project_root / "data" / "processed"
data_interim = project_root / "data" / "interim"
references = project_root / "references"


def finite_or_zero(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).is_finite())
        .then(pl.col(column))
        .otherwise(0.0)
        .fill_null(0.0)
        .fill_nan(0.0)
        .alias(column)
    )


def add_kmeans_category(df: pl.DataFrame, value_col: str, output_col: str) -> pl.DataFrame:
    labels = ["Baixo", "Médio", "Médio-Alto", "Alto", "Muito Alto"]

    values = (
        df.get_column(value_col)
        .fill_null(0.0)
        .fill_nan(0.0)
        .to_numpy()
        .reshape(-1, 1)
    )

    if len(values) == 0:
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias(output_col))

    n_clusters = 5 if len(values) >= 5 else len(values)
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=20)
    cluster_ids = kmeans.fit_predict(values)

    centers = kmeans.cluster_centers_.flatten()
    cluster_order = np.argsort(centers)
    cluster_to_rank = {int(cluster): rank for rank, cluster in enumerate(cluster_order)}
    ranks = np.array([cluster_to_rank[int(c)] for c in cluster_ids])

    if n_clusters < 5 and n_clusters > 1:
        scaled_ranks = np.rint(ranks * (4 / (n_clusters - 1))).astype(int)
    elif n_clusters == 1:
        scaled_ranks = np.zeros(len(ranks), dtype=int)
    else:
        scaled_ranks = ranks

    scaled_ranks = np.clip(scaled_ranks, 0, 4)
    categories = [labels[int(rank)] for rank in scaled_ranks]
    return df.with_columns(pl.Series(output_col, categories))


def add_export_categories(df: pl.DataFrame) -> pl.DataFrame:
    df = add_kmeans_category(df, "potential_value", "potential_category")
    df = add_kmeans_category(
        df, "unrealized_potential_value", "unrealized_potential_category"
    )
    return df


######## Loading the data ########
df_ease = pl.read_parquet(data_processed / "ease_of_trade.parquet")
df_demand = pl.read_parquet(data_processed / "demand_potential.parquet")
df_supply = pl.read_parquet(data_processed / "supply_potential_sc.parquet")
df_bilateral = pl.read_parquet(data_interim / "bilateral_exports_sh6.parquet")
df_sh6_comp = pl.read_excel(references / "sh6_mundo_comp.xlsx")
df_products_br = pl.read_excel(references / "products_br_mdic.xlsx")

df_supply.head()

df_sh6_map = (
    df_sh6_comp.select(
        [
            pl.col("sh6").cast(pl.Utf8).str.zfill(6).alias("sh6"),
            pl.col("sc_comp").cast(pl.Utf8).alias("sc_comp"),
        ]
    )
    .group_by("sh6")
    .agg(pl.col("sc_comp").first().alias("sc_comp"))
    .join(
        df_products_br.select(
            [
                pl.col("CO_SH6").cast(pl.Utf8).str.zfill(6).alias("sh6"),
                pl.col("NO_SH6_POR").alias("product_description_br"),
            ]
        ),
        on="sh6",
        how="left",
    )
)

######## Building product-market panel ########
df = (
    df_supply.join(
        df_demand.select(["importer", "sh6", "product_description", "projected_import_value"]),
        on=["sh6"],
        how="left",
    )
    .join(
        df_ease.select(["importer", "ease_of_trade"]),
        on=["importer"],
        how="left",
    )
    .join(
        df_bilateral,
        on=["exporter", "importer", "sh6"],
        how="left",
    )
)

df = df.with_columns(
    [
        finite_or_zero("projected_import_value"),
        finite_or_zero("ease_of_trade"),
        finite_or_zero("sc_share_proj_2030"),
        finite_or_zero("proj_exports_sc_2030"),
        finite_or_zero("bilateral_exports_sc_sh6"),
    ]
)


######## Monetary potential using SC supply horizon ########
df = df.with_columns(
    [
        (
            pl.col("sc_share_proj_2030")
            * pl.col("projected_import_value")
            * pl.col("ease_of_trade")
        )
        .fill_null(0.0)
        .fill_nan(0.0)
        .alias("epi_score"),
    ]
)

sum_weight = pl.sum("epi_score").over("sh6")

df = df.with_columns(
    [
        pl.when(sum_weight > 0)
        .then(pl.col("epi_score") / sum_weight)
        .otherwise(0.0)
        .alias("epi_weight_share"),
    ]
)

df = df.with_columns(
    [
        (pl.col("proj_exports_sc_2030") * pl.col("epi_weight_share")).alias(
            "allocated_supply_value"
        ),
        pl.col("epi_score").alias("market_feasible_value"),
    ]
)

df = df.with_columns(
    [
        pl.min_horizontal([pl.col("allocated_supply_value"), pl.col("market_feasible_value")]).alias(
            "potential_value"
        )
    ]
)

df = df.with_columns(
    [
        pl.max_horizontal(
            [pl.col("potential_value") - pl.col("bilateral_exports_sc_sh6"), pl.lit(0.0)]
        ).alias("unrealized_potential_value"),
        pl.min_horizontal([pl.col("potential_value"), pl.col("bilateral_exports_sc_sh6")]).alias(
            "realized_potential_value"
        ),
        pl.max_horizontal(
            [pl.col("bilateral_exports_sc_sh6") - pl.col("potential_value"), pl.lit(0.0)]
        ).alias("overtrade_value"),
    ]
)

df = df.with_columns(
    [
        pl.when(pl.col("potential_value") > 0)
        .then(pl.col("bilateral_exports_sc_sh6") / pl.col("potential_value"))
        .otherwise(0.0)
        .fill_nan(0.0)
        .alias("potential_utilization_ratio"),
    ]
)

df = (
    df.with_columns(pl.col("sh6").cast(pl.Utf8).str.zfill(6).alias("sh6"))
    .join(df_sh6_map, on="sh6", how="left")
    .with_columns(
        pl.coalesce([pl.col("product_description_br"), pl.col("product_description")]).alias(
            "product_description_br"
        )
    )
)


######## Saving detailed output ########
final_columns = [
    "exporter",
    "importer",
    "sh6",
    "product_description_br",
    "sc_comp",
    "potential_value",
    "bilateral_exports_sc_sh6",
    "unrealized_potential_value",
    "potential_utilization_ratio",
    "potential_category",
    "unrealized_potential_category",
]

df_out = df.select(
    [
        "exporter",
        "importer",
        "sh6",
        "product_description_br",
        "sc_comp",
        "potential_value",
        "unrealized_potential_value",
        "bilateral_exports_sc_sh6",
        "potential_utilization_ratio",
    ]
)

df_out = add_export_categories(df_out)

df_out.write_parquet(data_processed / "epi_monetary_sc.parquet")


######## Saving aggregated outputs ########
df_country = (
    df.group_by(["importer"])
    .agg(
        [
            pl.col("exporter").first().alias("exporter"),
            pl.sum("potential_value").alias("potential_value"),
            pl.sum("unrealized_potential_value").alias("unrealized_potential_value"),
            pl.sum("bilateral_exports_sc_sh6").alias("bilateral_exports_sc"),
        ]
    )
    .with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("sh6"),
            pl.lit(None).cast(pl.Utf8).alias("product_description_br"),
            pl.lit(None).cast(pl.Utf8).alias("sc_comp"),
            pl.when(pl.col("potential_value") > 0)
            .then(pl.col("bilateral_exports_sc") / pl.col("potential_value"))
            .otherwise(0.0)
            .alias("potential_utilization_ratio"),
        ]
    )
    .select(
        [
            "exporter",
            "importer",
            "sh6",
            "product_description_br",
            "sc_comp",
            "potential_value",
            "bilateral_exports_sc",
            "unrealized_potential_value",
            "potential_utilization_ratio",
        ]
    )
    .sort("unrealized_potential_value", descending=True)
)

df_country = add_export_categories(df_country)

df_country.write_parquet(data_processed / "epi_monetary_sc_country.parquet")

df_product = (
    df.group_by(["sh6"])
    .agg(
        [
            pl.col("exporter").first().alias("exporter"),
            pl.col("product_description_br").first().alias("product_description_br"),
            pl.col("sc_comp").first().alias("sc_comp"),
            pl.sum("potential_value").alias("potential_value"),
            pl.sum("unrealized_potential_value").alias("unrealized_potential_value"),
            pl.sum("bilateral_exports_sc_sh6").alias("bilateral_exports_sc_sh6"),
        ]
    )
    .with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("importer"),
            pl.when(pl.col("potential_value") > 0)
            .then(pl.col("bilateral_exports_sc_sh6") / pl.col("potential_value"))
            .otherwise(0.0)
            .alias("potential_utilization_ratio"),
        ]
    )
    .select(
        [
            "exporter",
            "importer",
            "sh6",
            "product_description_br",
            "sc_comp",
            "potential_value",
            "bilateral_exports_sc_sh6",
            "unrealized_potential_value",
            "potential_utilization_ratio",
        ]
    )
    .sort("unrealized_potential_value", descending=True)
)

df_product = add_export_categories(df_product)

df_product.write_parquet(data_processed / "epi_monetary_sc_sh6.parquet")
