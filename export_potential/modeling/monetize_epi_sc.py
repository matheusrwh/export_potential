"""
Monetiza o EPI para Santa Catarina sem depender de horizonte de mercado mundial.

Ideia central:
1) Usa o score bruto EPI (share de oferta * demanda projetada * ease) como peso de atratividade.
2) Aloca a oferta projetada de SC por SH6 entre mercados conforme esses pesos.
3) Limita o valor potencial por mercado ao teto de viabilidade (o proprio score bruto).
4) Deriva potencial nao realizado comparando potencial estimado com exportacao bilateral atual.
"""

from pathlib import Path

import polars as pl


######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_processed = project_root / "data" / "processed"
data_interim = project_root / "data" / "interim"


def finite_or_zero(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).is_finite())
        .then(pl.col(column))
        .otherwise(0.0)
        .fill_null(0.0)
        .fill_nan(0.0)
        .alias(column)
    )


######## Loading the data ########
df_ease = pl.read_parquet(data_processed / "ease_of_trade.parquet")
df_demand = pl.read_parquet(data_processed / "demand_potential.parquet")
df_supply = pl.read_parquet(data_processed / "supply_potential_sc.parquet")
df_bilateral = pl.read_parquet(data_interim / "bilateral_exports_sh6.parquet")

df_supply.head()

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
        finite_or_zero("sc_share_proj_2029"),
        finite_or_zero("proj_exports_sc_2029"),
        finite_or_zero("bilateral_exports_sc_sh6"),
    ]
)


######## Monetary potential using SC supply horizon ########
df = df.with_columns(
    [
        (
            pl.col("sc_share_proj_2029")
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
        (pl.col("proj_exports_sc_2029") * pl.col("epi_weight_share")).alias(
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


######## Saving detailed output ########
final_columns = [
    "exporter",
    "importer",
    "sh6",
    "product_description",
    "potential_value",
    "unrealized_potential_value",
    "potential_utilization_ratio",
]

df_out = df.select(
    final_columns
)

df_out.write_json(data_processed / "epi_monetary_sc.json")


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
            pl.lit(None).cast(pl.Utf8).alias("product_description"),
            pl.when(pl.col("potential_value") > 0)
            .then(pl.col("bilateral_exports_sc") / pl.col("potential_value"))
            .otherwise(0.0)
            .alias("potential_utilization_ratio"),
        ]
    )
    .select(final_columns)
    .sort("unrealized_potential_value", descending=True)
)

df_country.write_json(data_processed / "epi_monetary_sc_country.json")

df_product = (
    df.group_by(["sh6"])
    .agg(
        [
            pl.col("exporter").first().alias("exporter"),
            pl.col("product_description").first().alias("product_description"),
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
    .select(final_columns)
    .sort("unrealized_potential_value", descending=True)
)

df_product.write_json(data_processed / "epi_monetary_sc_sh6.json")
