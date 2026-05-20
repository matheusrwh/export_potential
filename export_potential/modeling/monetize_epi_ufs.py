#%%
"""
Monetiza o EPI por UF sem depender de horizonte de mercado mundial.

Ideia central:
1) Usa o score bruto EPI (share de oferta * demanda projetada * ease) como peso de atratividade.
2) Aloca a oferta projetada por UF por SH6 entre mercados conforme esses pesos.
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

BRAZIL_CODE = "BR"
# Codigos que representam o Brasil na coluna importer (auto-referencia a remover).
BRAZIL_IMPORTER_CODES = ["BR", "BRA"]


def finite_or_zero(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).is_finite())
        .then(pl.col(column))
        .otherwise(0.0)
        .fill_null(0.0)
        .fill_nan(0.0)
        .alias(column)
    )


def add_kmeans_category(
    df: pl.DataFrame, value_col: str, output_col: str, group_col: str = "sg_uf"
) -> pl.DataFrame:
    labels = ["Baixo", "Médio", "Médio-Alto", "Alto", "Muito Alto"]

    def apply_kmeans(group_df: pl.DataFrame) -> pl.DataFrame:
        values = (
            group_df.get_column(value_col)
            .fill_null(0.0)
            .fill_nan(0.0)
            .to_numpy()
            .reshape(-1, 1)
        )

        if len(values) == 0:
            return group_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(output_col))

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
        return group_df.with_columns(pl.Series(output_col, categories))

    # Apply KMeans inside each UF to avoid cross-UF dominance in categories.
    return df.group_by(group_col).map_groups(apply_kmeans)


def add_export_categories(df: pl.DataFrame, group_col: str = "sg_uf") -> pl.DataFrame:
    df = add_kmeans_category(df, "potential_value", "potential_category", group_col)
    df = add_kmeans_category(
        df,
        "unrealized_potential_value",
        "unrealized_potential_category",
        group_col,
    )
    return df


def add_brazil_aggregate_flag(df: pl.DataFrame) -> pl.DataFrame:
    """True na UF agregada sg_uf='BR', False nas 27 UFs reais."""
    return df.with_columns((pl.col("sg_uf") == BRAZIL_CODE).alias("is_brazil_aggregate"))


def build_brazil_uf_aggregate(df: pl.DataFrame, exports_col: str) -> pl.DataFrame:
    """Soma as 27 UFs em uma UF artificial sg_uf='BR' (Brasil agregado)."""
    # Agrega sobre sg_uf preservando as demais dimensoes presentes no df.
    group_keys = [c for c in ("exporter", "importer", "sh6") if c in df.columns]
    df_br = (
        df.group_by(group_keys)
        .agg(
            [
                pl.col("product_description_br").first().alias("product_description_br"),
                pl.col("sc_comp").first().alias("sc_comp"),
                pl.sum("potential_value").alias("potential_value"),
                pl.sum("unrealized_potential_value").alias("unrealized_potential_value"),
                pl.sum(exports_col).alias(exports_col),
            ]
        )
        .with_columns(
            [
                pl.lit(BRAZIL_CODE).alias("sg_uf"),
                pl.when(pl.col("potential_value") > 0)
                .then(pl.col(exports_col) / pl.col("potential_value"))
                .otherwise(0.0)
                .alias("potential_utilization_ratio"),
            ]
        )
    )

    return df_br.select(df.columns)


def write_partitioned_by_uf(
    df: pl.DataFrame,
    partition_dir: Path,
    relative_base: str,
) -> None:
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Clean previous partition files to avoid stale UFs from older runs.
    for old_file in partition_dir.glob("sg_uf=*.parquet"):
        old_file.unlink()

    partitions = df.partition_by("sg_uf", as_dict=True, include_key=False, maintain_order=True)

    index_rows = []

    for uf_key, df_part in partitions.items():
        sg_uf = uf_key[0] if isinstance(uf_key, tuple) else uf_key

        if sg_uf is None:
            continue

        file_name = f"sg_uf={sg_uf}.parquet"
        file_path = partition_dir / file_name
        df_part.write_parquet(file_path, compression="snappy")

        row = {
            "sg_uf": sg_uf,
            "file_name": file_name,
            "relative_path": f"{relative_base}/{file_name}",
            "rows": df_part.height,
            "size_bytes": file_path.stat().st_size,
        }

        if "sh6" in df_part.columns:
            row["sh6_count"] = df_part.select(pl.col("sh6").n_unique()).item()

        index_rows.append(row)

    df_index = pl.DataFrame(index_rows).sort("sg_uf")
    df_index.write_parquet(partition_dir / "index.parquet", compression="snappy")
    df_index.write_json(partition_dir / "index.json")


######## Loading the data ########
df_ease = pl.read_parquet(data_processed / "ease_of_trade_ufs.parquet")
df_demand = pl.read_parquet(data_processed / "demand_potential.parquet")
df_supply = pl.read_parquet(data_processed / "supply_potential_ufs.parquet")
df_bilateral = pl.read_parquet(data_interim / "bilateral_exports_sh6.parquet")
df_sh6_comp = pl.read_excel(references / "sh6_mundo_comp.xlsx")
df_products_br = pl.read_excel(references / "products_br_mdic.xlsx")

#%%

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
        df_ease.select(["importer", "sg_uf", "ease_of_trade"]),
        on=["importer", "sg_uf"],
        how="left",
    )
    .join(
        df_bilateral,
        on=["exporter", "importer", "sh6", "sg_uf"],
        how="left",
    )
)


df.head()   

#%%
df = df.with_columns(
    [
        finite_or_zero("projected_import_value"),
        finite_or_zero("ease_of_trade"),
        finite_or_zero("uf_share_proj_2030"),
        finite_or_zero("proj_exports_uf_2030"),
        finite_or_zero("bilateral_exports_uf_sh6"),
    ]
)


######## Monetary potential using UF supply horizon ########
df = df.with_columns(
    [
        (
            pl.col("uf_share_proj_2030")
            * pl.col("projected_import_value")
            * pl.col("ease_of_trade")
        )
        .fill_null(0.0)
        .fill_nan(0.0)
        .alias("epi_score"),
    ]
)

sum_weight = pl.sum("epi_score").over(["sh6", "sg_uf"])

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
        (pl.col("proj_exports_uf_2030") * pl.col("epi_weight_share")).alias(
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
            [pl.col("potential_value") - pl.col("bilateral_exports_uf_sh6"), pl.lit(0.0)]
        ).alias("unrealized_potential_value"),
        pl.min_horizontal([pl.col("potential_value"), pl.col("bilateral_exports_uf_sh6")]).alias(
            "realized_potential_value"
        ),
        pl.max_horizontal(
            [pl.col("bilateral_exports_uf_sh6") - pl.col("potential_value"), pl.lit(0.0)]
        ).alias("overtrade_value"),
    ]
)

df = df.with_columns(
    [
        pl.when(pl.col("potential_value") > 0)
        .then(pl.col("bilateral_exports_uf_sh6") / pl.col("potential_value"))
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


######## Removendo o Brasil como importador (sem auto-referencia) ########
# Filtro aplicado apos todos os calculos: preserva os valores ja validados
# dos demais mercados; apenas descarta as linhas Brasil -> Brasil.
df = df.filter(~pl.col("importer").is_in(BRAZIL_IMPORTER_CODES))


######## Saving detailed output ########
final_columns = [
    "exporter",
    "importer",
    "sg_uf",
    "sh6",
    "product_description_br",
    "sc_comp",
    "potential_value",
    "bilateral_exports_uf_sh6",
    "unrealized_potential_value",
    "potential_utilization_ratio",
    "potential_category",
    "unrealized_potential_category",
    "is_brazil_aggregate",
]

df_out_base = df.select(
    [
        "exporter",
        "importer",
        "sg_uf",
        "sh6",
        "product_description_br",
        "sc_comp",
        "potential_value",
        "unrealized_potential_value",
        "bilateral_exports_uf_sh6",
        "potential_utilization_ratio",
    ]
)

df_out = pl.concat(
    [df_out_base, build_brazil_uf_aggregate(df_out_base, "bilateral_exports_uf_sh6")],
    how="vertical",
)
df_out = add_export_categories(df_out)
df_out = add_brazil_aggregate_flag(df_out)

df_out.write_parquet(data_processed / "epi_monetary_ufs.parquet")

partition_dir = data_processed / "epi_monetary_ufs_by_uf"
write_partitioned_by_uf(df_out, partition_dir, "data/processed/epi_monetary_ufs_by_uf")

######## Saving aggregated outputs ########
df_country_base = (
    df.group_by(["importer", "sg_uf"])
    .agg(
        [
            pl.col("exporter").first().alias("exporter"),
            pl.sum("potential_value").alias("potential_value"),
            pl.sum("unrealized_potential_value").alias("unrealized_potential_value"),
            pl.sum("bilateral_exports_uf_sh6").alias("bilateral_exports_uf"),
        ]
    )
    .with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("sh6"),
            pl.lit(None).cast(pl.Utf8).alias("product_description_br"),
            pl.lit(None).cast(pl.Utf8).alias("sc_comp"),
            pl.when(pl.col("potential_value") > 0)
            .then(pl.col("bilateral_exports_uf") / pl.col("potential_value"))
            .otherwise(0.0)
            .alias("potential_utilization_ratio"),
        ]
    )
    .select(
        [
            "exporter",
            "importer",
            "sg_uf",
            "sh6",
            "product_description_br",
            "sc_comp",
            "potential_value",
            "bilateral_exports_uf",
            "unrealized_potential_value",
            "potential_utilization_ratio",
        ]
    )
)

df_country = pl.concat(
    [df_country_base, build_brazil_uf_aggregate(df_country_base, "bilateral_exports_uf")],
    how="vertical",
)
df_country = add_export_categories(df_country)
df_country = add_brazil_aggregate_flag(df_country)
df_country = df_country.sort("unrealized_potential_value", descending=True)

df_country.write_parquet(data_processed / "epi_monetary_ufs_country.parquet")

partition_dir = data_processed / "epi_monetary_ufs_country_by_uf"
write_partitioned_by_uf(
    df_country,
    partition_dir,
    "data/processed/epi_monetary_ufs_country_by_uf",
)

df_product_base = (
    df.group_by(["sh6", "sg_uf"])
    .agg(
        [
            pl.col("exporter").first().alias("exporter"),
            pl.col("product_description_br").first().alias("product_description_br"),
            pl.col("sc_comp").first().alias("sc_comp"),
            pl.sum("potential_value").alias("potential_value"),
            pl.sum("unrealized_potential_value").alias("unrealized_potential_value"),
            pl.sum("bilateral_exports_uf_sh6").alias("bilateral_exports_uf_sh6"),
        ]
    )
    .with_columns(
        [
            pl.when(pl.col("potential_value") > 0)
            .then(pl.col("bilateral_exports_uf_sh6") / pl.col("potential_value"))
            .otherwise(0.0)
            .alias("potential_utilization_ratio"),
        ]
    )
    .select(
        [
            "exporter",
            "sg_uf",
            "sh6",
            "product_description_br",
            "sc_comp",
            "potential_value",
            "bilateral_exports_uf_sh6",
            "unrealized_potential_value",
            "potential_utilization_ratio",
        ]
    )
)

df_product = pl.concat(
    [df_product_base, build_brazil_uf_aggregate(df_product_base, "bilateral_exports_uf_sh6")],
    how="vertical",
)
df_product = add_export_categories(df_product)
df_product = add_brazil_aggregate_flag(df_product)
df_product = df_product.sort("unrealized_potential_value", descending=True)

df_product.write_parquet(data_processed / "epi_monetary_ufs_sh6.parquet")

partition_dir = data_processed / "epi_monetary_ufs_sh6_by_uf"
write_partitioned_by_uf(
    df_product,
    partition_dir,
    "data/processed/epi_monetary_ufs_sh6_by_uf",
)
