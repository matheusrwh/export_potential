import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
df_ease = pl.read_parquet(data_processed / 'ease_of_trade.parquet')
df_demand = pl.read_parquet(data_processed / 'demand_potential.parquet')
df_supply = pl.read_parquet(data_processed / 'supply_potential_sc.parquet')
df_bilateral_sh6 = pl.read_parquet(data_interim / 'bilateral_exports_sh6.parquet')
df_countries = pl.read_csv(references / 'countries_br.csv', encoding='latin1', separator=';')

df_ease.head()
df_demand.head()
df_supply.head()
df_bilateral_sh6.head()
df_countries.head()

df_epi = df_supply.join(
    df_demand.select(['importer', 'sh6', 'projected_import_value']),
    on=['sh6'],
    how='left').join(
        df_ease.select(['importer', 'ease_of_trade']),
        on=['importer'],
        how='left').join(
            df_bilateral_sh6,
            on=['exporter', 'importer', 'sh6'],
            how='left'        
        )

df_epi.head()

df_epi = df_epi.with_columns([
    (pl.col('sc_share_proj_2027') * pl.col('projected_import_value') * pl.col('ease_of_trade')).alias('epi_score')
])

df_epi = df_epi.sort('epi_score', descending=True)

df_epi = df_epi.filter(pl.col('epi_score').is_not_nan())

df_epi.head()

#### Normalizing the EPI scores ####
df_epi = df_epi.with_columns([
    pl.col('epi_score').fill_null(0).alias('epi_score'),
    pl.col('proj_exports_sc_2027').fill_null(0).alias('proj_exports_sc_2027'),
    pl.col('bilateral_exports_sc_sh6').fill_null(0).alias('bilateral_exports_sc_sh6'),
])

df_alpha = (
    df_epi
    .group_by('sh6')
    .agg([
        pl.col('epi_score').sum().alias('sum_epi_k'),
        pl.col('proj_exports_sc_2027').first().alias('proj_exports_sc_2027'),  # usa o valor já existente
    ])
    .with_columns([
        pl.when(pl.col('sum_epi_k') > 0)
          .then(pl.col('proj_exports_sc_2027') / pl.col('sum_epi_k'))
          .otherwise(0.0)
          .alias('alpha_k')
    ])
    .select(['sh6','alpha_k'])
)

df_epi = (
    df_epi
    .join(df_alpha, on='sh6', how='left')
    .with_columns((pl.col('epi_score') * pl.col('alpha_k')).alias('epi_score_normalized'))
)


#### Calculating the potential value of SC exports ###
df_epi = df_epi.with_columns([
    (pl.col('epi_score_normalized') - pl.col('bilateral_exports_sc_sh6')).alias('unrealized_potential')
])

df_epi = df_epi.with_columns([
    pl.when(pl.col('unrealized_potential') < 0)
      .then(0)
      .otherwise(pl.col('unrealized_potential'))
      .alias('unrealized_potential')
])

df_epi = df_epi.select(['exporter', 'importer', 'sh6', 'product_description',
                        'bilateral_exports_sc_sh6', 'proj_exports_sc_2027', 'projected_import_value', 'epi_score', 'epi_score_normalized',
                        'unrealized_potential'])

df_epi.head()

df_epi = df_epi.join(
    df_countries.select([
        pl.col('CO_PAIS_ISOA3').alias('importer'),
        pl.col('NO_PAIS').alias('importer_name')]),
    on='importer',
    how='left'
)

df_epi.write_parquet(data_processed / 'epi_scores.parquet')