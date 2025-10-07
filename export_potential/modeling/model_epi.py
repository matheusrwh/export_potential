import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[1]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
df_ease = pl.read_parquet(data_processed / 'ease_of_trade.parquet')
df_demand = pl.read_parquet(data_processed / 'demand_potential.parquet')
df_supply = pl.read_parquet(data_processed / 'supply_potential_sc.parquet')
df_bilateral_sh6 = pl.read_parquet(data_interim / 'bilateral_exports_sh6.parquet')

df_ease.head()
df_demand.head()
df_supply.head()
df_bilateral_sh6.head()

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

total_projected_import = df_epi['projected_import_value'].sum()
total_epi_score = df_epi['epi_score'].sum()
scaling_factor = total_projected_import / total_epi_score

#print(df_epi['epi_score'].sum())

df_epi = df_epi.with_columns([
    (pl.col('epi_score') * scaling_factor).alias('epi_score_normalized')
])

df_epi = df_epi.with_columns([
    pl.col('bilateral_exports_sc_sh6').fill_null(0).alias('bilateral_exports_sc_sh6')
])

df_epi = df_epi.with_columns([
    (pl.col('epi_score_normalized') - pl.col('bilateral_exports_sc_sh6')).alias('potential_value_sc'),
    (pl.col('epi_score_normalized') - pl.col('projected_import_value')).alias('potential_value_total')
])

df_epi = df_epi.with_columns([
    pl.when(pl.col('potential_value_sc') < 0)
      .then(0)
      .otherwise(pl.col('potential_value_sc'))
      .alias('potential_value_sc')
])

df_epi = df_epi.with_columns([
    pl.when(pl.col('potential_value_total') < 0)
      .then(0)
      .otherwise(pl.col('potential_value_total'))
      .alias('potential_value_total')
])

df_epi = df_epi.select(['exporter', 'importer', 'sh6', 'product_description',
                        'bilateral_exports_sc_sh6', 'projected_import_value', 'epi_score_normalized',
                        'potential_value_sc', 'potential_value_total'])

df_epi.head()

df_epi.write_parquet(data_processed / 'epi_scores.parquet')