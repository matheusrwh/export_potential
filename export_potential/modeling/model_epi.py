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

# Normalização do epi_score para cada par (importer, sh6)
df_epi = df_epi.with_columns([
    pl.col('epi_score').fill_null(0).alias('epi_score')
])

grouped = df_epi.group_by(['importer', 'sh6']).agg([
    pl.col('epi_score').sum().alias('sum_epi_score'),
    pl.col('projected_import_value').sum().alias('sum_projected_import_value')
])

df_epi = df_epi.join(grouped, on=['importer', 'sh6'], how='left')

df_epi = df_epi.with_columns([
    (pl.col('epi_score') * pl.col('sum_projected_import_value') / pl.col('sum_epi_score')).alias('epi_score_normalized')
])

df_epi = df_epi.with_columns([
    pl.col('bilateral_exports_sc_sh6').fill_null(0).alias('bilateral_exports_sc_sh6')
])

df_epi = df_epi.with_columns([
    (pl.col('epi_score_normalized') - pl.col('bilateral_exports_sc_sh6')).alias('potential_value_sc')
])

df_epi = df_epi.with_columns([
    pl.when(pl.col('potential_value_sc') < 0)
      .then(0)
      .otherwise(pl.col('potential_value_sc'))
      .alias('potential_value_sc')
])

df_epi = df_epi.select(['exporter', 'importer', 'sh6', 'product_description',
                        'bilateral_exports_sc_sh6', 'projected_import_value', 'epi_score_normalized',
                        'potential_value_sc'])

df_epi.head()

df_epi_doors = df_epi.filter(
    pl.col('sh6') == 441820
)

df_epi_doors.write_excel(data_processed / 'epi_scores_doors.xlsx')

df_epi.write_parquet(data_processed / 'epi_scores.parquet')