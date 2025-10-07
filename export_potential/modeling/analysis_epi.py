import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[1]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
df_epi = pl.read_parquet(data_processed / 'epi_scores.parquet')

df_epi.head()

######################### AGREGGATING BY PRODUCT #########################
df_epi_sh6 = df_epi.group_by(['sh6', 'product_description']).agg([
    pl.sum('bilateral_exports_sc_sh6').alias('bilateral_exports_sc_sh6'),
    pl.sum('projected_import_value').alias('projected_import_value'),
    pl.sum('epi_score_normalized').alias('epi_score_normalized'),
    pl.sum('potential_value_sc').alias('potential_value_sc'),
    pl.sum('potential_value_total').alias('potential_value_total')
])

df_epi_sh6 = df_epi_sh6.with_columns([
    (pl.col('bilateral_exports_sc_sh6') / pl.col('potential_value_sc')).alias('realized_potential_sc'),
    (pl.col('projected_import_value') / pl.col('potential_value_total')).alias('realized_potential_total')
])

df_epi_sh6 = df_epi_sh6.sort('bilateral_exports_sc_sh6', descending=True)

df_epi_sh6.head()

######################### AGREGGATING BY COUNTRY #########################
df_epi_country = df_epi.group_by(['importer']).agg([
    pl.sum('bilateral_exports_sc_sh6').alias('bilateral_exports_sc_sh6'),
    pl.sum('projected_import_value').alias('projected_import_value'),
    pl.sum('epi_score_normalized').alias('epi_score_normalized'),
    pl.sum('potential_value_sc').alias('potential_value_sc'),
    pl.sum('potential_value_total').alias('potential_value_total')
])

df_epi_country = df_epi_country.with_columns([
    (pl.col('bilateral_exports_sc_sh6') / pl.col('potential_value_sc')).alias('realized_potential_sc'),
    (pl.col('projected_import_value') / pl.col('potential_value_total')).alias('realized_potential_total')
])

df_epi_country = df_epi_country.sort('bilateral_exports_sc_sh6', descending=True)

df_epi_country.head()