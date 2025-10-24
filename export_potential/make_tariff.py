import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
df_tariffs = pl.read_parquet(data_raw / 'tariffs.parquet').drop('Product Name')

df_tariffs.head()
df_tariffs.shape

######## Treating the data ########
df_tariffs = df_tariffs.rename({
    'Reporter': 'importer_code',
    'Reporter Name': 'importer_name',
    'Partner': 'exporter_code',
    'Partner Name': 'exporter_name',
    'Product': 'sh6',
    'Tariff_Final': 'tariff',
    'Tariff_Year': 'year'
})

df_countries = pl.read_csv(references / 'countries.csv')

df_tariffs.head()
df_countries.head()

df_tariffs = df_tariffs.join(
    df_countries.select(['country_code', 'country_name', 'country_iso3']),
    left_on='importer_code',
    right_on='country_code',
    how='left'
).join(
    df_countries.select(['country_code', 'country_name', 'country_iso3']),
    left_on='exporter_code',
    right_on='country_code',
    how='left',
    suffix='_exporter'
)

df_tariffs = df_tariffs.rename({
    'country_iso3': 'importer_iso3',
    'country_iso3_exporter': 'exporter_iso3'
})

df_tariffs = df_tariffs.select([
    'year', 'importer_name', 'importer_iso3', 'exporter_name',
    'exporter_iso3', 'sh6', 'tariff'
])

df_tariffs.head()
df_tariffs.shape

unique_sh6 = df_tariffs['sh6'].unique().to_list()
print(len(unique_sh6))

######## Trade elasticities ########
df_elasticities = pl.read_csv(data_raw / 'trade_elasticities.csv')

df_elasticities.head()

df_elasticities = df_elasticities.select(['HS6', 'sigma']).rename({'HS6': 'sh6'})

df_elasticities = df_elasticities.with_columns(
    pl.col("sh6").cast(str).str.zfill(6)
)

df_tariffs = df_tariffs.join(
    df_elasticities,
    on='sh6',
    how='left'
)

df_tariffs.head()
df_tariffs.shape

df_tariffs.write_parquet(data_raw / 'tariffs_processed.parquet')
