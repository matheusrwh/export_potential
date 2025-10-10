import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
df_tariffs = pl.read_parquet(data_raw / 'tariffs.parquet')

df_tariffs.head()

######## Treating the data ########
df_tariffs = df_tariffs.rename({
    'Reporter': 'importer',
    'Year': 'year',
    'Partner': 'exporter',
    'Product': 'sh6',
    'MFNRate': 'mfn_rate',
    'AppliedTariff': 'applied_tariff',
    'TotalTariffLines': 'total_tariff_lines',
    'IsTraded': 'is_traded'
})

df_tariffs = df_tariffs.with_columns([
    pl.col("sh6").str.slice(0, 6).alias("sh6_prefix"),
    pl.col("sh6").str.slice(9).alias("sh6_suffix")
])

df_tariffs = df_tariffs.drop("sh6")

df_tariffs = df_tariffs.rename({
    'sh6_prefix': 'sh6',
    'sh6_suffix': 'product'
})

df_countries = pl.read_csv(references / 'countries.csv')

df_tariffs.head()
df_countries.head()

df_tariffs = df_tariffs.join(
    df_countries.select(['country_name', 'country_iso3']),
    left_on='importer',
    right_on='country_name',
    how='left'
).join(
    df_countries.select(['country_name', 'country_iso3']),
    left_on='exporter',
    right_on='country_name',
    how='left',
    suffix='_exporter'
)

df_tariffs = df_tariffs.rename({
    'country_iso3': 'importer_iso3',
    'country_iso3_exporter': 'exporter_iso3'
})

df_tariffs = df_tariffs.select([
    'year', 'importer', 'importer_iso3', 'exporter', 'exporter_iso3',
    'sh6', 'product', 'mfn_rate', 'applied_tariff', 'total_tariff_lines', 'is_traded'
])

df_tariffs.head()

unique_sh6 = df_tariffs['sh6'].unique().to_list()
print(unique_sh6)

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