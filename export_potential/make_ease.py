'''
################################################################
SCRIPT E CÁLCULOS VALIDADOS - MATHEUS SOUZA DA ROSA - 07/10/2025
################################################################
'''

import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[1]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

#################### ------- BILATERAL EXPORTS ------- ####################
######## Loading the data ########
csv_files = list(data_raw.glob('*.csv'))
df_list = [pl.read_csv(f) for f in csv_files]
df_all = pl.concat(df_list)

df_all = df_all.rename({'t': 'year', 'i': 'exporter', 'j': 'importer',
                          'k': 'sh6', 'v': 'value', 'q': 'quantity'})

######## Mapping countries and products ########
df_countries = pl.read_csv(references / 'countries.csv')
df_products = pl.read_csv(references / 'products.csv')

df_all = (
    df_all
    .join(
        df_countries,
        left_on='exporter',
        right_on='country_code',
        how='left'
    )
    .join(
        df_countries,
        left_on='importer',
        right_on='country_code',
        how='left',
        suffix='_importer'
    )
    .join(
        df_products,
        left_on='sh6',
        right_on='code',
        how='left'
    )
)

df_all = (
    df_all
    .select([
        'year',
        'country_iso3',
        'country_iso3_importer',
        'sh6',
        'description',
        'value',
        'quantity'
    ])
    .rename({
        'country_iso3': 'exporter',
        'country_iso3_importer': 'importer',
        'description': 'product_description'
    })
    .with_columns([
        (pl.col('value') * 1000).alias('value'),
        pl.col('sh6').cast(pl.Int64)
    ])
    .filter(pl.col('exporter') == 'BRA')
)

df_all.head()

######## Filtering for Brazil and estimating SC share ########
df_all_bra = df_all.filter(pl.col('exporter') == 'BRA')

df_all_bra.head()

df_shares_sc = pl.read_excel(references / 'share_sc.xlsx')

df_all_bra.head()
df_shares_sc.head()

df_shares_sc = df_shares_sc.with_columns([
    pl.col('sh6').cast(pl.Int64)
])

df_shares_sc = df_shares_sc.unpivot(
    index=['sh6'],
    on=[col for col in df_shares_sc.columns if col != 'sh6'],
    variable_name='ano',
    value_name='share_sc'
)

df_shares_sc = df_shares_sc.with_columns([
    pl.col('ano').cast(pl.Int64)
])

df_shares_sc.head()

df_all_bra.head()
df_all_bra.shape

df_all_bra = df_all_bra.join(
    df_shares_sc,
    left_on=['sh6', 'year'],
    right_on=['sh6', 'ano'],
    how='left'
)

df_all_bra = df_all_bra.with_columns([
    (pl.col('value') * pl.col('share_sc')).alias('value_sc')
])

# Calculating weighted average of exports of SC over the last 5 years
pesos = [0.2, 0.4, 0.6, 0.8, 1.0]

recent_years = sorted(df_all_bra['year'].unique(), reverse=True)[:5]
df_recent = df_all_bra.filter(pl.col('year').is_in(recent_years))

weighted_exports = (
    df_recent
    .with_columns([
        pl.when(pl.col('year') == recent_years[0]).then(pesos[4])
         .when(pl.col('year') == recent_years[1]).then(pesos[3])
         .when(pl.col('year') == recent_years[2]).then(pesos[2])
         .when(pl.col('year') == recent_years[3]).then(pesos[1])
         .when(pl.col('year') == recent_years[4]).then(pesos[0])
         .otherwise(0)
         .alias('peso')
    ])
    .with_columns([
        (pl.col('value_sc') * pl.col('peso')).alias('weighted_value_sc')
    ])
    .group_by(['exporter', 'importer', 'sh6', 'product_description'])
    .agg([
        (pl.sum('weighted_value_sc') / pl.sum('peso')).alias('weighted_exports_sc')
    ])
)

df_all_bra = df_all_bra.join(
    weighted_exports.select(['exporter', 'importer', 'sh6', 'weighted_exports_sc']),
    on=['exporter', 'importer', 'sh6'],
    how='left'
)

df_all_bra = df_all_bra.filter(pl.col('year') == 2023)

df_bilateral = df_all_bra.group_by(['exporter', 'importer']).agg([
    pl.sum('weighted_exports_sc').alias('bilateral_exports_sc')
])

df_bilateral_sh6 = df_all_bra.group_by(['exporter', 'importer', 'sh6']).agg([
    pl.sum('weighted_exports_sc').alias('bilateral_exports_sc_sh6')
])

df_bilateral.head()
df_bilateral_sh6.head()

df_bilateral_sh6.write_parquet(data_interim / 'bilateral_exports_sh6.parquet')

#################### ------- SUPPLY AND DEMAND ------- ####################
df_demand = pl.read_parquet(data_processed / 'demand_potential.parquet')
df_supply_sc = pl.read_parquet(data_processed / 'supply_potential_sc.parquet')

df_demand.head()
df_supply_sc.head()

df_demand = df_demand.join(
    df_supply_sc.select(['sh6', 'sc_share_proj_2027']),
    on='sh6',
    how='left'
)

df_ease = df_demand.select(['importer', 'sh6', 'product_description',
                            'weighted_imports', 'sc_share_proj_2027'])

df_ease = df_ease.with_columns([
    (pl.col('weighted_imports') * pl.col('sc_share_proj_2027')).alias('value_sc')
])

df_ease = df_ease.group_by(['importer']).agg([
    pl.sum('value_sc').alias('sum_value_sc')
])

df_ease = df_ease.sort('sum_value_sc', descending=True)

df_ease.head()



#################### ------- EASE OF TRADE ------- ####################
df_bilateral.head()
df_ease.head()

df_ease = df_ease.join(
    df_bilateral,
    left_on='importer',
    right_on='importer',
    how='left'
)

df_ease.head()

df_ease = df_ease.with_columns([
    (pl.col('bilateral_exports_sc') / pl.col('sum_value_sc')).alias('ease_of_trade')
])

df_ease = df_ease.select([
    'exporter',
    'importer',
    'ease_of_trade'
])

df_ease.write_parquet(data_processed / 'ease_of_trade.parquet')