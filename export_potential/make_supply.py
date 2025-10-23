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

######## Loading the data ########
df_all = pl.read_parquet(data_interim / 'comex_exps_weighted.parquet')

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

# Joining the SC shares with the main dataframe
df_all_bra = df_all_bra.join(
    df_shares_sc,
    left_on=['sh6', 'year'],
    right_on=['sh6', 'ano'],
    how='left'
)

# Calculating the value of exports of SC
df_all_bra = df_all_bra.with_columns([
    (pl.col('value') * pl.col('share_sc')).alias('valor_sc')
])

df_all_bra.head()

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
        (pl.col('valor_sc') * pl.col('peso')).alias('weighted_value_sc')
    ])
    .group_by(['exporter', 'sh6', 'product_description'])
    .agg([
        (pl.sum('weighted_value_sc') / pl.sum('peso')).alias('weighted_exports_sc')
    ])
)

df_all_bra = df_all_bra.join(
    weighted_exports.select(['exporter', 'sh6', 'weighted_exports_sc']),
    on=['exporter', 'sh6'],
    how='left'
)

df_all_bra = df_all_bra.filter(pl.col('year') == 2023)

df_all_bra.head()
df_all_bra.shape







########## Projecting exports for SC ##########
acc_growth_gdp = 1.195

df_all_bra = df_all_bra.with_columns([
    (pl.col('weighted_exports_sc') * acc_growth_gdp).alias('proj_exports_sc_2027')
])

########## Projecting exports for all countries ##########
df_gdp_growth = pl.read_excel(references / 'gdp_growth.xlsx')

df_gdp_growth.head()

# Setting the mean GDP growth rate for countries without specific data
for col in df_gdp_growth.columns:
    if df_gdp_growth[col].null_count() > 0 and df_gdp_growth[col].dtype in [pl.Float64, pl.Int64]:
        mean_value = df_gdp_growth[col].mean()
        df_gdp_growth = df_gdp_growth.with_columns(
            pl.col(col).fill_null(mean_value).alias(col)
        )

# Accumulated growth from 2022 to 2027
growth_cols = [str(year) for year in range(2022, 2028)]

df_gdp_growth = df_gdp_growth.with_columns([
    pl.fold(
        acc=pl.lit(1.0),
        function=lambda acc, x: acc * (1 + x),
        exprs=[pl.col(col) for col in growth_cols]
    ).alias('gdp_index_2027')
])

df_gdp_growth = df_gdp_growth.select(['ISO', 'gdp_index_2027'])

df_all.head()
df_gdp_growth.head()

# Calculating projected exports for all countries
df_all = df_all.filter(pl.col('year') == 2023)

df_all = df_all.join(
    df_gdp_growth,
    left_on='exporter',
    right_on='ISO',
    how='left'
)

df_all = df_all.with_columns([
    (pl.col('weighted_exports') * pl.col('gdp_index_2027')).alias('proj_exports_2027')
])




########### Calculating the share of SC in overall exports projections ##########
# Merging SC projections with overall projections
df_all_bra.head()
df_all.head()

df_all = df_all.join(
    df_all_bra.select(['exporter', 'sh6', 'proj_exports_sc_2027']),
    on=['exporter', 'sh6'],
    how='left'
)

df_all = df_all.with_columns([
    (
        pl.col('proj_exports_sc_2027') / pl.sum('proj_exports_2027').over('sh6')
    ).alias('sc_share_proj_2027')
])

df_supply_sc = df_all.filter(pl.col('sc_share_proj_2027').is_not_null())

df_supply_sc = df_supply_sc.select([
    'exporter',
    'sh6',
    'product_description',
    'weighted_exports',
    'proj_exports_sc_2027',
    'sc_share_proj_2027'
])

df_supply_sc = df_supply_sc.sort('sc_share_proj_2027', descending=True)

df_supply_sc.head()

df_supply_sc.write_parquet(data_processed / 'supply_potential_sc.parquet')



