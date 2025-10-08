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
df_all = pl.read_parquet(data_interim / 'comex_imps_weighted.parquet')

df_all.head()






######## Loading the population data ########
df_pop = pl.read_excel(references / 'pop_growth.xlsx')

df_pop.head()

df_pop = df_pop.with_columns([
    pl.col(df_pop.columns[2:]).cast(pl.Int64)
])

# Calculating the CAGR for population between 2015 and 2021
df_pop = df_pop.with_columns([
    (
        ((pl.col('2021') / pl.col('2015')) ** (1 / (2021 - 2015)) - 1)
    ).alias('CAGR_2015_2021')
])

df_pop = df_pop.with_columns([
    pl.when(pl.col(col) == 0).then(None).otherwise(pl.col(col)).alias(col)
    for col in df_pop.columns[2:]
])

# Substituting missing values for years after 2015 using the CAGR
for year in df_pop.columns[2:-1]:  # Ignora 'CAGR_2015_2021'
    df_pop = df_pop.with_columns([
        pl.when(pl.col(year).is_null())
        .then((pl.col('2015') * ((1 + pl.col('CAGR_2015_2021')) ** (int(year) - 2015))).cast(pl.Int64))
        .otherwise(pl.col(year))
        .alias(year)
    ])

# Calculating the annual growth rates from 2022 to 2027
for year in range(2022, 2028):
    prev_year = str(year - 1)
    curr_year = str(year)
    df_pop = df_pop.with_columns([
        ((pl.col(curr_year) / pl.col(prev_year)) - 1).alias(f'growth_{curr_year}')
    ])

# Calculating the cumulative growth index from 2022 to 2027
growth_cols = [f'growth_{year}' for year in range(2022, 2028)]
df_pop = df_pop.with_columns([
    (
        pl.fold(
            acc=pl.lit(1.0),
            function=lambda acc, x: acc * (1 + x),
            exprs=[pl.col(col) for col in growth_cols]
        ).cast(pl.Float64)
    ).alias('pop_index_2027')
])

df_pop_growth = df_pop.select(['ISO', 'pop_index_2027'])

df_pop_growth.head()


########## Projecting GDP for all countries ##########
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

df_pop_growth.head()
df_gdp_growth.head()




########## Estimating GDP per capita growth ##########
df_growth = df_gdp_growth.join(
    df_pop_growth,
    on='ISO',
    how='inner'
)

df_growth.head()

df_growth = df_growth.with_columns([
    (pl.col('gdp_index_2027') / pl.col('pop_index_2027')).alias('gdp_pc_index_2027'),
])

############### Mean elasticity == 1.201 ################
df_growth = df_growth.with_columns([
    (pl.col('gdp_pc_index_2027') ** (1.201)).alias('gdp_pc_adj_index_2027')
])

######## Estimating demand growth ##########
df_growth = df_growth.with_columns([
    (pl.col('gdp_pc_adj_index_2027') * pl.col('pop_index_2027')).alias('demand_index_2027')
])

df_growth = df_growth.select(['ISO', 'demand_index_2027'])

df_growth.head()

######### Merging demand growth with trade data ########
df_demand = df_all.filter(pl.col('year') == 2023)

df_demand = df_demand.join(
    df_growth,
    left_on='importer',
    right_on='ISO',
    how='left'
)

df_demand = df_demand.with_columns([
    (
        pl.col('weighted_imports') * pl.col('demand_index_2027').over('sh6')
    ).alias('projected_import_value')
])

df_demand.head()

df_demand = df_demand.select([
    'importer',
    'sh6',
    'product_description',
    'weighted_imports',
    'demand_index_2027',
    'projected_import_value'
])

df_demand.write_parquet(data_processed / 'demand_potential.parquet')


