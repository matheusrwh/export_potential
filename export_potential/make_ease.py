'''
################################################################
SCRIPT E CÁLCULOS VALIDADOS - MATHEUS SOUZA DA ROSA - 07/10/2025
################################################################
'''
#%% 

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
csv_files = list(data_raw.glob('BACI_*.csv'))
df_list = [pl.read_csv(f) for f in csv_files]
df_all = pl.concat(df_list)

df_all = df_all.rename({'t': 'year', 'i': 'exporter', 'j': 'importer',
                          'k': 'sh6', 'v': 'value', 'q': 'quantity'})


#%% 

######## Mapping countries and products ########
df_countries = pl.read_csv(references / 'countries.csv')
df_products = pl.read_csv(references / 'products.csv')

#%%

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

#%% 
######## Filtering for Brazil and estimating UF shares in bilateral exports ########
df_all_bra = df_all.filter(pl.col('exporter') == 'BRA')

df_all_bra.head()

#%%
# Load UF shares and join to bilateral flows
df_shares_ufs = pl.read_csv(references / 'share-ufs.csv', 
                            null_values=['null'],
                            schema_overrides={'cd_sh6': pl.Int64},).with_columns([
    pl.col('cd_sh6').cast(pl.Utf8).alias('sh6'),
    pl.col('nr_ano').cast(pl.Int64).alias('year'),
    pl.col('sg_uf').alias('sg_uf'),
    (pl.col('pct_participacao') / 100.0).alias('share_uf')
]).select(['cd_sh6', 'nr_ano', 'sg_uf', 'share_uf'])

df_shares_ufs.head()

#%% 
df_all_bra = df_all_bra.join(
    df_shares_ufs,
    left_on=['sh6', 'year'],
    right_on=['cd_sh6', 'nr_ano'],
    how='left'
)

df_all_bra.head()

#%% 

df_all_bra = df_all_bra.with_columns([
    (pl.col('value') * pl.col('share_uf')).alias('value_uf')
])

df_all_bra.head()

#%% 


# Calculating weighted average of exports per UF over the last 8 years
pesos = [i / 7 for i in range(8)]

recent_years = sorted(df_all_bra['year'].unique(), reverse=True)[:8]
df_recent = df_all_bra.filter(pl.col('year').is_in(recent_years))

weighted_exports = (
    df_recent
    .with_columns([
        pl.when(pl.col('year') == recent_years[0]).then(pesos[7])
         .when(pl.col('year') == recent_years[1]).then(pesos[6])
         .when(pl.col('year') == recent_years[2]).then(pesos[5])
         .when(pl.col('year') == recent_years[3]).then(pesos[4])
         .when(pl.col('year') == recent_years[4]).then(pesos[3])
         .when(pl.col('year') == recent_years[5]).then(pesos[2])
         .when(pl.col('year') == recent_years[6]).then(pesos[1])
         .when(pl.col('year') == recent_years[7]).then(pesos[0])
         .otherwise(0)
         .alias('peso')
    ])
    .with_columns([
        (pl.col('value_uf') * pl.col('peso')).alias('weighted_value_uf')
    ])
    .group_by(['exporter', 'importer', 'sh6', 'product_description', 'sg_uf'])
    .agg([
        (pl.sum('weighted_value_uf') / pl.sum('peso')).alias('weighted_exports_uf')
    ])
)

df_recent.head()    
#%% 

df_all_bra = df_all_bra.join(
    weighted_exports.select(['exporter', 'importer', 'sh6', 'sg_uf', 'weighted_exports_uf']),
    on=['exporter', 'importer', 'sh6', 'sg_uf'],
    how='left'
)

df_all_bra.head()
#%% 

df_all_bra = df_all_bra.filter(pl.col('year') == 2024)

df_bilateral = df_all_bra.group_by(['exporter', 'importer', 'sg_uf']).agg([
    pl.sum('weighted_exports_uf').alias('bilateral_exports_uf')
])

df_bilateral.head()
#%% 

df_bilateral_sh6 = df_all_bra.group_by(['exporter', 'importer', 'sh6', 'sg_uf']).agg([
    pl.sum('weighted_exports_uf').alias('bilateral_exports_uf_sh6')
])

df_bilateral.head()
df_bilateral_sh6.head()

df_bilateral_sh6.write_parquet(data_interim / 'bilateral_exports_sh6.parquet')

df_bilateral_sh6.head()

#%% 

#################### ------- SUPPLY AND DEMAND ------- ####################
df_demand = pl.read_parquet(data_processed / 'demand_potential.parquet')
df_supply_ufs = pl.read_parquet(data_processed / 'supply_potential_ufs.parquet')

df_demand.head()
df_supply_ufs.head()

#%% 

# Join demand with UF shares (creates a row per sg_uf/sh6)
df_demand = df_demand.join(
    df_supply_ufs.select(['sh6', 'sg_uf', 'uf_share_proj_2030']),
    on='sh6',
    how='left'
)

df_ease = df_demand.select(['importer', 'sh6', 'product_description',
                            'weighted_imports', 'sg_uf', 'uf_share_proj_2030'])

df_ease = df_ease.with_columns([
    (pl.col('weighted_imports') * pl.col('uf_share_proj_2030')).alias('value_uf')
])

df_ease = df_ease.group_by(['importer', 'sg_uf']).agg([
    pl.sum('value_uf').alias('sum_value_uf')
])

df_ease = df_ease.sort('sum_value_uf', descending=True)

df_ease.head()



#################### ------- EASE OF TRADE ------- ####################
df_bilateral.head()
df_ease.head()

df_ease = df_ease.join(
    df_bilateral,
    left_on=['importer', 'sg_uf'],
    right_on=['importer', 'sg_uf'],
    how='left'
)

df_ease.head()

df_ease = df_ease.with_columns([
    (pl.col('bilateral_exports_uf') / pl.col('sum_value_uf')).alias('ease_of_trade')
])

df_ease = df_ease.select([
    'importer',
    'sg_uf',
    'ease_of_trade'
])


df_ease.write_parquet(data_processed / 'ease_of_trade_ufs.parquet')