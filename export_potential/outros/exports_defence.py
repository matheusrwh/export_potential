'''
################################################################
SCRIPT E CÁLCULOS VALIDADOS - MATHEUS SOUZA DA ROSA - 07/10/2025
################################################################
'''

import polars as pl
from pathlib import Path
from numpy import log, power

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
df_all = pl.read_parquet(data_interim / 'comex_exps_weighted.parquet')

df_all = df_all.with_columns(
    pl.col('sh6').cast(pl.Utf8).str.zfill(6).alias('sh6')
)

df_all.head()
df_all.shape

######## Filterinf for defence products ########
df_defence_sh6 = pl.read_excel(references / 'ncm_bid.xlsx', sheet_name='Codigos_NCM')

df_defence_sh6 = df_defence_sh6.rename({
    'Codigo': 'codigo',
    'NCM': 'codigo_desc',
    'Plataforma': 'plataforma'
})

####### Treating SH6 codes ########
df_defence_sh6 = df_defence_sh6.with_columns(
    pl.col('codigo').cast(pl.Utf8).str.slice(0, 6).alias('sh6')
)

defence_sh6_list = df_defence_sh6['sh6'].unique().to_list()

#print(defence_sh6_list)

df_defence_sh6 = df_defence_sh6.with_columns(
    pl.col('sh6').str.zfill(2)
)

def filter_by_partial_sh6(df, sh6_list):
    mask = pl.zeros(len(df), dtype=pl.Boolean)
    for code in sh6_list:
        mask = mask | df['sh6'].str.starts_with(code)
    return df.filter(mask)

df_all = filter_by_partial_sh6(df_all, defence_sh6_list)

df_all.head()
df_all.shape

#######################################
# Weighting the exports over the last 5 years
#######################################
df_all = df_all.group_by([
    'year', 'exporter', 'sh6', 'product_description']).agg([
    pl.sum('value').alias('value'),
    pl.sum('quantity').alias('quantity')
])

pesos = [0.2, 0.4, 0.6, 0.8, 1.0]

recent_years = sorted(df_all['year'].unique(), reverse=True)[:5]
df_recent = df_all.filter(pl.col('year').is_in(recent_years))

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
        (pl.col('value') * pl.col('peso')).alias('weighted_value')
    ])
    .group_by(['exporter', 'sh6', 'product_description'])
    .agg([
        (pl.sum('weighted_value') / pl.sum('peso')).alias('weighted_exports')
    ])
)

df_all = df_all.join(
    weighted_exports.select(['exporter', 'sh6', 'weighted_exports']),
    on=['exporter', 'sh6'],
    how='left'
)

def calculate_cagr(start, end, periods):
    if start is None or end is None or start == 0 or periods == 0:
        return None
    return round(power(end / start, 1 / periods) - 1, 2)

cagr_years = [2019, 2023]
df_cagr = (
    df_all.filter(pl.col('year').is_in(cagr_years))
    .group_by(['exporter', 'sh6', 'year'])
    .agg([
        pl.sum('value').alias('total_value')
    ])
    .pivot(
        values='total_value',
        index=['exporter', 'sh6'],
        on='year'
    )
    .with_columns([
        pl.struct(['2019', '2023']).map_elements(
            lambda x: calculate_cagr(x['2019'], x['2023'], 2023-2019)
        ).alias('cagr_value_2019_2023')
    ])
)

df_all = df_all.join(
    df_cagr.select(['exporter', 'sh6', 'cagr_value_2019_2023']),
    on=['exporter', 'sh6'],
    how='left'
)

df_all.head()

df_countries = pl.read_csv(references / 'countries_br.csv', encoding='latin1', separator=';')

df_countries.head()

df_all = df_all.join(
    df_countries.select([
        pl.col('CO_PAIS_ISOA3').alias('exporter'),
        pl.col('NO_PAIS').alias('exporter_name')]),
    on='exporter',
    how='left'
)

df_all = df_all.with_columns(
    pl.when(pl.col('exporter') == 'S19')
    .then(pl.lit('Taiwan'))
    .otherwise(pl.col('exporter_name'))
    .alias('exporter_name')
)

df_all = df_all.with_columns([
    pl.col('sh6').cast(str).str.zfill(6).alias('sh6')
])

df_products = pl.read_excel(references / 'products_br_mdic.xlsx')
df_products.head()

df_all = df_all.join(
    df_products,
    left_on='sh6',
    right_on='CO_SH6',
    how='left'
)

df_all = df_all.with_columns([
    (pl.col('sh6') + ' - ' + pl.col('NO_SH6_POR')).alias('sh6_product')
])

df_all = df_all.rename({'NO_SH6_POR': 'product_description_br'})

df_all.head()













########################################
# Grouping and analysis
########################################
df_all_year_filtered = df_all.filter(pl.col('year') == 2023)

df_defence_countries = (
    df_all_year_filtered.group_by('exporter_name').agg([
        pl.sum('weighted_exports').alias('total_weighted_exports_defence')
    ])
    .with_columns(
        pl.col('total_weighted_exports_defence').round(2)
    )
    .sort('total_weighted_exports_defence', descending=True)
)

#######################################
df_defence_countries_sh6 = (
    df_all_year_filtered.group_by(['exporter_name', 'sh6_product', 'product_description_br']).agg([
        pl.sum('weighted_exports').alias('total_weighted_exports_defence_sh6')
    ])
    .with_columns(
        pl.col('total_weighted_exports_defence_sh6').round(2)
    )
    .sort('total_weighted_exports_defence_sh6', descending=True)
)
#######################################

# Dataframe completo: produtos de defesa entre 2019 e 2023 por país, com CAGR
df_all.head()
df_all.write_excel(data_processed / 'export_potential_defence_complete.xlsx')

# Dataframe agrupado por país com média ponderada das exportações nos últimos 5 anos
df_defence_countries.head()
df_defence_countries.write_excel(data_processed / 'export_potential_defence_countries.xlsx')

# Dataframe agrupado por país e produto (sh6) com média ponderada das exportações nos últimos 5 anos
df_defence_countries_sh6.head()
df_defence_countries_sh6.write_excel(data_processed / 'export_potential_defence_countries_sh6.xlsx')