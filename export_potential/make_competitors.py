import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_raw = project_root / 'data' / 'raw'
app_data = project_root / 'app' / 'data'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
csv_files = [f for f in data_raw.glob('baci_*.csv')]
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
)

df_all.head()

############ Treating the data ############
df_all = df_all.with_columns([
    pl.col('sh6').cast(str).str.zfill(6).alias('sh6')
])

df_description_br = pl.read_excel(references / 'products_br_mdic.xlsx')

df_description_br.head()

df_all = df_all.join(
    df_description_br.select([
        pl.col('CO_SH6').alias('sh6'),
        pl.col('NO_SH6_POR').alias('product_description_br')
    ]),
    on='sh6',
    how='left'
)

df_all = df_all.with_columns([
    (pl.col('sh6') + ' - ' + pl.col('product_description_br')).alias('sh6_product')
])

df_countries = pl.read_csv(references / 'countries_br.csv', encoding='latin1', separator=';')

df_countries.head()

df_all = df_all.join(
    df_countries.select([
        pl.col('CO_PAIS_ISOA3').alias('importer'),
        pl.col('NO_PAIS').alias('importer_name')
    ]),
    on='importer',
    how='left'
)

df_all = df_all.join(
    df_countries.select([
        pl.col('CO_PAIS_ISOA3').alias('exporter'),
        pl.col('NO_PAIS').alias('exporter_name')
    ]),
    on='exporter',
    how='left'
)

df_all = df_all.select([
    'year', 'exporter', 'exporter_name', 'importer', 'importer_name',
    'sh6', 'product_description_br', 'sh6_product', 'value'])

df_all.head()
df_all.shape

########### Filtering for SC-SH6 ###########
df_sh6_sc = pl.read_excel(references / 'share_sc.xlsx')

df_all = df_all.filter(
    pl.col('sh6').is_in(df_sh6_sc.select(pl.col('sh6')).to_series().implode())
)

df_all.head()
df_all.shape

########### Calculating the cagr_5y ###########
# Calculando a CAGR de 5 anos das exportações do produto sh6 por país importador
def calculate_cagr(df, value_col, year_col):
    min_year = df[year_col].min()
    max_year = df[year_col].max()
    n_years = max_year - min_year
    start_value = df.filter(pl.col(year_col) == min_year)[value_col].sum()
    end_value = df.filter(pl.col(year_col) == max_year)[value_col].sum()
    if start_value == 0 or n_years == 0:
        return None
    cagr = ((end_value / start_value) ** (1 / n_years)) - 1
    return cagr

cagr_df = (
    df_all
    .group_by(['exporter', 'importer', 'sh6', 'exporter_name', 'importer_name', 'product_description_br', 'sh6_product'])
    .agg([
        pl.col('year').min().alias('min_year'),
        pl.col('year').max().alias('max_year'),
        pl.col('value').filter(pl.col('year') == pl.col('year').min()).sum().alias('start_value'),
        pl.col('value').filter(pl.col('year') == pl.col('year').max()).sum().alias('end_value')
    ])
    .with_columns([
        (((pl.col('end_value') / pl.col('start_value')) ** (1 / (pl.col('max_year') - pl.col('min_year'))) - 1)*100).alias('cagr_5y')
    ])
)

cagr_df.head()

df_all = df_all.join(
    cagr_df.select(['exporter', 'importer', 'sh6', 'cagr_5y']),
    on=['exporter', 'importer', 'sh6'],
    how='left'
)

df_all = df_all.filter(pl.col('year') == 2023)

df_all = df_all.with_columns([
    pl.col('value').sum().over(['importer', 'sh6']).alias('importer_sh6_total_value')
])

df_all = df_all.with_columns([
    ((pl.col('value') / pl.col('importer_sh6_total_value')) * 100).alias('importer_sh6_share')
])

df_all.head()
df_all.shape

df_all = df_all.select([
    'year', 'exporter', 'exporter_name', 'importer', 'importer_name',
    'sh6', 'product_description_br', 'sh6_product', 'value', 'cagr_5y', 'importer_sh6_share'])

def format_contabil(value):
    if value >= 1e9:
        return f"{value/1e9:,.1f} bi".replace(",", "X").replace(".", ",").replace("X", ".")
    elif value >= 1e6:
        return f"{value/1e6:,.1f} mi".replace(",", "X").replace(".", ",").replace("X", ".")
    elif value >= 1e3:
        return f"{value/1e3:,.1f} mil".replace(",", "X").replace(".", ",").replace("X", ".")
    else:
        return f"{value:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")

def format_decimal(value, decimals=1):
    return f"{value:.{decimals}f}".replace(".", ",")

df_all = df_all.with_columns(
    pl.col("cagr_5y").map_elements(lambda x: format_decimal(x, 1)).alias("cagr_5y_adj"),
    pl.col('value').map_elements(format_contabil).alias('value_contabil'),
    pl.col('importer_sh6_share').map_elements(lambda x: format_decimal(x, 2)).alias('importer_sh6_share')
)

df_all.head()

df_all.write_parquet(app_data / 'df_competitors.parquet', compression='snappy')