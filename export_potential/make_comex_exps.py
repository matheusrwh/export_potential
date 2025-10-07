import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[1]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

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
)

df_all.head()

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

df_all.head()
df_all.shape

df_all.write_parquet(data_interim / 'comex_exps_weighted.parquet')
