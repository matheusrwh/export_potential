import polars as pl
from pathlib import Path

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
app = project_root / 'app'
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Loading the data ########
df_ease = pl.read_parquet(data_processed / 'ease_of_trade.parquet')
df_demand = pl.read_parquet(data_processed / 'demand_potential.parquet')
df_supply = pl.read_parquet(data_processed / 'supply_potential_sc.parquet')
df_bilateral_sh6 = pl.read_parquet(data_interim / 'bilateral_exports_sh6.parquet')

df_ease.head()
df_demand.head()
df_supply.head()
df_bilateral_sh6.head()

df_epi = df_supply.join(
    df_demand.select(['importer', 'sh6', 'projected_import_value']),
    on=['sh6'],
    how='left').join(
        df_ease.select(['importer', 'ease_of_trade']),
        on=['importer'],
        how='left').join(
            df_bilateral_sh6,
            on=['exporter', 'importer', 'sh6'],
            how='left'        
        )

df_epi.head()

df_epi = df_epi.with_columns([
    (pl.col('sc_share_proj_2027') * pl.col('projected_import_value') * pl.col('ease_of_trade')).alias('epi_score')
])

df_epi = df_epi.sort('epi_score', descending=True)

df_epi = df_epi.filter(pl.col('epi_score').is_not_nan())

df_epi.head()

#### Normalizing the EPI scores between 0 and 1 ####
df_epi = df_epi.with_columns([
    pl.col('epi_score').fill_null(0).alias('epi_score'),
    pl.col('proj_exports_sc_2027').fill_null(0).alias('proj_exports_sc_2027'),
    pl.col('bilateral_exports_sc_sh6').fill_null(0).alias('bilateral_exports_sc_sh6'),
])

# Calculate min and max epi_score per sh6
df_min_max = (
    df_epi
    .group_by('sh6')
    .agg([
        pl.col('epi_score').min().alias('min_epi_score'),
        pl.col('epi_score').max().alias('max_epi_score'),
    ])
)

# Join min/max and normalize
df_epi = (
    df_epi
    .join(df_min_max, on='sh6', how='left')
    .with_columns([
        pl.when(pl.col('max_epi_score') != pl.col('min_epi_score'))
          .then((pl.col('epi_score') - pl.col('min_epi_score')) / (pl.col('max_epi_score') - pl.col('min_epi_score')))
          .otherwise(0.0)
          .alias('epi_score_normalized')
    ])
)

df_epi.head()


################ JOINS E FORMATAÇÃO FINAL ################
df_countries = pl.read_csv(references / 'countries_br.csv', encoding='latin1', separator=';')

df_countries.head()

df_epi = df_epi.join(
    df_countries.select([
        pl.col('CO_PAIS_ISOA3').alias('importer'),
        pl.col('NO_PAIS').alias('importer_name')]),
    on='importer',
    how='left'
)

df_epi = df_epi.with_columns([
    pl.col('sh6').cast(str).str.zfill(6).alias('sh6')
])

df_products = pl.read_excel(references / 'products_br_mdic.xlsx')
df_products.head()

df_epi = df_epi.join(
    df_products,
    left_on='sh6',
    right_on='CO_SH6',
    how='left'
)

df_epi = df_epi.with_columns([
    (pl.col('sh6') + ' - ' + pl.col('NO_SH6_POR')).alias('sh6_product')
])

df_epi = df_epi.rename({'NO_SH6_POR': 'product_description_br'})

df_sc_comp = pl.read_excel(references / 'sh6_mundo_comp.xlsx')

df_sc_comp.head()

df_epi = df_epi.join(
    df_sc_comp,
    left_on='sh6',
    right_on='sh6',
    how='left'
)

cores_comp = ({
    'Alimentos e Bebidas': '#6BBE50',
    'Agropecuária': "#1FBDE1",
    'Papel e Celulose': '#CCD274',
    'Construção': '#5F72A1',
    'Equipamentos Elétricos': '#E4863B',
    'Fármacos': '#05B5A0',
    'Fumo': '#5A4A42',
    'Automotivo': '#3A6C9E',
    'Cerâmico': '#AC6142',
    'Indústria Diversa': '#3A814B',
    'Extrativo': '#46606C',
    'Indústria Gráfica': '#EC008C',
    'Madeira e Móveis': '#8A6138',
    'Máquinas e Equipamentos': '#E4863B',
    'Metalmecânica e Metalurgia': '#266563',
    'Óleo, Gás e Eletricidade': '#FBA81A',
    'Produtos Químicos e Plásticos': '#0B416C',
    'Saneamento Básico': '#2BB673',
    'Produção Florestal': "#024814",
    'Tecnologia da Informação e Comunicação': '#4B828B',
    'Têxtil, Confecção, Couro e Calçados': '#F05534'
})

# Converter para pandas
df_epi = df_epi.to_pandas()

df_epi['color'] = df_epi['sc_comp'].apply(lambda x: cores_comp.get(x, '#000000'))

df_epi = pl.from_pandas(df_epi)

df_epi = df_epi.select(['exporter', 'importer', 'importer_name', 'sh6', 'sh6_product', 'product_description_br', 'sc_comp', 'color',
                        'bilateral_exports_sc_sh6', 'proj_exports_sc_2027', 'projected_import_value', 'epi_score', 'epi_score_normalized'])

df_epi.write_parquet(data_processed / 'epi_scores.parquet')