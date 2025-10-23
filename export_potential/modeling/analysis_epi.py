import polars as pl
from pathlib import Path
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

######## Setting the directories ########
project_root = Path(__file__).resolve().parents[2]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
app_data = project_root / 'app' / 'data'
references = project_root / 'references'

def clusterize_group(group):
    group = group.copy()
    # Definir categorias por intervalo fixo
    bins = [0, 0.02, 0.04, 0.06, 0.2, 1.01]
    labels = ['Baixo', 'Médio-baixo', 'Médio', 'Médio-alto', 'Alto']
    group['categoria'] = pd.cut(group['epi_score_normalized'], bins=bins, labels=labels, include_lowest=True, right=False)
    group['categoria'] = group['categoria'].astype(str)  # Garantir que os labels apareçam como texto
    group['cluster'] = pd.Categorical(group['categoria'], categories=labels, ordered=True).codes
    group = group.sort_values(by=['cluster'], ascending=False)
    return group

######## Loading the data ########
df_epi = pl.read_parquet(data_processed / 'epi_scores.parquet')

df_epi.head()

######################### AGREGGATING BY PRODUCT #########################
df_epi_sh6 = df_epi.group_by(['sh6', 'sh6_product', 'product_description_br', 'sc_comp', 'color']).agg([
    pl.sum('bilateral_exports_sc_sh6').alias('bilateral_exports_sc_sh6'),
    pl.sum('epi_score').alias('epi_score'),
])

df_epi_sh6 = df_epi_sh6.sort('epi_score', descending=True)

df_epi_sh6.head()

# Normalizar epi_score entre 0 e 1
epi_min = df_epi_sh6['epi_score'].min()
epi_max = df_epi_sh6['epi_score'].max()
df_epi_sh6 = df_epi_sh6.with_columns([
    ((pl.col('epi_score') - epi_min) / (epi_max - epi_min)).alias('epi_score_normalized')
])

df_epi_sh6_pd = df_epi_sh6.to_pandas()
df_epi_sh6_clustered = clusterize_group(df_epi_sh6_pd)
df_epi_sh6 = pl.from_pandas(df_epi_sh6_clustered)

df_epi_sh6.write_parquet(app_data / 'epi_scores_sh6.parquet')

######################### AGREGGATING BY COUNTRY #########################
df_epi = df_epi.with_columns([
    pl.col('epi_score_normalized').fill_nan(0)
])

df_epi_country = df_epi.group_by(['importer', 'importer_name']).agg([
    pl.sum('bilateral_exports_sc_sh6').alias('bilateral_exports_sc_sh6'),
    pl.sum('epi_score').alias('epi_score'),
])

df_epi_country.head()

# Normalizar epi_score entre 0 e 1
epi_min = df_epi_country['epi_score'].min()
epi_max = df_epi_country['epi_score'].max()
df_epi_country = df_epi_country.with_columns([
    ((pl.col('epi_score') - epi_min) / (epi_max - epi_min)).alias('epi_score_normalized')
])

df_epi_country_pd = df_epi_country.to_pandas()
df_epi_country_clustered = clusterize_group(df_epi_country_pd)
df_epi_country = pl.from_pandas(df_epi_country_clustered)

df_epi_country = df_epi_country.sort('epi_score_normalized', descending=True)

df_epi_country.write_parquet(app_data / 'epi_scores_countries.parquet')

df_epi_country.head()

######################### PRODUCT AND MARKET #########################
df_epi.head()

df_epi_clustered_list = []
for sh6, group in df_epi.to_pandas().groupby('sh6'):
    clustered = clusterize_group(group)
    df_epi_clustered_list.append(clustered)

df_epi_clustered = pd.concat(df_epi_clustered_list, ignore_index=True)
df_epi = pl.from_pandas(df_epi_clustered)

df_epi.write_parquet(app_data / 'epi_scores.parquet')

######################### SC COMPETITIVA #########################
df_epi_comp = df_epi.group_by(['sc_comp', 'color']).agg([
    pl.sum('bilateral_exports_sc_sh6').alias('bilateral_exports_sc_sh6'),
    pl.sum('epi_score').alias('epi_score'),
])

# Normalizar epi_score entre 0 e 1
epi_min = df_epi_comp['epi_score'].min()
epi_max = df_epi_comp['epi_score'].max()
df_epi_comp = df_epi_comp.with_columns([
    ((pl.col('epi_score') - epi_min) / (epi_max - epi_min)).alias('epi_score_normalized')
])

df_epi_comp_pd = df_epi_comp.to_pandas()
df_epi_comp_clustered = clusterize_group(df_epi_comp_pd)
df_epi_comp = pl.from_pandas(df_epi_comp_clustered)

df_epi_comp = df_epi_comp.sort('epi_score_normalized', descending=False)

df_epi_comp.write_parquet(app_data / 'epi_scores_sc_comp.parquet')

df_epi_comp.head()
