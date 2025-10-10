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
references = project_root / 'references'

def clusterize_group(group):
    group = group.copy()
    group['log_epi_score'] = np.log(group['epi_score_normalized'])
    group = group.dropna(subset=['log_epi_score'])
    group = group[np.isfinite(group['log_epi_score'])]
    if len(group) < 2:
        group['cluster'] = 0
        group['categoria'] = 'Baixo'
        return group
    n_clusters = min(5, len(group))
    X = group[['log_epi_score']].values.reshape(-1, 1)
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=5)
    kmeans.fit(X)
    group['cluster'] = kmeans.labels_
    centroides = kmeans.cluster_centers_.flatten()
    ordem = np.argsort(centroides)
    mapa_ordenado = {old: new for new, old in enumerate(ordem)}
    group['cluster'] = group['cluster'].map(mapa_ordenado)
    categorias = ['Baixo', 'Médio-baixo', 'Médio', 'Médio-alto', 'Alto'][:n_clusters]
    group['categoria'] = group['cluster'].map(dict(enumerate(categorias)))
    group = group.sort_values(by=['cluster', 'log_epi_score'], ascending=False)
    return group

######## Loading the data ########
df_epi = pl.read_parquet(data_processed / 'epi_scores.parquet')

df_epi.head()

######################### AGREGGATING BY PRODUCT #########################
df_epi_sh6 = df_epi.group_by(['sh6', 'product_description']).agg([
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

# Mostrar a contagem de cada categoria no total
categoria_contagem = df_epi_sh6_clustered['categoria'].value_counts(normalize=True).reset_index()
categoria_contagem.columns = ['categoria', 'contagem']

print(categoria_contagem)

######################### AGREGGATING BY COUNTRY #########################
df_epi = df_epi.with_columns([
    pl.col('epi_score_normalized').fill_nan(0),
    pl.col('unrealized_potential').fill_nan(0)
])

df_epi_country = df_epi.group_by(['importer']).agg([
    pl.sum('bilateral_exports_sc_sh6').alias('bilateral_exports_sc_sh6'),
    pl.sum('epi_score').alias('epi_score'),
])

df_epi_country = df_epi_country.sort('bilateral_exports_sc_sh6', descending=True)

df_epi_country_pd = df_epi_country.to_pandas()
df_epi_country_clustered = clusterize_group(df_epi_country_pd)
df_epi_country = pl.from_pandas(df_epi_country_clustered)

# Mostrar a contagem de cada categoria no total
categoria_contagem_country = df_epi_country_clustered['categoria'].value_counts(normalize=True).reset_index()
categoria_contagem_country.columns = ['categoria', 'contagem']

print(categoria_contagem_country)

df_epi_country.head()

######################### AGREGGATING BY COUNTRY AND PRODUCT #########################
df_epi = df_epi.sort('bilateral_exports_sc_sh6', descending=True)

df_epi = df_epi.to_pandas()
df_epi_clustered = df_epi.groupby('sh6', group_keys=False).apply(clusterize_group)
df_epi = pl.from_pandas(df_epi_clustered)

df_epi.head()