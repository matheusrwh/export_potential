
#%%
from pathlib import Path

import polars as pl 

project_root = Path(__file__).resolve().parents[1]
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'

data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

epi_monetary_ufs_country = pl.read_parquet(data_processed / 'epi_monetary_ufs_country.parquet')
epi_monetary_ufs_sh6 = pl.read_parquet(data_processed / 'epi_monetary_ufs_sh6.parquet')
epi_monetary_ufs = pl.read_parquet(data_processed / 'epi_monetary_ufs.parquet')

#%%

epi_monetary_ufs_country.sample(10)

#%%
epi_monetary_ufs_sh6.sample(10)

#%%
epi_monetary_ufs.sample(10)