#%%
import pandas as pd 
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]

references = project_root / 'references'

# Validação rápida do calculo dos shares no databricks 

df_databricks = pd.read_csv(references / 'share-ufs.csv')
df_databricks.head()

# %%
df_shares = pd.read_excel(references / 'share_sc.xlsx')

df_shares.head()
# %%

df_databricks_sc = df_databricks[df_databricks['sg_uf'] == 'SC']
df_databricks_sc = df_databricks_sc[df_databricks_sc['nr_ano'] < 2025]
df_databricks_sc = df_databricks_sc.drop(columns=['vl_fob_total_produto', 'vl_fob_uf','sg_uf'])
df_databricks_sc.head()

# %%
df_shares_sc = df_shares.melt(
    id_vars=['sh6'],
    var_name='ano_rosa', 
    value_name='share_rosa')

df_shares_sc.head()
# %%
df_all = df_databricks_sc.merge(
    df_shares_sc, 
    left_on=['cd_sh6', 'nr_ano'], 
    right_on=['sh6', 'ano_rosa'], 
    how='left')

df_all = df_all.assign(share_rosa_pct=df_all['share_rosa'] * 100)

df_all = df_all.dropna(subset=['share_rosa_pct', 'pct_participacao'])
df_all.head()

#%% 
df_all.sample(10)
# %%
# todos_iguais = (df_all['pct_participacao'] == df_all['share_rosa_pct']).all()
# print(todos_iguais)

# %%
divergentes = df_all.round({'pct_participacao': 10, 'share_rosa_pct': 10})
divergentes = divergentes[divergentes['pct_participacao'] != divergentes['share_rosa_pct']]
print(divergentes)
# %%
df_all.dtypes


#%% Salvando o arquivo final de shares 
 
df_final_shares = df_databricks[['cd_sh6', 'nr_ano', 'sg_uf', 'pct_participacao',]]

df_final_shares.to_csv(project_root / 'data' / 'processed' / 'final_shares.csv', index=False)
