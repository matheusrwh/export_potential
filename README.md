Adaptacao da metodologia de potencial de exportacoes do ITC para o Brasil, com desagregacao por Unidades da Federacao (UFs).

## Objetivo

Este projeto calcula potencial de exportacoes no nivel UF x produto x pais, preservando comparabilidade com o fluxo SC-only e expandindo o modelo para as 27 UFs.

## Estrutura do fluxo atual

O pipeline principal por UF e composto por:

1. `export_potential/make_comex_exps.py`
2. `export_potential/make_comex_imps.py`
3. `export_potential/make_demand.py`
4. `export_potential/make_supply.py`
5. `export_potential/make_ease.py`
6. `export_potential/modeling/monetize_epi_ufs.py`

## Passo a passo de utilizacao

### 1) Atualizar base de shares UF

Atualize `references/share-ufs.csv` com a extracao mais recente do Databricks.

Regra critica para comparabilidade com SC-only:

- o denominador do share por produto/ano deve ser calculado no universo completo de codigos de origem da base
- o filtro para as 27 UFs deve ocorrer no numerador e na grade final de saida

### 2) Validar fechamento de shares para SC

Execute:

```bash
python export_potential/validate_shares.py
```

Esperado:

- `Divergencias acima da tolerancia`: 0
- diferencas residuais apenas de ponto flutuante

### 3) Rodar o pipeline completo UF

Ordem recomendada (sequencial):

```bash
python export_potential/make_comex_exps.py
python export_potential/make_comex_imps.py
python export_potential/make_demand.py
python export_potential/make_supply.py
python export_potential/make_ease.py
python export_potential/modeling/monetize_epi_ufs.py
```

### 4) Explorar resultados no notebook atual

Notebook principal da exploracao atual:

- `notebooks/exploracao_analises_potencial_ufs.ipynb`

Este notebook entrega:

1. Dispersao por parceiro com:
	- eixo X: CAGR das exportacoes da UF para o parceiro no SH6
	- eixo Y: market share de importacoes do parceiro no SH6
	- tamanho da bolha: exportacao da UF no ultimo ano da serie
2. HHI de concentracao por UF para parceiros importadores no SH6 selecionado

## Scripts alterados e pontos de alteracao no codigo

Esta secao documenta exatamente os arquivos alterados para a versao UF e quais blocos de codigo mudaram em relacao ao fluxo SC-only.

### 1) `export_potential/make_shares.sql`

Pontos alterados:

1. CTE `base_agrupada_todas`:
	- agrega `vl_fob` por `nr_ano, cd_sh6, sg_uf` sem filtrar previamente para as 27 UFs.
2. CTE `denominador_produto`:
	- calcula `vl_fob_total_produto` com base em `base_agrupada_todas` (universo completo), preservando o denominador nacional do produto.
3. CTE `base_agrupada_ufs_validas`:
	- aplica o filtro de UFs validas apenas para numerador e grade de saida.
4. CTEs `grade_completa` e `base_completa`:
	- densificam a malha `ano x sh6 x uf` para garantir cobertura completa por UF.
5. Select final (`pct_participacao`):
	- razao final usa `COALESCE(vl_fob_uf, 0) / vl_fob_total_produto`.

Impacto da mudanca:

- evita distorcer o share ao reduzir o denominador apenas para UFs selecionadas.
- garante comparabilidade do recorte SC com o SC-only.

### 2) `export_potential/make_supply.py`

Pontos alterados:

1. Leitura e tipagem da base de shares:
	- carga de `references/share-ufs.csv`.
	- cast explicito de `cd_sh6` e `nr_ano` para inteiro.
2. Join de shares no fluxo Brasil:
	- join por `left_on=['sh6', 'year']` e `right_on=['cd_sh6', 'nr_ano']`.
3. Calculo de valor por UF:
	- nova coluna `valor_uf = value * pct_participacao`.
4. Media ponderada historica por UF:
	- bloco de pesos de 8 anos para gerar `weighted_exports_uf`.
5. Congelamento do denominador mundial por SH6:
	- bloco `df_world_proj_sh6 = df_all.group_by('sh6').agg(sum('proj_exports_2030'))` executado antes do join com linhas UF.
6. Share projetado UF 2030:
	- `uf_share_proj_2030 = proj_exports_uf_2030 / world_proj_exports_2030`.

Impacto da mudanca:

- impede inflacao artificial do denominador global apos expansao por UF.
- preserva comparabilidade do potencial de oferta com o baseline SC-only.

### 3) `export_potential/make_ease.py`

Pontos alterados:

1. Preparacao de fluxo bilateral por UF:
	- leitura de `share-ufs.csv` com alias `share_uf`.
	- calculo de `value_uf = value * share_uf` no nivel bilateral.
2. Persistencia intermediaria por UF e SH6:
	- exporta `data/interim/bilateral_exports_sh6.parquet` com chaves `exporter, importer, sh6, sg_uf`.
3. Expansao da demanda para UF:
	- join de demanda com `supply_potential_ufs` em `sh6` para criar linhas por `sg_uf`.
	- calculo `value_uf = weighted_imports * uf_share_proj_2030` e agregacao para `sum_value_uf`.
4. Calculo final de facilidade comercial por UF:
	- join por `['importer', 'sg_uf']` com bilateral.
	- `ease_of_trade = bilateral_exports_uf / sum_value_uf`.
	- exporta `ease_of_trade_ufs.parquet`.

Impacto da mudanca:

- transforma o ease de um indicador nacional para um indicador especifico por UF.
- mantem a logica economica original e adiciona granularidade territorial.

### 4) `export_potential/modeling/monetize_epi_ufs.py`

Pontos alterados:

1. Painel de monetizacao por UF:
	- join conjunto de `supply_potential_ufs`, `demand_potential`, `ease_of_trade_ufs` e `bilateral_exports_sh6`.
2. Higienizacao numerica:
	- funcao `finite_or_zero` aplicada a colunas criticas para evitar `null`, `nan` e `inf`.
3. Score bruto de atratividade:
	- `epi_score = uf_share_proj_2030 * projected_import_value * ease_of_trade`.
4. Alocacao da oferta por UF dentro de cada `sh6`:
	- `epi_weight_share = epi_score / sum(epi_score)` por `['sh6', 'sg_uf']`.
	- `allocated_supply_value = proj_exports_uf_2030 * epi_weight_share`.
5. Teto de viabilidade por mercado:
	- `potential_value = min(allocated_supply_value, market_feasible_value)`.
6. Derivacoes monetarias:
	- `unrealized_potential_value`, `realized_potential_value`, `overtrade_value`, `potential_utilization_ratio`.
7. Saidas agregadas por UF:
	- detalhado: `epi_monetary_ufs.json`.
	- agregado por pais: `epi_monetary_ufs_country.json`.
	- agregado por SH6: `epi_monetary_ufs_sh6.json`.

Impacto da mudanca:

- monetizacao passa a refletir capacidade e atratividade no nivel UF x SH6 x importador.
- agrega resultados de forma consistente com a versao detalhada.

## Saidas uteis do fluxo atual

Os artefatos essenciais do fluxo UF atual estao em `data/processed`:

- `demand_potential.parquet`
- `supply_potential_ufs.parquet`
- `ease_of_trade_ufs.parquet`
- `epi_monetary_ufs.json`
- `epi_monetary_ufs_country.json`
- `epi_monetary_ufs_sh6.json`

Observacao:

- outputs legados de comparacao SC-only e planilhas auxiliares antigas nao fazem parte do fluxo principal atual e podem ser descartados.