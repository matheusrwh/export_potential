'''%sql

tabela de shares por estado e produto foi calculada diretamente no databricks, utilizando a seguinte query SQL
A query calcula o valor total de exportação (vl_fob) por estado (sg_uf) e produto (cd_sh6) para cada ano (nr_ano). 
Em seguida, calcula a participação percentual de cada estado no total de exportação do produto para aquele ano. 
O modo de salvamento do notebook com a query SQL no databricks será validado. Para reprodução, a query pode ser copiada
e colada em um ambiente SQL que tenha acesso ao big data do Observatório.

Na versão local, a tabela de shares está salva em csv "/ references / share-ufs.csv .

Query e transformações realizadas no databricks:'''

WITH base_agrupada AS (
    SELECT 
        nr_ano,
        cd_sh6,
        sg_uf,
        SUM(vl_fob) AS vl_fob_uf
    FROM obsref.comex.flat_ncm
    WHERE CAST(nr_ano AS INT) BETWEEN 2017 AND 2024
      AND tp_carga = 'Exportação'
    GROUP BY nr_ano, cd_sh6, sg_uf
)
SELECT 
    nr_ano,
    cd_sh6,
    sg_uf,
    vl_fob_uf,

    SUM(vl_fob_uf) OVER(PARTITION BY nr_ano, cd_sh6) AS vl_fob_total_produto,
 
    (vl_fob_uf / SUM(vl_fob_uf) OVER(PARTITION BY nr_ano, cd_sh6)) AS pct_participacao
FROM base_agrupada
ORDER BY nr_ano DESC, cd_sh6, pct_participacao DESC;

