'''%sql

tabela de shares por estado e produto foi calculada diretamente no databricks, utilizando a seguinte query SQL
A query calcula o valor total de exportação (vl_fob) por estado (sg_uf) e produto (cd_sh6) para cada ano (nr_ano). 
Em seguida, calcula a participação percentual de cada estado no total de exportação do produto para aquele ano. 
O modo de salvamento do notebook com a query SQL no databricks será validado. Para reprodução, a query pode ser copiada
e colada em um ambiente SQL que tenha acesso ao big data do Observatório.

Na versão local, a tabela de shares está salva em csv "/ references / share-ufs.csv .

Query e transformações realizadas no databricks:'''

WITH anos AS (
    SELECT EXPLODE(SEQUENCE(2017, 2024)) AS nr_ano
),
ufs_validas AS (
    SELECT EXPLODE(ARRAY(
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
        'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
    )) AS sg_uf
),
base_agrupada_todas AS (
    SELECT 
        nr_ano,
        cd_sh6,
        sg_uf,
        SUM(vl_fob) AS vl_fob_uf
    FROM obsref.comex.flat_ncm
    WHERE CAST(nr_ano AS INT) BETWEEN 2017 AND 2024
      AND tp_carga = 'Exportação'
    GROUP BY nr_ano, cd_sh6, sg_uf
),
produtos AS (
    SELECT DISTINCT
        cd_sh6
    FROM base_agrupada_todas
),
denominador_produto AS (
    SELECT
        nr_ano,
        cd_sh6,
        SUM(COALESCE(vl_fob_uf, 0)) AS vl_fob_total_produto
    FROM base_agrupada_todas
    GROUP BY nr_ano, cd_sh6
),
base_agrupada_ufs_validas AS (
    SELECT
        nr_ano,
        cd_sh6,
        sg_uf,
        vl_fob_uf
    FROM base_agrupada_todas
    WHERE sg_uf IN (SELECT sg_uf FROM ufs_validas)
),
grade_completa AS (
    SELECT
        a.nr_ano,
        p.cd_sh6,
        u.sg_uf
    FROM anos a
    CROSS JOIN produtos p
    CROSS JOIN ufs_validas u
),
base_completa AS (
    SELECT
        g.nr_ano,
        g.cd_sh6,
        g.sg_uf,
        b.vl_fob_uf
    FROM grade_completa g
    LEFT JOIN base_agrupada_ufs_validas b
      ON g.nr_ano = b.nr_ano
     AND g.cd_sh6 = b.cd_sh6
     AND g.sg_uf = b.sg_uf
)
SELECT 
    b.nr_ano,
    b.cd_sh6,
    b.sg_uf,
    b.vl_fob_uf,

    d.vl_fob_total_produto,

    CASE
        WHEN d.vl_fob_total_produto > 0
        THEN COALESCE(b.vl_fob_uf, 0) / d.vl_fob_total_produto
        ELSE NULL
    END AS pct_participacao
FROM base_completa b
LEFT JOIN denominador_produto d
  ON b.nr_ano = d.nr_ano
 AND b.cd_sh6 = d.cd_sh6
ORDER BY nr_ano DESC, cd_sh6, pct_participacao DESC;

