
  create view "airflow"."public_silver"."precos_combustiveis__dbt_tmp"
    
    
  as (
    -- models/silver/precos_combustiveis.sql
select
    data_inicial,
    data_final,
    estado,
    municipio,
    produto,
    numero_de_postos_pesquisados,
    unidade_de_medida,
    preco_medio_revenda,
    desvio_padrao_revenda,
    preco_minimo_revenda,
    preco_maximo_revenda,
    coeficiente_variacao_revenda,
    moeda,
    medida
from "airflow"."silver"."precos_combustiveis"
  );