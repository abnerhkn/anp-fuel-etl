
  
    

  create  table "airflow"."public_gold"."precos_por_regiao__dbt_tmp"
  
  
    as
  
  (
    select
    r.regiao,
    p.produto,
    date_trunc('week', p.data_inicial) as semana,
    avg(p.preco_medio_revenda) as preco_medio_semana,
    min(p.preco_minimo_revenda) as preco_minimo_semana,
    max(p.preco_maximo_revenda) as preco_maximo_semana
from "airflow"."public_silver"."precos_combustiveis" p
join "airflow"."public"."dim_regiao_estado" r
  on p.estado = r.estado
group by 1,2,3
  );
  