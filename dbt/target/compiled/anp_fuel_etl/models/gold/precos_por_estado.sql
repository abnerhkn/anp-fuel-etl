-- models/gold/precos_por_estado.sql
select
    estado,
    produto,
    date_trunc('week', data_inicial) as semana,
    avg(preco_medio_revenda) as preco_medio_semana,
    min(preco_minimo_revenda) as preco_minimo_semana,
    max(preco_maximo_revenda) as preco_maximo_semana
from "airflow"."public_silver"."precos_combustiveis"
group by 1,2,3