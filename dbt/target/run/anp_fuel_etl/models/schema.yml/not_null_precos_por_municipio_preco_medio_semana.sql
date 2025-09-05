select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select preco_medio_semana
from "airflow"."public_gold"."precos_por_municipio"
where preco_medio_semana is null



      
    ) dbt_internal_test