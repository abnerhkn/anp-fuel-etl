select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select regiao
from "airflow"."public_gold"."precos_por_regiao"
where regiao is null



      
    ) dbt_internal_test