select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select municipio
from "airflow"."public_gold"."precos_por_municipio"
where municipio is null



      
    ) dbt_internal_test