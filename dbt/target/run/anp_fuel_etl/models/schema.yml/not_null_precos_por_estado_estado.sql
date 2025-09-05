select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select estado
from "airflow"."public_gold"."precos_por_estado"
where estado is null



      
    ) dbt_internal_test