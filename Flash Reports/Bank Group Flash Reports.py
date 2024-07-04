# Databricks notebook source
# MAGIC %md
# MAGIC <b>Total submission value for per bank groups </b>
# MAGIC

# COMMAND ----------

# DBTITLE 1,Bank Group
# Flash Reports suppose to be in Materialized View but the feature is in preview so for PoC purposes we using logic views. 

# For Bank Group Flash reports we using  all reporting date, it is not using the latest submission date as the test data is not having enough data for given complex aggregate 

spark.sql("""
CREATE or REPLACE VIEW fdscatalog.fds.flash_report_bankgroup_aggregates AS 
with dps_group_sum AS (
select 
*
from 
fdscatalog.fds.submission s 
LEFT JOIN fdscatalog.fds.submission_value sv 
ON s.submission_id = sv.submission_id 
where  s.data_point_signature IN (select DISTINCT dd.data_point_signature 
                                  from fdscatalog.fds.submission dd 
                                  group by dd.data_point_signature)
)
select reference_date,data_point_signature,sum(numeric_position) as sum_numeric_position,Metrics,Base,CUD,main_category,
secondary_category,CPS,RCP,RPR,MCB,tab_value,x_axis_value,y_axis_value,bank_group
from dps_group_sum  
group by reference_date,data_point_signature,Metrics,Base,CUD,main_category,
secondary_category,CPS,RCP,RPR,MCB,tab_value,x_axis_value,y_axis_value,bank_group
"""
)

