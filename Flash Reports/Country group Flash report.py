# Databricks notebook source
# DBTITLE 1,County Group
# Flash Reports suppose to be in Materialized View but the feature is in preview so for PoC purposes we using logic views. 

# In the county group flash reports, for PoC purposes we using EU group, other country group are not in our datamodel

# For Country group Flash reports we using  submission_latest reporting date ('2021-04-01T00:00:00')

spark.sql("""
CREATE or REPLACE VIEW fdscatalog.fds.flash_report_countrygroup_aggregates AS 
with dps_group_sum AS (
select 
*
from 
fdscatalog.fds.submission_latest s 
LEFT JOIN fdscatalog.fds.submission_value sv 
ON s.submission_id = sv.submission_id 
where  s.data_point_signature IN (select DISTINCT dd.data_point_signature 
                                  from fdscatalog.fds.submission_latest dd 
                                  group by dd.data_point_signature)
)
select reference_date,data_point_signature,sum(numeric_position) as sum_numeric_position,Metrics,Base,CUD,main_category,
secondary_category,CPS,RCP,RPR,MCB,tab_value,x_axis_value,y_axis_value,country_id
from dps_group_sum  
group by reference_date,data_point_signature,Metrics,Base,CUD,main_category,
secondary_category,CPS,RCP,RPR,MCB,tab_value,x_axis_value,y_axis_value,country_id
""")
