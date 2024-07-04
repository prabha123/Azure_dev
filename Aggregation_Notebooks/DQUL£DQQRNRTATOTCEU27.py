# Databricks notebook source
# # %run magic command can be used to include and run code from another notebook
%run "/Workspace/Users/p.muthusenapathy@accenture.com/UDF_Aggregates/UDF_for_Complex_Aggregates"

# # Accessing variables from Notebook B in Notebook A
# %store -r ids
# %store -r cycle_id
# print(cycle_id)
from pyspark.sql.functions import monotonically_increasing_id
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
import pandas as pd
import json
import regex as re
import os
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from datetime import datetime

currentDateAndTime = datetime.now()
import datetime

id_val_from_table = spark.sql("select max(id) as id, max(cycle_id) as cycle_id from fdscatalog.fds.aggregation")
ids = id_val_from_table.collect()[0][0]
cycle_id = id_val_from_table.collect()[0][1]
ids = ids + 1
cycle_id = cycle_id + 1

%store ids
%store cycle_id


# COMMAND ----------

# DBTITLE 1,Medium Query
# Quarterly amounts outstanding of UK resident banks' sterling and all foreign currency total derivatives assets with non-residents (in sterling millions) vis-a-vis EU27 countries not seasonally adjusted

spark.sql(f"""
INSERT INTO fdscatalog.fds.aggregation 
SELECT
    {ids} AS Id,
    {cycle_id} AS cycle_id,
    current_timestamp() AS execution_time,
    -- counrty group Equation 
    (eu.eu_sum) * (eq.eq_sum) / (gb.gb_sum) as time_series_value,
    eu.reference_date as reporting_date,
    'DQUL£DQQRNKFATOTCEU27' AS aggregate_dps_name
FROM 
( --Summation  of derivative assets from firms which reside in the EU
    SELECT
        SUM(sum_numeric_position) AS eu_sum,
        MAX(reference_date) AS reference_date 
    FROM
    (
        SELECT
            s.data_point_signature, s.RCP, s.sum_numeric_position, s.reference_date
        FROM
            fdscatalog.fds.flash_report_countrygroup_aggregates s
        WHERE
            --Country id represents the country group EU
            s.country_id = 1 
            --Filter Metric by Market Value
            AND s.Metrics = 'boe_met:mi9001' 
            --Filter by datapoints which are GBP or EUR currency
            AND (s.CUD = 'eba_CU:GBP' OR s.CUD = 'eba_CU:EUR' OR s.CUD = 'boe_eba_CU:x9001') 
            --Filter by DPs which are Derivatives
            AND s.main_category = 'eba_MC:x99' 
            --Filter by DPs which are assets
            AND s.Base = 'eba_BA:x6' 
            AND s.reference_date = '2021-04-01T00:00:00'
            AND s.tab_value = 'DQ.06.01.01'
        GROUP BY ALL
    )
) AS eu,
( --Summation of datapoints which are Deposit -taking corporations or other financial corporations and non financial sectors
    SELECT
        SUM(sum_numeric_position) AS eq_sum
    FROM
    ( 
        SELECT
            data_point_signature, RCP, sum_numeric_position, reference_date
        FROM
            fdscatalog.fds.flash_report_countrygroup_aggregates
        WHERE
            --Filter Metric by Market Value
            Metrics = 'boe_met:mi9001'
            --Filter by DPs which are Derivatives 
            AND main_category = 'eba_MC:x99' 
            --Filter by DPs which are assets
            AND Base = 'eba_BA:x6'  
            --Filter where receiving counter party is non-UK
            AND (RCP = 'boe_eba_GA:x9001' OR RCP = 'boe_eba_GA:x9006') 
            --Filter by datapoints which are GBPor EUR Currency
            AND (CUD = 'boe_eba_CU:x9001' OR CUD = 'eba_CU:EUR' OR CUD = 'eba_CU:GBP') 
            --Deposit -taking corporations or Central Monetory Institutions
            AND (CPS = 'boe_eba_CT:x9157' OR CPS = 'boe_eba_CT:x9158' OR CPS = 'boe_eba_CT:x9165') 
            --Entities of the group [Statistics]
            AND (RPR = 'boe_eba_RP:x9023' OR RPR = 'boe_eba_RP:x9006')
            AND tab_value = 'DQ.01.01.01'
    )
) AS eq,
( --Total of derivative assets from which are Non GB Resident Countried DQ
    SELECT
        SUM(sum_numeric_position) AS gb_sum
    FROM
    ( 
        SELECT
            data_point_signature, RCP, sum_numeric_position, reference_date
        FROM
            fdscatalog.fds.flash_report_countrygroup_aggregates s
        WHERE
            --Filter where receiving counter party in non-UK
            RCP NOT IN ('eba_GA:GB')
            --Filter Metric by Market Value
            AND Metrics = 'boe_met:mi9001'
            --Filter by datapoints which are GBPor EUR Currency
            AND (CUD = 'eba_CU:GBP' OR CUD = 'eba_CU:EUR' OR CUD = 'boe_eba_CU:x9001')
            --Filter by DPs which are Derivatives
            AND main_category = 'eba_MC:x99'
             --Filter by DPs which are assets
            AND Base = 'eba_BA:x6'
            AND tab_value = 'DQ.06.01.01'
    )
) AS gb
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC           sum(s.sum_numeric_position)
# MAGIC         FROM
# MAGIC             fdscatalog.fds.flash_report_countrygroup_aggregates s
# MAGIC         WHERE
# MAGIC             --Filter where receiving counter party in non-UK
# MAGIC             RCP NOT IN ('eba_GA:GB')
# MAGIC             --Filter Metric by Market Value
# MAGIC             AND Metrics = 'boe_met:mi9001'
# MAGIC             --Filter by datapoints which are GBPor EUR Currency
# MAGIC             AND (CUD = 'eba_CU:GBP' OR CUD = 'eba_CU:EUR' OR CUD = 'boe_eba_CU:x9001')
# MAGIC             --Filter by DPs which are Derivatives
# MAGIC             AND main_category = 'eba_MC:x99'
# MAGIC              --Filter by DPs which are assets
# MAGIC             AND Base = 'eba_BA:x6'
