# Databricks notebook source
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

id_val_from_table_agg = spark.sql("select max(id) as id, max(cycle_id) as cycle_id from fdscatalog.fds.aggregation")
ids = id_val_from_table_agg.collect()[0][0]
cycle_id = id_val_from_table_agg.collect()[0][1]
ids = ids + 1
cycle_id = cycle_id + 1


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Simple Aggregation SQL Equation  </b>

# COMMAND ----------

# DBTITLE 1,Final Aggregation(Simple)
# # Aggregate description: Quarterly amounts outstanding of UK resident banks' 
# all foreign currency total derivatives assets with non-resident banks (excl. CMIs) (in sterling millions) not seasonally adjusted

spark.sql(f"""
WITH simple_aggregate AS (
    SELECT 
    {ids} AS Id, 
    {cycle_id} AS cycle_id,
    current_timestamp AS execution_time, 
    SUM(sum_numeric_position) as time_series_value,
    reference_date as reporting_date,
    'DQUL£DQQRNKFATOTC' AS aggregate_dps_name
    FROM fdscatalog.fds.flash_report_boxcode_aggregates WHERE 
        --Filter metric by market value
        Metrics = 'boe_met:mi9001' 
        --Filter by Non UK resident
        AND RCP = 'boe_eba_GA:x9001'
        --Filter by DPs which are derivatives 
        AND main_category = 'eba_MC:x99'
        AND Base = 'eba_BA:x6'
        --DPs of currency Euro, Deposit-taking corporation or Central Monetory Institutions 
        AND (CUD = 'boe_eba_CU:x9001' OR CUD = 'eba_CU:EUR') 
        --Filter by Deposit-taking corporations [counterparty's home country definition] or Central Monetary Institutions [counterparty's home country definition]
        AND (CPS = 'boe_eba_CT:x9157' OR CPS = 'boe_eba_CT:x9158') 
        --Filter by Entities of the group [Statistics] or Other than entities of the group 
        AND (RPR = 'boe_eba_RP:x9023' OR RPR = 'boe_eba_RP:x9006' or RPR is null)
        AND tab_value = 'DQ.01.01.01'
        group by reference_date
)
--Insert simple_aggregate table into aggregation table
INSERT INTO fdscatalog.fds.aggregation
SELECT * FROM simple_aggregate
""")
