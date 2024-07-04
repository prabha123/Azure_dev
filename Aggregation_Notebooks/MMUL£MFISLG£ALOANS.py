# Databricks notebook source
# DBTITLE 1,Libarary
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

spark.sql(f"""
INSERT INTO fdscatalog.fds.aggregation 
SELECT 
    {ids} AS Id, 
    {cycle_id} AS cycle_id, 
    current_timestamp() AS execution_time, 
    total_bank_group_loans_to_LA - total_deductions_value,
    total_bank_group_loans_to_LA.reference_date as reporting_date,
    'MMUL£MFISLG£ALOANS' as aggregate_dps_name
FROM
(
    SELECT SUM (bk_sum) as total_bank_group_loans_to_LA,reference_date 
    FROM fdscatalog.fds.total_bank_group_loans_to_LA() group by all
) AS total_bank_group_loans_to_LA,
(
    SELECT total_deductions_value 
    FROM fdscatalog.fds.total_transit_item_deductions()
) AS total_transit_item_deductions
""")
