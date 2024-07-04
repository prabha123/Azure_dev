# Databricks notebook source
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
import pandas as pd
import json
import regex as re
import os
from pyspark.sql.functions import expr

# COMMAND ----------

def val_fromtable():
  id_val_from_table = sqlContext.sql("select max(submission_value_id) as i, max(submission_value_type_id) as j, max(submission_id) as sub_id from fdscatalog.fds.submission_value" ) 
  return  id_val_from_table

# COMMAND ----------

overall_data = []
files = os.listdir("/dbfs/FileStore/tables/for_demo/")
id_val_from_table = val_fromtable()
i = id_val_from_table.collect()[0][0]
j = id_val_from_table.collect()[0][1]
sub_id = id_val_from_table.collect()[0][2]

for file_path in files:
    num_val =  pd.DataFrame()
    #Form vale
    file_name = "/dbfs/FileStore/tables/for_demo/"+file_path #replace with file fath from Azure storage or DBFS file path
    form = 2 if 'pb' in file_name else (3 if 'bt' in file_name else 1)
    bank = 6 if 'BANK6' in file_name else (1 if 'BANK1' in file_name else(4 if 'BANK4' in file_name else (5 if 'BANK5' in file_name else (2 if 'BANK2' in file_name else 3))))


    with open(file_name) as f:
        data = json.load(f)
        # print(data)
    df1 = []
    df2 = []
    for key,v in data["facts"].items():
        # Get the "value" key
        i = i+1
        j = j+1
        sub_id = sub_id+1
        form_val = form
        curr_val = 1
        coun_val = 1
        sub_per_val = 23
        bank_val = bank

        if form == 2:
            sec_val = None
            rpr_val = None
        else:
            sec_val = None
        timestamp_val = None
        num_val = v['value']
        # # Flatten the "dimensions" dictionary
        dimensions = v['dimensions']
        df_dim = pd.json_normalize(dimensions)
        df_dim = df_dim.assign(submission_id = [sub_id])
        df_dim = df_dim.assign(numeric_value=[num_val])
        df_dim = df_dim.assign(form_id=[form_val])
        df_dim = df_dim.assign(currency_id = [curr_val])
        df_dim = df_dim.assign(country_id = [coun_val])
        df_dim = df_dim.assign(submission_period_id = [sub_per_val])
        df_dim = df_dim.assign(bank_id = [bank_val])
        df_dim = df_dim.assign(secondary_category = [sec_val])
        if form == 2:
            df_dim = df_dim.assign(RPR = [rpr_val])
        df_dim = df_dim.assign(timestamp = [timestamp_val])
        df_dim = df_dim.assign(submission_value_id = [i])
        df_dim = df_dim.assign(submission_value_type_id = [j])

        df_dim = df_dim.assign(dps=[dimensions])
        #print(df_dim)
        
        # Flatten the "cfl:extendedDataPointInfo" list
        extended_data_point_info = v['cfl:extendedDataPointInfo']
        df_data_point_info = pd.json_normalize(extended_data_point_info,max_level=0)
        #print(df_data_point_info)

        df1.append(df_dim)
        df2.append(df_data_point_info)

    dimension_df = pd.concat(df1)
    dps_df = pd.concat(df2)

    final_frame = pd.concat([dimension_df,dps_df],axis=1)
    final_frame['tab_val'] = final_frame['table'].str[-11:]

    if form == 1:
        #DQ form
        selected_columns = final_frame[["submission_id","dps","period","currency_id","country_id","submission_period_id","form_id","bank_id","concept","eba_dim:BAS","boe_dim:CUD","eba_dim:MCY","secondary_category","eba_dim:CPS","eba_dim:RCP","eba_dim:RPR",'eba_dim:MCB',"timestamp",'dimensions']]

        selected_columns.rename(columns = {'concept':'Metrics', 'eba_dim:BAS':'Base', 'eba_dim:MCY':'main_category', 'eba_dim:RCP':'RCP','eba_dim:CPS':'CPS','boe_dim:CUD':'CUD','eba_dim:MCB':'MCB','numeric_value':'numeric_position','dps':'data_point_signature','period':'reference_date',"eba_dim:RPR":'RPR'}, inplace = True) 
        
    elif form == 2:
        #PB form
        selected_columns = final_frame[["submission_id","dps","period","currency_id","country_id","submission_period_id","form_id","bank_id","concept","eba_dim:BAS","boe_dim:CUD","eba_dim:MCY","secondary_category","eba_dim:CPS","eba_dim:RCP","RPR", 'eba_dim:MCB','timestamp','dimensions']]

        selected_columns.rename(columns = {'concept':'Metrics', 'eba_dim:BAS':'Base', 'eba_dim:MCY':'main_category', 'eba_dim:RCP':'RCP','eba_dim:CPS':'CPS','boe_dim:CUD':'CUD','eba_dim:MCB':'MCB','numeric_value':'numeric_position','dps':'data_point_signature','period':'reference_date'}, inplace = True) 
    else:
        #BT form
        selected_columns = final_frame[["submission_id","dps","period","currency_id","country_id","submission_period_id","form_id","bank_id","concept","eba_dim:BAS","boe_dim:CUD","eba_dim:MCY","secondary_category","eba_dim:CPS","eba_dim:RCP","eba_dim:RPR",'eba_dim:MCB',"timestamp",'dimensions']] 

        selected_columns.rename(columns = {'concept':'Metrics', 'eba_dim:BAS':'Base', 'eba_dim:MCY':'main_category', 'eba_dim:RCP':'RCP','eba_dim:CPS':'CPS','boe_dim:CUD':'CUD','eba_dim:MCB':'MCB','numeric_value':'numeric_position','dps':'data_point_signature','period':'reference_date',"eba_dim:RPR":'RPR'}, inplace = True) 

    submission_val = final_frame[["submission_value_id","numeric_value", "x", "y","submission_id","submission_value_type_id","period","tab_val"]]
    submission_val.rename(columns = {'period':'reference_date'})

    df = spark.createDataFrame(selected_columns)
    submissionval_table = spark.createDataFrame(submission_val)
    #submissionval_table = submissionval_table.withColumn("numeric_value",expr("try_cast(tab_val as int)"))
    #display(submissionval_table)

    df.write.format("delta").mode("append").insertInto("fdscatalog.fds.submission")
    submissionval_table.write.format("delta").mode("append").insertInto("fdscatalog.fds.submission_value")
    
    # Create a Temporary View on top of our DataFrame, making it
    # accessible to the SQL MERGE statement below
    #df.createOrReplaceTempView("submissionMergeData")
    #submissionval_table.createOrReplaceTempView("submissionvalueMergeData")

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Seperate Notebook</b>

# COMMAND ----------

# MAGIC %md
# MAGIC <b><p>Submission_Latest table Data Ingestion</p></b>

# COMMAND ----------


# spark.sql("CREATE MATERIALIZED VIEW fdscatalog.fds.submission_latest AS " 
#           "SELECT * FROM fdscatalog.fds.submission WHERE reference_date = (SELECT MAX(reference_date) FROM fdscatalog.fds.submission)"
#   )

# For PoC we using the below date as the latest one becuase the actual latest date does not hold values for simple and medium aggregates
 
spark.sql("CREATE or REPLACE VIEW fdscatalog.fds.submission_latest AS " 
          "SELECT * FROM fdscatalog.fds.submission WHERE reference_date = '2021-04-01T00:00:00'"
  )  

# COMMAND ----------

# #SQL code to write in python

# dps_group_sum = spark.sql("""
# with dps_group_sum AS (
# select s.bank_id, sv.numeric_position,sv.x_axis_value,sv.y_axis_value,s.reference_date,s.data_point_signature from fdscatalog.fds.submission s 
# LEFT JOIN fdscatalog.fds.submission_value sv 
# ON s.submission_id = sv.submission_id 
# where  s.data_point_signature = '{boe_eba_CU:x9001, boe_met:mi9001, eba_BA:x6, boe_eba_CT:x9157, boe_eba_MC:x9246, eba_MC:x99, boe_eba_GA:x9001, boe_eba_RP:x9023, eba_TR:x16, ns0:213800BARCNOMFJ9X769, 2021-10-01T00:00:00, iso4217:GBP}'
# group by  s.bank_id, s.reference_date, s.data_point_signature,sv.numeric_position, sv.x_axis_value,sv.y_axis_value
# )
# select sum(numeric_position),reference_date,data_point_signature from dps_group_sum group by reference_date,data_point_signature
# """)
# display(dps_group_sum)
