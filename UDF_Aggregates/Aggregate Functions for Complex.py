# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE or REPLACE FUNCTION fdscatalog.fds.total_bank_group_loans_to_LA()
# MAGIC RETURNS TABLE (dps STRING, bank_group STRING,bk_sum DOUBLE, reference_date STRING)
# MAGIC LANGUAGE SQL
# MAGIC RETURN SELECT data_point_signature, bank_group, sum_numeric_position, reference_date
# MAGIC             FROM 
# MAGIC                 fdscatalog.fds.flash_report_bankgroup_aggregates 
# MAGIC             WHERE 
# MAGIC                 --Filter by Central banks
# MAGIC                 (bank_group = 'CTBK' OR bank_group = 'PBNB' OR bank_group = 'PBNS' OR bank_group = 'PBBK' OR bank_group = 'PBBS')
# MAGIC                 --Filter by Pound Sterling
# MAGIC                 AND CUD = 'eba_CU:GBP' 
# MAGIC                 --Filter by Regional governments or local authorities
# MAGIC                 AND CPS = 'eba_CT:x16' 
# MAGIC                 --Filter by (UNITED KINGDOM)
# MAGIC                 AND RCP = 'eba_GA:GB' 
# MAGIC                  -- Filter by Nominal value or Market value [statistics] or Outstanding amount
# MAGIC                 AND (Metrics = 'boe_met:mi9001' OR Metrics = 'boe_met:mi8001' OR Metrics = 'eba_met:mi357')
# MAGIC                  --Filter by Liablities or Assets (net) or memorandum items
# MAGIC                 AND (Base = 'boe_eba_BA:x9004' or Base = 'eba_BA:x6' or Base = 'eba_BA:x17')
# MAGIC                 --Filter by DPs which are Deposits [statistics] or Debt securities or Loans and Advances 
# MAGIC                 AND (main_category = 'boe_eba_MC:x9329' or main_category = 'eba_MC:x469' or main_category = 'boe_eba_MC:x6037')
# MAGIC                 --Filter by Bills or Instruments used for loans and advances excluding advances that are not loans and special central bank loans and reverse repo or Term loans. Reverse repurchase loans, Buy sell-backs, Securities borrowing transactions
# MAGIC                 AND (MCB = 'boe_eba_MC:x9055' or MCB = 'boe_eba_MC:x9344' or MCB = 'boe_eba_MC:x9035' OR MCB is NULL)
# MAGIC                 AND (tab_value = 'BT.01.01.01' or  tab_value = 'BT.02.01.01' OR tab_value = 'PB.01.01.01' )

# COMMAND ----------

spark.sql("""
CREATE or REPLACE FUNCTION fdscatalog.fds.total_transit_item_deductions()
RETURNS TABLE (total_deductions_value DOUBLE)
LANGUAGE SQL
RETURN
  -- Equation for counter party sector and residence of counter party
  SELECT ((-CAST('{TRANSITEM_ADD}' AS DOUBLE) *  sum(total_CTBK_AB.sum_numeric_position)) - sum(total_CTBK_AB.sum_numeric_position) ) + ((CAST('{TRANSITEM_SUB}' AS DOUBLE) * sum(total_PBBK_PBNB_AB.sum_numeric_position))- sum(total_PBBK_PBNB_AB.sum_numeric_position)) AS total_deduction_value
  FROM (
    SELECT data_point_signature, bank_group, sum_numeric_position, reference_date
    FROM fdscatalog.fds.flash_report_bankgroup_aggregates 
    -- Filter by Nominal value 
    WHERE Metrics = 'eba_met:mi357'
    --Filter by Central banks
    AND (bank_group = 'PBBK' OR bank_group = 'PBNB')
    --Filter by Pound Sterling 
    AND CUD = 'eba_CU:GBP' 
    --Filter by UNITED KINGDOM
    AND (RCP = 'eba_GA:GB' OR RCP = 'boe_eba_GA:x9001')
    --Filter by Liablities
    AND Base = 'eba_BA:x7' 
    -- Filter by Deposits [Statistics]
    AND main_category = 'boe_eba_MC:x9394'
    --Filter by  Other deposit instrument. Credit items in course of transmission to
    AND MCB = 'boe_eba_MC:x9390'
    --Filter by Monetary Financial Institutions 
    AND (CPS = 'boe_eba_CT:x9001' OR CPS = 'boe_eba_CT:x9163')
    AND tab_value = 'BT.01.01.01' AND reference_date = '2022-04-01T00:00:00' 
    --GROUP BY data_point_signature, bank_group, reference_date, sum_numeric_position
  ) AS total_CTBK_AB,
  (
    SELECT data_point_signature, bank_group, sum_numeric_position, reference_date
    FROM fdscatalog.fds.flash_report_bankgroup_aggregates 
    WHERE 
    -- Filter by Nominal value 
    Metrics = 'eba_met:mi357' 
    --Filter by rest of the bank other then CTBK
    AND bank_group = 'CTBK'
    --Filter by Pound Sterling  
    AND CUD = 'eba_CU:GBP' 
    --Filter by (Non UK resident)
    AND (RCP = 'eba_GA:GB' OR RCP = 'boe_eba_GA:x9001')
    --Filter by Liablities
    AND Base = 'eba_BA:x7'
     -- Filter by Deposits [Statistics]
    AND (main_category = 'boe_eba_MC:x9394' OR main_category = 'boe_eba_MC:x9039')
    --Filter by  Other deposit instrument. Credit items in course of transmission to
    AND (MCB = 'boe_eba_MC:x9390' or MCB is NULL)
    --Filter by Deposit-taking corporations and Central Monetary Institutions [counterparty's home country definition
    AND (CPS = 'boe_eba_CT:x9001' OR CPS = 'boe_eba_CT:x9163')
    AND (tab_value = 'BT.01.01.01' OR tab_value = 'PB.01.01.01')  AND reference_date = '2022-04-01T00:00:00' 
    --GROUP BY data_point_signature, bank_group, reference_date,sum_numeric_position
   ) AS total_PBBK_PBNB_AB
""")
