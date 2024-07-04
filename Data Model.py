# Databricks notebook source
# MAGIC %sql
# MAGIC --  CREATE TABLE fdscatalog.fds.aggregation (
# MAGIC --     id       INTEGER PRIMARY KEY,
# MAGIC --     cycle_id     INTEGER,
# MAGIC --     execution_time timestamp NOT NULL,
# MAGIC --     timeseries_value  double,
# MAGIC --     reporting_date timestamp,
# MAGIC --     aggregate_dps_name String
# MAGIC -- );
# MAGIC
# MAGIC --  CREATE TABLE fdscatalog.fds.bank (
# MAGIC --     bank_id       INTEGER PRIMARY KEY,
# MAGIC --     bank_name     STRING,
# MAGIC --     bank_group_id INTEGER,
# MAGIC --     bank_type_id  INTEGER
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE  fdscatalog.fds.bank_group (
# MAGIC --     bank_group_id INTEGER PRIMARY KEY,
# MAGIC --     bank_id INTEGER,
# MAGIC --     bank_group    STRING,
# MAGIC --    FOREIGN KEY (bank_id) REFERENCES fdscatalog.fds.bank (bank_id)
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE fdscatalog.fds.country (
# MAGIC --     country_id   INTEGER PRIMARY KEY,
# MAGIC --     country_group_id INTEGER ,
# MAGIC --     FOREIGN KEY (country_group_id) REFERENCES fdscatalog.fds.country_group,
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE fdscatalog.fds.country_group (
# MAGIC --     country_group_id INTEGER PRIMARY KEY,
# MAGIC --     country_group    STRING
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE fdscatalog.fds.country_code (
# MAGIC --     country_code_id INTEGER PRIMARY KEY,
# MAGIC --     country_group_id INTEGER,
# MAGIC --     country_code    STRING,
# MAGIC --     country_name String,
# MAGIC --     FOREIGN KEY (country_group_id) REFERENCES fdscatalog.fds.country_group(country_group_id)
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE fdscatalog.fds.factor (
# MAGIC --     factor_id INTEGER PRIMARY KEY,
# MAGIC --     factor_name    STRING,
# MAGIC --     factor_value   INTEGER,
# MAGIC --     factor_desc    STRING
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE fdscatalog.fds.form (
# MAGIC --     form_id          INTEGER PRIMARY KEY,
# MAGIC --     form             STRING NOT NULL,
# MAGIC --     form_description STRING
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE  fdscatalog.fds.submission (
# MAGIC --     submission_id        INTEGER PRIMARY KEY,
# MAGIC --     data_point_signature STRING,
# MAGIC --     reference_date       STRING,
# MAGIC --     currency_id          INTEGER,
# MAGIC --     country_id           INTEGER NOT NULL,
# MAGIC --     submission_period_id INTEGER ,
# MAGIC --     form_id              INTEGER NOT NULL,
# MAGIC --     form_name            STRING,
# MAGIC --     bank_id              INTEGER NOT NULL,
# MAGIC --     bank_name            STRING,
# MAGIC --     Metrics              STRING,
# MAGIC --     Base                 STRING,
# MAGIC --     CUD                  STRING,
# MAGIC --     main_category        STRING,
# MAGIC --     secondary_category   STRING,
# MAGIC --     CPS                  STRING,
# MAGIC --     RCP                  STRING,
# MAGIC --     RPR                  STRING,
# MAGIC --     MCB                  STRING,
# MAGIC --     timestamp            TIMESTAMP,
# MAGIC --     dimensions           STRING,
# MAGIC --     bank_group           STRING,
# MAGIC --     FOREIGN KEY (bank_id) REFERENCES fdscatalog.fds.bank (bank_id),
# MAGIC --     FOREIGN KEY (currency_id) REFERENCES fdscatalog.fds.currency (currency_id),
# MAGIC --     FOREIGN KEY (form_id) REFERENCES fdscatalog.fds.form (form_id),
# MAGIC --     FOREIGN KEY (submission_period_id) REFERENCES fdscatalog.fds.submission_period (submission_period_id),
# MAGIC --     FOREIGN KEY (country_id) REFERENCES fdscatalog.fds.country (country_id)
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE TABLE fdscatalog.fds.submission_value (
# MAGIC --     submission_value_id      INTEGER PRIMARY KEY,
# MAGIC --     numeric_position         DOUBLE,
# MAGIC --     x_axis_value             STRING,
# MAGIC --     y_axis_value             STRING,
# MAGIC --     submission_id            INTEGER NOT NULL,
# MAGIC --     submission_value_type_id INTEGER,
# MAGIC --     timestamp_sub                TIMESTAMP,
# MAGIC --     tab_value                STRING,
# MAGIC --     FOREIGN KEY (submission_id) REFERENCES fdscatalog.fds.submission (submission_id),
# MAGIC --     FOREIGN KEY (submission_value_type_id) REFERENCES fdscatalog.fds.submission_value_type (submission_value_type_id)
# MAGIC -- );

# COMMAND ----------

# Insert statement for bank table
insert_statement = '''
INSERT INTO fdscatalog.fds.bank (bank_id, bank_name, bank_group_id, bank_type_id)
VALUES (1, 'BANK1', 201),
       (2, 'BANK2', 202),
       (3, 'BANK3', 202),
       (4, 'BANK4', 203),
       (5, 'BANK5', 204),
       (6, 'BANK6', 205)
'''

# COMMAND ----------

# SQL insert statement for bank group
insert_statement = """
INSERT INTO fdscatalog.fds.bank_group (bank_group_id, bank_id, bank_group)
VALUES (201, 1, 'CTBK'),
       (202, 2,3, 'PBBK'),
       (203, 4, 'PBBS'),
       (204, 5, 'PBNB'),
       (205, 6, 'PBNS')
"""
