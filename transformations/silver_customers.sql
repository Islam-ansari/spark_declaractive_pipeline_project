-- Please edit the sample below
-- this table will follow SCD 1 type - only latest data will be available (UPSERT)
create or refresh streaming table silver.customers;

create flow silver_insert
as auto cdc into silver.customers
from stream (silver.customers_clean)
keys (customer_id)
sequence by created_date
stored as scd type 1;