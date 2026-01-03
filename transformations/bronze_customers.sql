 create or refresh streaming table bronze.customers
 as select * 
          , _metadata.file_path as input_file_path
          , current_timestamp() as ingestion_timestamp
 from cloud_files (
  '/Volumes/circuitbox/landing/operational_data/customers',
  'json',
  map('cloudFiles.inferColumnTypes','true')
 );
