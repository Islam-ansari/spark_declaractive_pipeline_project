# create a streaming addresses table in bronze layer that will incrementally read data from volume
from pyspark import pipelines as dp
from pyspark.sql import functions as F

spark.sql('USE SCHEMA bronze')

@dp.table(
    name = 'addresses',
    comment = 'addresses data on bronze layer'
)
# it will create streaming_table materialized view etc based on the return value - no need to define
def create_bronze_addresses():

    return (
        spark.readStream
                        .format('cloudFiles')
                        .option('cloudFiles.format','csv')
                        .option('cloudFiles.inferColumnTypes','true')
                        .load('/Volumes/circuitbox/landing/operational_data/addresses/')
                        .select('*')
                        .withColumn('input_file_path',F.col('_metadata.file_path'))
                        .withColumn('ingestion_timestamp', F.current_timestamp())
    )


# Implementing silver.addresses_clean

spark.sql("USE SCHEMA silver")

@dp.table(
    name = "addresses_clean",
    comment="clean addresses table in silver layer"
)

@dp.expect('valid_postcode',F.length('postcode')==5)
@dp.expect_or_fail('valid_customer_id',F.col('customer_id').isNotNull())
@dp.expect_or_drop('valid_address', 'address_line_1 is not null')

def create_silver_addresses_clean():
    return (
        spark.readStream.table("bronze.addresses")
             .select("customer_id",
                     "address_line_1",
                     "city",
                     "state",
                     "postcode",
                     "created_date")
    )


spark.sql("USE SCHEMA silver")

dp.create_streaming_table(
    name="addresses",
    comment='addresses table at silver layer'
)

dp.create_auto_cdc_flow(
    target="addresses",
    source="addresses_clean",
    keys = ["customer_id"],
    sequence_by="created_date",
    stored_as_scd_type=2
)