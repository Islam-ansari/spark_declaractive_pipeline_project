-- create a streaming table in bronze layer - which will load data incrementally

use schema bronze;

create or refresh streaming table orders
as select *,
          _metadata.file_path as input_file_path,
          current_timestamp() as ingestion_timestamp
  from cloud_files(
    '/Volumes/circuitbox/landing/operational_data/orders/',
    'json',
    map('cloudFiles.inferColumnTypes','true')
  );

-- create orders_clean table in silver layer
use schema silver;

create or refresh streaming table silver.orders_clean(
  constraint customer_id expect (customer_id is not null) on violation fail update,
  constraint order_id expect (order_id is not null) on violation fail update,
  constraint order_status expect (order_status in ('Pending','Completed','Shipped','Cancelled')),
  constraint payment_method expect (payment_method in ('PayPal','Bank Transfer','Credit Card'))
  )
as select customer_id,
          items,
          order_id,
          order_status,
          cast(order_timestamp as timestamp) as order_timestamp,
          payment_method
from stream(bronze.orders);


-- final orders table in silver layer 
use schema silver;

create or refresh streaming table silver.orders
as select order_id,
         customer_id,
         order_timestamp,
         payment_method,
         order_status,
         item.category as category,
         item.item_id as item_id,
         item.name as name,
         item.price as price,
         item.quantity as quantity
from (
  select order_id,
         customer_id,
         order_timestamp,
         payment_method,
         order_status,
         explode(items) as item
        from stream(silver.orders_clean)
);

