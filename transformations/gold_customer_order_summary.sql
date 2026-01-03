use schema gold;

create or refresh materialized view gold_customer_order_summary
as
select 
      c.customer_id,
      c.customer_name,
      c.date_of_birth,
      c.telephone,
      c.email,
      a.address_line_1,
      a.city,
      a.state,
      a.postcode,
      count(distinct(o.order_id)) as total_orders,
      sum(o.quantity) as total_items_ordered,
      sum(o.price * o.quantity) as total_order_amount
from silver.customers c
join silver.addresses a on c.customer_id = a.customer_id
join silver.orders o on o.customer_id = c.customer_id
where a.`__END_AT` is null
group by all;