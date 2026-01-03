-- creating streaming table silver.customer_clean
create or refresh streaming table silver.customers_clean(
    constraint customer_id expect (customer_id is not null) on violation fail update,
    constraint customer_name expect (customer_name is not null) on violation drop row,
    constraint telephone expect (length(telephone) >= 10),
    constraint email expect (email is not null),
    constraint date_of_birth expect (date_of_birth >= date('1920-01-01'))
    )
as select customer_id,
          customer_name,
          cast(date_of_birth as date) as date_of_birth,
          email,
          telephone,
          cast(created_date as date) as created_date

from stream(bronze.customers);

