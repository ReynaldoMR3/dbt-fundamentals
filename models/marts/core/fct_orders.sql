with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select * from {{ ref('stg_orders')}}
),


fact_orders as (
    select
          orders.order_id
        , orders.customer_id
        , sum(payments.amount) as amount
    from orders
    left join 
        payments 
    on
        orders.order_id = payments.orderid
    group by
        orders.order_id
        , orders.customer_id
)


select * from fact_orders