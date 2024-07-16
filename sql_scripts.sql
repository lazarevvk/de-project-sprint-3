

alter table mart.f_sales
add column status text;

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select dc.date_id, item_id, customer_id, city_id, quantity,
case when status = 'refunded' then -payment_amount else payment_amount end as payment_amount, 
status from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual;



drop table if exists mart.f_customer_retention;
CREATE TABLE mart.f_customer_retention (
	id serial4 NOT NULL,
	new_customers_count int8 NOT NULL,
	item_id int8 not null,
	returning_customers_count int8 NOT NULL,
	refunded_customers_count int8 NOT NULL,
	period_name text NOT NULL,
	period_id int8 NULL,
	new_customers_revenue numeric(10, 2) NULL,
	returning_customers_revenue numeric(10, 2) NULL,
	customers_refunded numeric(10, 2) NULL,
	CONSTRAINT f_customer_retention_pkey PRIMARY KEY (id)
);

INSERT INTO mart.f_customer_retention (
    period_name,
    period_id,
    item_id,
    new_customers_count,
    new_customers_revenue,
    returning_customers_count,
    returning_customers_revenue,
    customers_refunded,
    refunded_customers_count
)
select distinct
    'weekly' AS period_name,
    c.week_of_year AS period_id,
    co.item_id,
    COUNT(DISTINCT CASE WHEN co.order_count = 1 THEN co.customer_id END) AS new_customers_count,
    SUM(CASE WHEN co.order_count = 1 THEN co.total_revenue END) AS new_customers_revenue,
    COUNT(DISTINCT CASE WHEN co.order_count > 1 THEN co.customer_id END) AS returning_customers_count,
    SUM(CASE WHEN co.order_count > 1 THEN co.total_revenue END) AS returning_customers_revenue,
    SUM(co.refunded_orders_count) AS customers_refunded,
    COUNT(DISTINCT CASE WHEN co.refunded_orders_count > 0 THEN co.customer_id END) AS refunded_customers_count
FROM (
    select Distinct
        ol.customer_id,
        ol.item_id,
        c.week_of_year,
        COUNT(DISTINCT ol.uniq_id) AS order_count,
        COUNT(DISTINCT CASE WHEN ol.status = 'refunded' THEN ol.uniq_id END) AS refunded_orders_count,
        SUM(ol.payment_amount) AS total_revenue
    FROM staging.user_order_log ol
    JOIN mart.d_calendar c ON c.date_actual = ol.date_time::date
    GROUP BY ol.customer_id, ol.item_id, c.week_of_year
) co
JOIN mart.d_calendar c ON c.week_of_year = co.week_of_year
GROUP BY c.first_day_of_week, c.week_of_year, co.item_id;

