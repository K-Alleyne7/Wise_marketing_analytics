with customer_layer as (
select distinct customer_id,
industry,
extract(year from min(transaction_date)) as cohort_year,
min(transaction_date) as first_transaction_date,
max(transaction_date) as most_recent_transaction_date,
count(customer_id) as transactions,
sum(amount) as net_amount,
(sum(amount) / count(customer_id))::decimal(15,2) as net_aov
from wise_test.synthetic_data_042026
group by 1,2
)
,customer_status as (
select customer_id,
industry,
cohort_year::text as cohort_year,
first_transaction_date,
most_recent_transaction_date,
most_recent_transaction_date - first_transaction_date as active_days,
case
	when most_recent_transaction_date - first_transaction_date <= 30 then 'LT 1 month'
	when most_recent_transaction_date - first_transaction_date between 31 and 90 then '1-3 months'
	when most_recent_transaction_date - first_transaction_date between 91 and 180 then '3-6 months'
	when most_recent_transaction_date - first_transaction_date between 181 and 365 then '6-12 months'
	when most_recent_transaction_date - first_transaction_date between 366 and 545 then '12-18 months'
	when most_recent_transaction_date - first_transaction_date between 546 and 730 then '18-24 months'
	when most_recent_transaction_date - first_transaction_date >= 731 then 'GT 24 months' end as tenure_length_grouping,
(greatest(((most_recent_transaction_date - first_transaction_date) / 365.25),1))::decimal(15,2) as active_years,
to_date('2023-01-01','YYYY-MM-DD') - most_recent_transaction_date as days_since_last_order,
case when (to_date('2023-01-01','YYYY-MM-DD') - most_recent_transaction_date) >= 365 then 'Lapsed' else 'Active' end as customer_status,
transactions,
net_amount,
net_aov
from customer_layer
)

select 
case grouping(industry) when 1 then 'Total' else industry end as industry,
case grouping(cohort_year) when 1 then 'Total' else cohort_year end as cohort_year,
case grouping (tenure_length_grouping) when 1 then 'Total' else tenure_length_grouping end as tenure_length,
case grouping (customer_status) when 1 then 'Total' else customer_status end as customer_status,
count(distinct customer_id) as customers,
sum(net_amount) as net_amount,
sum(transactions) as transactions,
sum(transactions) / count(distinct customer_id)  as lifetime_frequency,
(sum(net_amount) / sum(transactions))::decimal(15,2) as net_aov,
(sum(net_amount) / count(distinct customer_id))::decimal(15,2) as net_value_per_customer,
(avg(active_days))::decimal(15,1) as average_active_days,
(avg(active_days) / 30)::decimal(15,1) as average_active_months
from customer_status
group by
cube(industry, cohort_year, tenure_length_grouping, customer_status)
order by 1,2,3,4
