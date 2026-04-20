select
to_char(first_transaction_date, 'Month') as transaction_month_name,
count(customer_id) as transactions
from (select distinct customer_id,
industry,
extract(year from min(transaction_date)) as cohort_year,
min(transaction_date) as first_transaction_date,
max(transaction_date) as most_recent_transaction_date,
count(customer_id) as transactions,
sum(amount) as net_amount,
(sum(amount) / count(customer_id))::decimal(15,2) as net_aov
from wise_test.synthetic_data_042026
group by 1,2)
group by 1
