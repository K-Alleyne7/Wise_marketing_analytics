CREATE SCHEMA wise_test;

CREATE TABLE wise_test.synthetic_data_042026 
(customer_id text,
transaction_date date,
amount	decimal(15,2),
industry text);

SET datestyle = 'ISO, DMY';
COPY wise_test.synthetic_data_042026  (
customer_id,
transaction_date,
amount,
industry
) 
FROM '/Downloads/Wise synthetic data.csv' 
DELIMITER ',' CSV HEADER;
