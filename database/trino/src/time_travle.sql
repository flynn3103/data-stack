CREATE TABLE lakehouse.demo.customers (
    customer_id BIGINT,
    name VARCHAR,
    email VARCHAR,
    signup_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/customers/'
);


INSERT INTO lakehouse.demo.customers (customer_id, name, email, signup_date) VALUES
(1, 'Alice', 'alice@example.com', TIMESTAMP '2023-01-15 08:45:00'),
(2, 'Bob', 'bob@example.com', TIMESTAMP '2023-02-10 12:00:00');

ALTER TABLE lakehouse.demo.customers ADD COLUMN phone VARCHAR;


INSERT INTO lakehouse.demo.customers (customer_id, name, email, signup_date, phone) VALUES
(3, 'Charlie', 'charlie@example.com', TIMESTAMP '2023-03-20 09:30:00', '555-1234'),
(4, 'Diana', 'diana@example.com', TIMESTAMP '2023-04-22 15:10:00', '555-5678');


select * from lakehouse.demo.customers;

ALTER TABLE lakehouse.demo.customers RENAME COLUMN phone TO phone_number;

-- TIME TRAVEL

SELECT * FROM lakehouse.demo."customers$snapshots";

SELECT * FROM lakehouse.demo.customers for version as of 7857094153242422747;

SELECT * FROM lakehouse.demo.customers FOR TIMESTAMP AS OF TIMESTAMP '2024-10-05 14:30:46.973 +0700';




