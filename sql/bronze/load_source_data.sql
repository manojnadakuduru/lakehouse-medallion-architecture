-- Initial Load
INSERT INTO datamodeling.default.source_data VALUES
(1001, '2024-07-01', 1, 'Alice Johnson', 'alice@gmail.com', 501, 'iPhone 14', 'Electronics', 1, 999.99, 'Credit Card', 'USA', '2024-07-01'),
(1002, '2024-07-01', 2, 'Bob Smith', 'bob@yahoo.com', 502, 'AirPods Pro', 'Electronics', 2, 199.99, 'PayPal', 'USA', '2024-07-01'),
(1003, '2024-07-01', 3, 'Charlie Brown', 'charlie@outlook.com', 503, 'Nike Shoes', 'Footwear', 1, 129.99, 'Credit Card', 'Canada', '2024-07-01');

-- Incremental Load
INSERT INTO datamodeling.default.source_data VALUES
(1004, '2024-07-02', 4, 'David Lee', 'david@abc.com', 504, 'Samsung S23', 'Electronics', 1, 899.99, 'Credit Card', 'USA', '2024-07-02'),
(1005, '2024-07-02', 1, 'Alice Johnson', 'alice@gmail.com', 503, 'Nike Shoes', 'Footwear', 2, 129.99, 'Credit Card', 'USA', '2024-07-02');
