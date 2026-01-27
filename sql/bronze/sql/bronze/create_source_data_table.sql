CREATE TABLE IF NOT EXISTS datamodeling.default.source_data (
  order_id INT PRIMARY KEY,
  order_date DATE,
  customer_id INT,
  customer_name VARCHAR(100),
  customer_email VARCHAR(100),
  product_id INT,
  product_name VARCHAR(100),
  product_category VARCHAR(50),
  quantity INT,
  unit_price DECIMAL(10, 2),
  payment_type VARCHAR(50),
  country VARCHAR(50),
  last_updated DATE
);
