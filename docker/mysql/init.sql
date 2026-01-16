-- MySQL Initialization Script for Pipeline Testing
-- Creates tables and sample data for integration tests

-- Customers table for multi-source join testing
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(100),
    country VARCHAR(100),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loyalty_points INT DEFAULT 0
);

-- Transactions table for aggregation testing
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(50),
    transaction_status VARCHAR(50) DEFAULT 'completed',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Product_inventory table for validation testing
CREATE TABLE IF NOT EXISTS product_inventory (
    inventory_id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(100) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    quantity_available INT DEFAULT 0,
    warehouse_location VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Sales_metrics table for incremental processing
CREATE TABLE IF NOT EXISTS sales_metrics (
    metric_id INT AUTO_INCREMENT PRIMARY KEY,
    metric_date DATE NOT NULL,
    total_sales DECIMAL(15, 2) DEFAULT 0.00,
    total_orders INT DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0.00,
    new_customers INT DEFAULT 0,
    UNIQUE KEY (metric_date)
);

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, phone, address, city, country, loyalty_points) VALUES
    ('John', 'Smith', 'john.smith@example.com', '+1-555-0101', '123 Main St', 'New York', 'USA', 150),
    ('Emma', 'Johnson', 'emma.johnson@example.com', '+1-555-0102', '456 Oak Ave', 'Los Angeles', 'USA', 200),
    ('Michael', 'Brown', 'michael.brown@example.com', '+44-20-1234', '789 High St', 'London', 'UK', 100),
    ('Sophia', 'Davis', 'sophia.davis@example.com', '+61-2-5678', '321 George St', 'Sydney', 'Australia', 250),
    ('James', 'Wilson', 'james.wilson@example.com', '+1-555-0103', '654 Elm St', 'Chicago', 'USA', 50),
    ('Olivia', 'Martinez', 'olivia.martinez@example.com', '+34-91-1234', '987 Gran Via', 'Madrid', 'Spain', 300),
    ('William', 'Garcia', 'william.garcia@example.com', '+52-55-1234', '147 Reforma', 'Mexico City', 'Mexico', 75),
    ('Ava', 'Rodriguez', 'ava.rodriguez@example.com', '+1-555-0104', '258 Pine St', 'Houston', 'USA', 180),
    ('Liam', 'Anderson', 'liam.anderson@example.com', '+1-555-0105', '369 Maple Dr', 'Phoenix', 'USA', 120),
    ('Isabella', 'Taylor', 'isabella.taylor@example.com', '+44-20-5678', '741 Baker St', 'London', 'UK', 220)
ON DUPLICATE KEY UPDATE loyalty_points = loyalty_points;

-- Insert sample transactions
INSERT INTO transactions (customer_id, amount, payment_method, transaction_status) VALUES
    (1, 245.50, 'credit_card', 'completed'),
    (1, 89.99, 'paypal', 'completed'),
    (2, 567.25, 'credit_card', 'completed'),
    (2, 123.45, 'debit_card', 'completed'),
    (3, 899.99, 'credit_card', 'completed'),
    (4, 345.00, 'paypal', 'completed'),
    (5, 67.80, 'credit_card', 'completed'),
    (6, 1234.99, 'credit_card', 'completed'),
    (7, 456.00, 'debit_card', 'pending'),
    (8, 234.50, 'paypal', 'completed'),
    (9, 789.00, 'credit_card', 'completed'),
    (10, 543.75, 'credit_card', 'completed'),
    (1, 99.99, 'credit_card', 'refunded'),
    (3, 178.50, 'paypal', 'completed'),
    (5, 299.99, 'debit_card', 'completed')
ON DUPLICATE KEY UPDATE transaction_status = transaction_status;

-- Insert sample product inventory
INSERT INTO product_inventory (sku, product_name, quantity_available, warehouse_location) VALUES
    ('LAP-001', 'ThinkPad X1 Carbon', 25, 'Warehouse A'),
    ('MSE-001', 'Logitech MX Master 3', 150, 'Warehouse A'),
    ('KEY-001', 'Mechanical Gaming Keyboard', 75, 'Warehouse B'),
    ('MON-001', 'Dell UltraSharp 27"', 40, 'Warehouse C'),
    ('WEB-001', 'Logitech C920 HD Webcam', 60, 'Warehouse B'),
    ('HDP-001', 'Sony WH-1000XM5', 80, 'Warehouse A'),
    ('LMP-001', 'LED Desk Lamp', 200, 'Warehouse D'),
    ('CHR-001', 'Herman Miller Aeron', 15, 'Warehouse C'),
    ('USB-001', 'Anker USB-C Cable 6ft', 500, 'Warehouse D'),
    ('PWR-001', 'Anker PowerCore 20000', 120, 'Warehouse B'),
    ('CSE-001', 'iPhone 14 Pro Case', 300, 'Warehouse D'),
    ('SCR-001', 'Tempered Glass Screen Protector', 450, 'Warehouse D')
ON DUPLICATE KEY UPDATE quantity_available = VALUES(quantity_available);

-- Insert sample sales metrics
INSERT INTO sales_metrics (metric_date, total_sales, total_orders, avg_order_value, new_customers) VALUES
    (CURDATE() - INTERVAL 7 DAY, 5234.50, 15, 348.97, 3),
    (CURDATE() - INTERVAL 6 DAY, 6789.25, 18, 377.18, 2),
    (CURDATE() - INTERVAL 5 DAY, 4321.00, 12, 360.08, 1),
    (CURDATE() - INTERVAL 4 DAY, 7890.75, 21, 375.75, 4),
    (CURDATE() - INTERVAL 3 DAY, 5678.50, 16, 354.91, 2),
    (CURDATE() - INTERVAL 2 DAY, 8901.25, 23, 387.01, 5),
    (CURDATE() - INTERVAL 1 DAY, 6543.00, 19, 344.37, 3)
ON DUPLICATE KEY UPDATE
    total_sales = VALUES(total_sales),
    total_orders = VALUES(total_orders),
    avg_order_value = VALUES(avg_order_value),
    new_customers = VALUES(new_customers);

-- Create indexes for better performance
CREATE INDEX idx_customers_country ON customers(country);
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_transactions_customer_id ON transactions(customer_id);
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_transactions_status ON transactions(transaction_status);
CREATE INDEX idx_product_inventory_sku ON product_inventory(sku);
CREATE INDEX idx_sales_metrics_date ON sales_metrics(metric_date);

-- Display summary
SELECT
    (SELECT COUNT(*) FROM customers) AS total_customers,
    (SELECT COUNT(*) FROM transactions) AS total_transactions,
    (SELECT COUNT(*) FROM product_inventory) AS total_products,
    (SELECT COUNT(*) FROM sales_metrics) AS total_metrics;
