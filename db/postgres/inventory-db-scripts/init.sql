-- Inventory Service Database Initialization

DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS inventory_transactions CASCADE;

-- Create Inventory Table
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL UNIQUE,
    product_name VARCHAR(255),
    stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    warehouse_location VARCHAR(255),
    reorder_level INTEGER DEFAULT 10,
    last_restock_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Inventory Transactions Log
CREATE TABLE inventory_transactions (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    transaction_type VARCHAR(50) NOT NULL CHECK (transaction_type IN ('RESTOCK', 'SALE', 'RETURN', 'ADJUSTMENT', 'RESERVED', 'UNRESERVED')),
    quantity_changed INTEGER NOT NULL,
    old_quantity INTEGER,
    new_quantity INTEGER,
    reference_id VARCHAR(255),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255)
);

-- Indexes for performance
CREATE INDEX idx_inventory_product_id ON inventory(product_id);
CREATE INDEX idx_inventory_stock_quantity ON inventory(stock_quantity);
CREATE INDEX idx_inventory_reorder_level ON inventory(stock_quantity) WHERE stock_quantity <= reorder_level;
CREATE INDEX idx_transactions_product_id ON inventory_transactions(product_id);
CREATE INDEX idx_transactions_type ON inventory_transactions(transaction_type);
CREATE INDEX idx_transactions_created_at ON inventory_transactions(created_at DESC);

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_inventory_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_inventory_updated_at
BEFORE UPDATE ON inventory
FOR EACH ROW
EXECUTE FUNCTION update_inventory_updated_at();

-- Trigger to prevent negative stock (additional validation)
CREATE OR REPLACE FUNCTION prevent_negative_stock()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.stock_quantity < 0 THEN
        RAISE EXCEPTION 'Stock quantity cannot be negative. Current: %, Attempted: %', OLD.stock_quantity, NEW.stock_quantity;
    END IF;

    IF NEW.reserved_quantity < 0 THEN
        RAISE EXCEPTION 'Reserved quantity cannot be negative. Current: %, Attempted: %', OLD.reserved_quantity, NEW.reserved_quantity;
    END IF;

    IF NEW.reserved_quantity > NEW.stock_quantity THEN
        RAISE EXCEPTION 'Reserved quantity (%) cannot exceed stock quantity (%) for product %', NEW.reserved_quantity, NEW.stock_quantity, NEW.product_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_prevent_negative_stock
BEFORE UPDATE ON inventory
FOR EACH ROW
EXECUTE FUNCTION prevent_negative_stock();

-- Trigger to check reorder level
CREATE OR REPLACE FUNCTION check_reorder_level()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.stock_quantity <= NEW.reorder_level AND OLD.stock_quantity > NEW.reorder_level THEN
        INSERT INTO inventory_transactions (product_id, transaction_type, quantity_changed, old_quantity, new_quantity, notes)
        VALUES (NEW.product_id, 'ADJUSTMENT', 0, OLD.stock_quantity, NEW.stock_quantity, 'REORDER_LEVEL_ALERT');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_check_reorder_level
AFTER UPDATE ON inventory
FOR EACH ROW
EXECUTE FUNCTION check_reorder_level();

-- Seed data
INSERT INTO inventory (product_id, product_name, stock_quantity, warehouse_location, reorder_level)
VALUES
    (1, 'Wireless Headphones', 50, 'Shelf A1', 5),
    (2, 'USB-C Cable', 200, 'Shelf B2', 20),
    (3, 'Laptop Stand', 25, 'Shelf C3', 5),
    (4, 'Mechanical Keyboard', 15, 'Shelf A4', 3),
    (5, 'Wireless Mouse', 100, 'Shelf B5', 10);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;
