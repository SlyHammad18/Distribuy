-- Inventory Service Stored Procedures

-- Procedure to check stock availability (with row-level locking for concurrency)
CREATE OR REPLACE FUNCTION check_and_reserve_stock(
    p_product_id INTEGER,
    p_quantity INTEGER
)
RETURNS TABLE(available BOOLEAN, current_stock INTEGER, message VARCHAR) AS $$
DECLARE
    v_current_stock INTEGER;
    v_available_after_reserve INTEGER;
BEGIN
    IF p_product_id IS NULL OR p_product_id <= 0 OR p_quantity IS NULL OR p_quantity <= 0 THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Invalid product or quantity'::VARCHAR;
        RETURN;
    END IF;

    -- SELECT FOR UPDATE provides row-level locking
    SELECT stock_quantity INTO v_current_stock
    FROM inventory
    WHERE product_id = p_product_id
    FOR UPDATE;

    IF v_current_stock IS NULL THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Product not found in inventory'::VARCHAR;
        RETURN;
    END IF;

    v_available_after_reserve := v_current_stock - p_quantity;

    IF v_available_after_reserve >= 0 THEN
        -- Reserve the stock
        UPDATE inventory
        SET reserved_quantity = reserved_quantity + p_quantity
        WHERE product_id = p_product_id;

        -- Log transaction
        INSERT INTO inventory_transactions (product_id, transaction_type, quantity_changed, old_quantity, new_quantity, reference_id)
        VALUES (p_product_id, 'RESERVED', p_quantity, v_current_stock, v_current_stock - p_quantity, 'RESERVE');

        RETURN QUERY SELECT TRUE, v_available_after_reserve, 'Stock reserved successfully'::VARCHAR;
    ELSE
        RETURN QUERY SELECT FALSE, v_current_stock, 'Insufficient stock available'::VARCHAR;
    END IF;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, COALESCE(v_current_stock, 0)::INTEGER, ('Error checking stock: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to deduct stock (confirm sale)
CREATE OR REPLACE FUNCTION deduct_stock(
    p_product_id INTEGER,
    p_quantity INTEGER,
    p_order_id VARCHAR DEFAULT NULL
)
RETURNS TABLE(success BOOLEAN, remaining_stock INTEGER, message VARCHAR) AS $$
DECLARE
    v_current_stock INTEGER;
    v_reserved INTEGER;
BEGIN
    IF p_product_id IS NULL OR p_product_id <= 0 OR p_quantity IS NULL OR p_quantity <= 0 THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Invalid product or quantity'::VARCHAR;
        RETURN;
    END IF;

    SELECT stock_quantity, reserved_quantity INTO v_current_stock, v_reserved
    FROM inventory
    WHERE product_id = p_product_id
    FOR UPDATE;

    IF v_current_stock IS NULL THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Product not found'::VARCHAR;
        RETURN;
    END IF;

    IF v_current_stock < p_quantity THEN
        RETURN QUERY SELECT FALSE, v_current_stock, 'Insufficient stock to complete sale'::VARCHAR;
        RETURN;
    END IF;

    UPDATE inventory
    SET
        stock_quantity = stock_quantity - p_quantity,
        reserved_quantity = GREATEST(0, reserved_quantity - p_quantity)
    WHERE product_id = p_product_id;

    INSERT INTO inventory_transactions (product_id, transaction_type, quantity_changed, old_quantity, new_quantity, reference_id, created_by)
    VALUES (p_product_id, 'SALE', -p_quantity, v_current_stock, v_current_stock - p_quantity, p_order_id, 'SYSTEM');

    IF (v_current_stock - p_quantity) <= 10 THEN
        INSERT INTO inventory_transactions (product_id, transaction_type, quantity_changed, old_quantity, new_quantity, notes)
        VALUES (p_product_id, 'ADJUSTMENT', 0, v_current_stock, v_current_stock - p_quantity, 'LOW_STOCK_WARNING');
    END IF;

    RETURN QUERY SELECT TRUE, (v_current_stock - p_quantity), 'Stock deducted successfully'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, COALESCE(v_current_stock, 0)::INTEGER, ('Error deducting stock: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get current inventory
CREATE OR REPLACE FUNCTION get_inventory_status(p_product_id INTEGER DEFAULT NULL)
RETURNS TABLE(
    product_id INTEGER,
    product_name VARCHAR,
    stock_quantity INTEGER,
    reserved_quantity INTEGER,
    available_quantity INTEGER,
    warehouse_location VARCHAR,
    reorder_level INTEGER,
    last_restock_date TIMESTAMP,
    updated_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        i.product_id,
        i.product_name,
        i.stock_quantity,
        i.reserved_quantity,
        (i.stock_quantity - i.reserved_quantity)::INTEGER,
        i.warehouse_location,
        i.reorder_level,
        i.last_restock_date,
        i.updated_at
    FROM inventory i
    WHERE (p_product_id IS NULL OR i.product_id = p_product_id)
    ORDER BY i.product_id;
END;
$$ LANGUAGE plpgsql;

-- Procedure to restock inventory
CREATE OR REPLACE FUNCTION restock_inventory(
    p_product_id INTEGER,
    p_quantity INTEGER,
    p_notes TEXT DEFAULT NULL
)
RETURNS TABLE(success BOOLEAN, new_stock INTEGER, message VARCHAR) AS $$
DECLARE
    v_old_stock INTEGER;
    v_new_stock INTEGER;
BEGIN
    IF p_product_id IS NULL OR p_product_id <= 0 OR p_quantity IS NULL OR p_quantity <= 0 THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Invalid product or quantity'::VARCHAR;
        RETURN;
    END IF;

    SELECT stock_quantity INTO v_old_stock
    FROM inventory
    WHERE product_id = p_product_id
    FOR UPDATE;

    IF v_old_stock IS NULL THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Product not found'::VARCHAR;
        RETURN;
    END IF;

    UPDATE inventory
    SET
        stock_quantity = stock_quantity + p_quantity,
        last_restock_date = CURRENT_TIMESTAMP
    WHERE product_id = p_product_id
    RETURNING stock_quantity INTO v_new_stock;

    INSERT INTO inventory_transactions (product_id, transaction_type, quantity_changed, old_quantity, new_quantity, notes, created_by)
    VALUES (p_product_id, 'RESTOCK', p_quantity, v_old_stock, v_new_stock, p_notes, 'ADMIN');

    RETURN QUERY SELECT TRUE, v_new_stock, 'Stock replenished successfully'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, COALESCE(v_old_stock, 0)::INTEGER, ('Error restocking: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to unreserve stock (if order cancelled)
CREATE OR REPLACE FUNCTION unreserve_stock(p_product_id INTEGER, p_quantity INTEGER)
RETURNS TABLE(success BOOLEAN, message VARCHAR) AS $$
DECLARE
    v_rows_updated INTEGER;
BEGIN
    IF p_product_id IS NULL OR p_product_id <= 0 OR p_quantity IS NULL OR p_quantity <= 0 THEN
        RETURN QUERY SELECT FALSE, 'Invalid product or quantity'::VARCHAR;
        RETURN;
    END IF;

    UPDATE inventory
    SET reserved_quantity = GREATEST(0, reserved_quantity - p_quantity)
    WHERE product_id = p_product_id;

    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
    IF v_rows_updated = 0 THEN
        RETURN QUERY SELECT FALSE, 'Product not found'::VARCHAR;
        RETURN;
    END IF;

    INSERT INTO inventory_transactions (product_id, transaction_type, quantity_changed, notes)
    VALUES (p_product_id, 'UNRESERVED', -p_quantity, 'Order cancelled - stock unreserved');

    RETURN QUERY SELECT TRUE, 'Stock unreserved successfully'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, ('Error unreserving stock: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get low stock items
CREATE OR REPLACE FUNCTION get_low_stock_items()
RETURNS TABLE(
    product_id INTEGER,
    product_name VARCHAR,
    stock_quantity INTEGER,
    reorder_level INTEGER,
    status VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        i.product_id,
        i.product_name,
        i.stock_quantity,
        i.reorder_level,
        CASE
            WHEN i.stock_quantity = 0 THEN 'OUT_OF_STOCK'
            WHEN i.stock_quantity <= i.reorder_level THEN 'LOW_STOCK'
            ELSE 'ADEQUATE'
        END::VARCHAR
    FROM inventory i
    WHERE i.stock_quantity <= i.reorder_level
    ORDER BY i.stock_quantity ASC;
END;
$$ LANGUAGE plpgsql;

-- Trigger helper function refresh (non-destructive deployment)
CREATE OR REPLACE FUNCTION update_inventory_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;
