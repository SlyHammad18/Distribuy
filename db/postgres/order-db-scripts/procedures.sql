-- Order Service Stored Procedures

-- Procedure to create a new order with 2PC simulation
CREATE OR REPLACE FUNCTION create_order(
    p_user_id INTEGER,
    p_items JSONB,
    p_shipping_address TEXT,
    p_payment_method VARCHAR
)
RETURNS TABLE(success BOOLEAN, order_id INTEGER, message VARCHAR) AS $$
DECLARE
    v_order_id INTEGER;
    v_total_price DECIMAL := 0;
    v_item JSONB;
    v_product_id INTEGER;
    v_quantity INTEGER;
    v_unit_price DECIMAL;
    v_subtotal DECIMAL;
BEGIN
    IF p_user_id IS NULL OR p_user_id <= 0 THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Invalid user id'::VARCHAR;
        RETURN;
    END IF;

    IF p_items IS NULL OR jsonb_typeof(p_items) <> 'array' OR jsonb_array_length(p_items) = 0 THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Order items are required'::VARCHAR;
        RETURN;
    END IF;

    INSERT INTO orders (user_id, total_price, status, payment_method, shipping_address)
    VALUES (p_user_id, 0, 'PENDING', p_payment_method, p_shipping_address)
    RETURNING id INTO v_order_id;

    -- Phase 1: PREPARE - validate and stage resources
    INSERT INTO transactional_log (order_id, phase, status, services_involved)
    VALUES (v_order_id, 'PREPARE', 'INITIATED', jsonb_build_object('user_id', p_user_id));

    FOR v_item IN SELECT * FROM jsonb_array_elements(p_items)
    LOOP
        v_product_id := NULLIF(v_item->>'product_id', '')::INTEGER;
        v_quantity := NULLIF(v_item->>'quantity', '')::INTEGER;
        v_unit_price := NULLIF(v_item->>'unit_price', '')::DECIMAL;
        v_subtotal := NULLIF(v_item->>'subtotal', '')::DECIMAL;

        IF v_product_id IS NULL OR v_quantity IS NULL OR v_quantity <= 0 OR v_unit_price IS NULL OR v_subtotal IS NULL THEN
            RAISE EXCEPTION 'Invalid order item payload: %', v_item::TEXT;
        END IF;

        v_total_price := v_total_price + v_subtotal;

        INSERT INTO order_items (
            order_id, product_id, product_name, quantity,
            unit_price, subtotal
        )
        VALUES (
            v_order_id,
            v_product_id,
            v_item->>'product_name',
            v_quantity,
            v_unit_price,
            v_subtotal
        );
    END LOOP;

    UPDATE orders SET total_price = v_total_price WHERE id = v_order_id;

    INSERT INTO transactional_log (order_id, phase, status, services_involved)
    VALUES (v_order_id, 'COMMIT', 'SUCCESS', jsonb_build_object('items_count', jsonb_array_length(p_items)));

    RETURN QUERY SELECT TRUE, v_order_id, 'Order created successfully'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    INSERT INTO transactional_log (order_id, phase, status, services_involved)
    VALUES (COALESCE(v_order_id, 0), 'ROLLBACK', 'FAILED', jsonb_build_object('error', SQLERRM::text));

    RETURN QUERY SELECT FALSE, 0::INTEGER, ('Error creating order: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get order details
CREATE OR REPLACE FUNCTION get_order_details(p_order_id INTEGER)
RETURNS TABLE(
    order_id INTEGER,
    user_id INTEGER,
    total_price DECIMAL,
    status VARCHAR,
    payment_method VARCHAR,
    created_at TIMESTAMP,
    item_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.id,
        o.user_id,
        o.total_price,
        o.status,
        o.payment_method,
        o.created_at,
        COUNT(oi.id)::INTEGER
    FROM orders o
    LEFT JOIN order_items oi ON o.id = oi.order_id
    WHERE o.id = p_order_id
    GROUP BY o.id, o.user_id, o.total_price, o.status, o.payment_method, o.created_at;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get all orders for a user
CREATE OR REPLACE FUNCTION get_user_orders(p_user_id INTEGER, p_limit INTEGER DEFAULT 50, p_offset INTEGER DEFAULT 0)
RETURNS TABLE(
    order_id INTEGER,
    total_price DECIMAL,
    status VARCHAR,
    created_at TIMESTAMP,
    item_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.id,
        o.total_price,
        o.status,
        o.created_at,
        COUNT(oi.id)::INTEGER
    FROM orders o
    LEFT JOIN order_items oi ON o.id = oi.order_id
    WHERE o.user_id = p_user_id
    GROUP BY o.id, o.total_price, o.status, o.created_at
    ORDER BY o.created_at DESC
    LIMIT p_limit OFFSET p_offset;
END;
$$ LANGUAGE plpgsql;

-- Procedure to update order status
CREATE OR REPLACE FUNCTION update_order_status(
    p_order_id INTEGER,
    p_new_status VARCHAR,
    p_reason TEXT DEFAULT NULL
)
RETURNS TABLE(success BOOLEAN, message VARCHAR) AS $$
DECLARE
    v_old_status VARCHAR;
    v_valid_statuses VARCHAR[] := ARRAY['PENDING', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'FAILED'];
BEGIN
    IF p_new_status IS NULL OR btrim(p_new_status) = '' THEN
        RETURN QUERY SELECT FALSE, 'New status is required'::VARCHAR;
        RETURN;
    END IF;

    IF NOT (p_new_status = ANY(v_valid_statuses)) THEN
        RETURN QUERY SELECT FALSE, ('Invalid status: ' || p_new_status)::VARCHAR;
        RETURN;
    END IF;

    SELECT status INTO v_old_status FROM orders WHERE id = p_order_id;

    IF v_old_status IS NULL THEN
        RETURN QUERY SELECT FALSE, 'Order not found'::VARCHAR;
        RETURN;
    END IF;

    IF v_old_status = p_new_status THEN
        RETURN QUERY SELECT TRUE, 'Order status unchanged'::VARCHAR;
        RETURN;
    END IF;

    UPDATE orders
    SET status = p_new_status
    WHERE id = p_order_id;

    INSERT INTO order_status_history (order_id, old_status, new_status, reason)
    VALUES (p_order_id, v_old_status, p_new_status, p_reason);

    RETURN QUERY SELECT TRUE, 'Order status updated successfully'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, ('Error updating order status: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get order status history
CREATE OR REPLACE FUNCTION get_order_history(p_order_id INTEGER)
RETURNS TABLE(
    old_status VARCHAR,
    new_status VARCHAR,
    changed_at TIMESTAMP,
    reason TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT osh.old_status, osh.new_status, osh.changed_at, osh.reason
    FROM order_status_history osh
    WHERE osh.order_id = p_order_id
    ORDER BY osh.changed_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Procedure to simulate 2PC failure scenario
CREATE OR REPLACE FUNCTION simulate_2pc_failure(p_order_id INTEGER)
RETURNS TABLE(success BOOLEAN, message VARCHAR) AS $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS(SELECT 1 FROM orders WHERE id = p_order_id) INTO v_exists;
    IF NOT v_exists THEN
        RETURN QUERY SELECT FALSE, 'Order not found'::VARCHAR;
        RETURN;
    END IF;

    INSERT INTO transactional_log (order_id, phase, status, services_involved)
    VALUES (p_order_id, 'ROLLBACK', 'SIMULATED_FAILURE', jsonb_build_object('error', 'Simulated 2PC failure'));

    UPDATE orders SET status = 'FAILED' WHERE id = p_order_id;

    RETURN QUERY SELECT TRUE, 'Simulated 2PC failure for debugging'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, ('Error in failure simulation: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Trigger helper function refresh (non-destructive deployment)
CREATE OR REPLACE FUNCTION log_order_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO order_status_history (order_id, old_status, new_status, changed_by, changed_at, reason)
        VALUES (NEW.id, OLD.status, NEW.status, current_user::VARCHAR, CURRENT_TIMESTAMP, 'STATUS_TRIGGER');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;
