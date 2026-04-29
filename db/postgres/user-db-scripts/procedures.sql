-- User Service Stored Procedures

-- Procedure to register a new user
CREATE OR REPLACE FUNCTION register_user(
    p_name VARCHAR,
    p_email VARCHAR,
    p_password VARCHAR,
    p_phone VARCHAR DEFAULT NULL
)
RETURNS TABLE(success BOOLEAN, user_id INTEGER, message VARCHAR) AS $$
DECLARE
    v_user_id INTEGER;
BEGIN
    IF p_name IS NULL OR btrim(p_name) = '' THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Name is required'::VARCHAR;
        RETURN;
    END IF;

    IF p_email IS NULL OR btrim(p_email) = '' THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Email is required'::VARCHAR;
        RETURN;
    END IF;

    IF p_password IS NULL OR btrim(p_password) = '' THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Password is required'::VARCHAR;
        RETURN;
    END IF;

    IF EXISTS(SELECT 1 FROM users WHERE email = p_email) THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Email already registered'::VARCHAR;
        RETURN;
    END IF;

    INSERT INTO users (name, email, password, phone, is_active)
    VALUES (p_name, p_email, p_password, p_phone, TRUE)
    RETURNING id INTO v_user_id;

    RETURN QUERY SELECT TRUE, v_user_id, 'User registered successfully'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, 0::INTEGER, ('Error registering user: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to authenticate user
CREATE OR REPLACE FUNCTION authenticate_user(
    p_email VARCHAR,
    p_password VARCHAR
)
RETURNS TABLE(success BOOLEAN, user_id INTEGER, message VARCHAR) AS $$
DECLARE
    v_user_id INTEGER;
    v_password VARCHAR;
BEGIN
    IF p_email IS NULL OR btrim(p_email) = '' OR p_password IS NULL THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Email and password are required'::VARCHAR;
        RETURN;
    END IF;

    SELECT id, password INTO v_user_id, v_password
    FROM users
    WHERE email = p_email AND is_active = TRUE;

    IF v_user_id IS NULL THEN
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'User not found or inactive'::VARCHAR;
    ELSIF v_password = p_password THEN
        -- Update last login timestamp
        UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = v_user_id;
        RETURN QUERY SELECT TRUE, v_user_id, 'Authentication successful'::VARCHAR;
    ELSE
        RETURN QUERY SELECT FALSE, 0::INTEGER, 'Invalid password'::VARCHAR;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get user profile
CREATE OR REPLACE FUNCTION get_user_profile(p_user_id INTEGER)
RETURNS TABLE(
    user_id INTEGER,
    name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address TEXT,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    country VARCHAR,
    is_admin BOOLEAN,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    last_login TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        id, name, email, phone, address, city, state, zip_code, country, is_admin, is_active, created_at, last_login
    FROM users
    WHERE id = p_user_id AND is_active = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Procedure to update user profile
CREATE OR REPLACE FUNCTION update_user_profile(
    p_user_id INTEGER,
    p_name VARCHAR,
    p_phone VARCHAR,
    p_address TEXT,
    p_city VARCHAR,
    p_state VARCHAR,
    p_zip_code VARCHAR,
    p_country VARCHAR
)
RETURNS TABLE(success BOOLEAN, message VARCHAR) AS $$
DECLARE
    v_rows_updated INTEGER;
BEGIN
    UPDATE users
    SET
        name = COALESCE(p_name, name),
        phone = COALESCE(p_phone, phone),
        address = COALESCE(p_address, address),
        city = COALESCE(p_city, city),
        state = COALESCE(p_state, state),
        zip_code = COALESCE(p_zip_code, zip_code),
        country = COALESCE(p_country, country)
    WHERE id = p_user_id;

    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
    IF v_rows_updated = 0 THEN
        RETURN QUERY SELECT FALSE, 'User not found'::VARCHAR;
        RETURN;
    END IF;

    RETURN QUERY SELECT TRUE, 'Profile updated successfully'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, ('Error updating profile: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Procedure to get all users (admin only)
CREATE OR REPLACE FUNCTION get_all_users(p_limit INTEGER DEFAULT 100, p_offset INTEGER DEFAULT 0)
RETURNS TABLE(
    user_id INTEGER,
    name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    is_admin BOOLEAN,
    is_active BOOLEAN,
    created_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT id, name, email, phone, is_admin, is_active, created_at
    FROM users
    ORDER BY created_at DESC
    LIMIT p_limit OFFSET p_offset;
END;
$$ LANGUAGE plpgsql;

-- Procedure to toggle user active status (admin only)
CREATE OR REPLACE FUNCTION toggle_user_status(p_user_id INTEGER)
RETURNS TABLE(success BOOLEAN, new_status BOOLEAN, message VARCHAR) AS $$
DECLARE
    v_new_status BOOLEAN;
BEGIN
    UPDATE users
    SET is_active = NOT is_active
    WHERE id = p_user_id
    RETURNING is_active INTO v_new_status;

    IF v_new_status IS NULL THEN
        RETURN QUERY SELECT FALSE, NULL::BOOLEAN, 'User not found'::VARCHAR;
        RETURN;
    END IF;

    RETURN QUERY SELECT TRUE, v_new_status, 'User status updated'::VARCHAR;
EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT FALSE, NULL::BOOLEAN, ('Error updating user status: ' || SQLERRM)::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- Trigger helper function refresh (non-destructive deployment)
CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND (to_jsonb(NEW) - 'updated_at') = (to_jsonb(OLD) - 'updated_at') THEN
        RETURN NEW;
    END IF;

    INSERT INTO user_audit_log (user_id, action, old_data, new_data)
    VALUES (
        COALESCE(NEW.id, OLD.id),
        TG_OP,
        CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END,
        CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE to_jsonb(NEW) END
    );
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;
