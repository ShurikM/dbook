-- customers.accounts
CREATE TABLE customers.accounts (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    name VARCHAR(150) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    tier VARCHAR(20) NOT NULL DEFAULT 'standard',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX idx_ca_status ON customers.accounts(status);

-- customers.addresses
CREATE TABLE customers.addresses (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    label VARCHAR(50),
    street VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50),
    zip VARCHAR(20),
    country VARCHAR(2) NOT NULL DEFAULT 'US',
    is_default BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_addr_account ON customers.addresses(account_id);

-- customers.payment_methods
CREATE TABLE customers.payment_methods (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    type VARCHAR(30) NOT NULL,
    provider VARCHAR(50),
    card_last_four VARCHAR(4),
    expiry VARCHAR(7),
    billing_address_id INT REFERENCES customers.addresses(id),
    is_default BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_pm_account ON customers.payment_methods(account_id);

-- customers.preferences
CREATE TABLE customers.preferences (
    id SERIAL PRIMARY KEY,
    account_id INT UNIQUE NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    language VARCHAR(5) DEFAULT 'en',
    currency VARCHAR(3) DEFAULT 'USD',
    timezone VARCHAR(50) DEFAULT 'America/New_York',
    notification_email BOOLEAN DEFAULT true,
    notification_sms BOOLEAN DEFAULT false,
    notification_push BOOLEAN DEFAULT true
);

-- customers.wishlists
CREATE TABLE customers.wishlists (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL DEFAULT 'Default',
    is_public BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_wl_account ON customers.wishlists(account_id);

-- customers.wishlist_items (FK to catalog.products added after catalog schema)
CREATE TABLE customers.wishlist_items (
    id SERIAL PRIMARY KEY,
    wishlist_id INT NOT NULL REFERENCES customers.wishlists(id) ON DELETE CASCADE,
    product_id INT NOT NULL,  -- FK added in 02_catalog.sql
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority INT DEFAULT 0
);
CREATE INDEX idx_wli_wishlist ON customers.wishlist_items(wishlist_id);

-- customers.login_history
CREATE TABLE customers.login_history (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    ip_address INET,
    user_agent TEXT,
    device_type VARCHAR(20),
    login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success BOOLEAN NOT NULL DEFAULT true
);
CREATE INDEX idx_lh_account ON customers.login_history(account_id);
CREATE INDEX idx_lh_login_at ON customers.login_history(login_at);
