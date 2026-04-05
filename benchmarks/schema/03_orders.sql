-- orders.carts
CREATE TABLE orders.carts (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    session_id VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    converted_at TIMESTAMPTZ
);
CREATE INDEX idx_oc_account ON orders.carts(account_id);
CREATE INDEX idx_oc_status ON orders.carts(status);

-- orders.cart_items
CREATE TABLE orders.cart_items (
    id SERIAL PRIMARY KEY,
    cart_id INT NOT NULL REFERENCES orders.carts(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES catalog.products(id),
    quantity INT NOT NULL DEFAULT 1,
    unit_price NUMERIC(10,2) NOT NULL,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_oci_cart ON orders.cart_items(cart_id);
CREATE INDEX idx_oci_product ON orders.cart_items(product_id);

-- orders.orders
-- NOTE: promotion_id FK to billing.promotions added in 04_billing.sql via ALTER TABLE
CREATE TABLE orders.orders (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id),
    shipping_address_id INT REFERENCES customers.addresses(id),
    billing_address_id INT REFERENCES customers.addresses(id),
    payment_method_id INT REFERENCES customers.payment_methods(id),
    subtotal NUMERIC(10,2) NOT NULL,
    tax NUMERIC(10,2) NOT NULL DEFAULT 0,
    shipping_cost NUMERIC(10,2) NOT NULL DEFAULT 0,
    discount_amount NUMERIC(10,2) NOT NULL DEFAULT 0,
    total NUMERIC(10,2) NOT NULL CHECK (total >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    promotion_id INT,  -- FK added in 04_billing.sql
    placed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX idx_oo_account ON orders.orders(account_id);
CREATE INDEX idx_oo_status ON orders.orders(status);
CREATE INDEX idx_oo_placed_at ON orders.orders(placed_at);
CREATE INDEX idx_oo_shipping_addr ON orders.orders(shipping_address_id);
CREATE INDEX idx_oo_billing_addr ON orders.orders(billing_address_id);
CREATE INDEX idx_oo_payment ON orders.orders(payment_method_id);

-- orders.order_items
CREATE TABLE orders.order_items (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders.orders(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES catalog.products(id),
    quantity INT NOT NULL DEFAULT 1,
    unit_price NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
);
CREATE INDEX idx_ooi_order ON orders.order_items(order_id);
CREATE INDEX idx_ooi_product ON orders.order_items(product_id);
CREATE INDEX idx_ooi_status ON orders.order_items(status);

-- orders.shipments
CREATE TABLE orders.shipments (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders.orders(id) ON DELETE CASCADE,
    carrier VARCHAR(50),
    tracking_number VARCHAR(100),
    estimated_delivery DATE,
    shipped_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'preparing'
);
CREATE INDEX idx_os_order ON orders.shipments(order_id);
CREATE INDEX idx_os_status ON orders.shipments(status);

-- orders.returns
CREATE TABLE orders.returns (
    id SERIAL PRIMARY KEY,
    order_item_id INT NOT NULL REFERENCES orders.order_items(id),
    reason_code VARCHAR(50) NOT NULL,
    reason_detail TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'requested',
    refund_amount NUMERIC(10,2),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);
CREATE INDEX idx_or_order_item ON orders.returns(order_item_id);
CREATE INDEX idx_or_status ON orders.returns(status);

-- orders.order_status_history
CREATE TABLE orders.order_status_history (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders.orders(id) ON DELETE CASCADE,
    old_status VARCHAR(20),
    new_status VARCHAR(20) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by VARCHAR(100)
);
CREATE INDEX idx_osh_order ON orders.order_status_history(order_id);
CREATE INDEX idx_osh_changed_at ON orders.order_status_history(changed_at);

-- orders.order_notes
CREATE TABLE orders.order_notes (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders.orders(id) ON DELETE CASCADE,
    note_type VARCHAR(20) NOT NULL DEFAULT 'internal',
    body TEXT NOT NULL,
    created_by VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_on_order ON orders.order_notes(order_id);

-- orders.saved_for_later
CREATE TABLE orders.saved_for_later (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES catalog.products(id),
    saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_sfl_account ON orders.saved_for_later(account_id);
CREATE INDEX idx_sfl_product ON orders.saved_for_later(product_id);

-- orders.order_coupons
-- NOTE: promotion_id FK to billing.promotions added in 04_billing.sql via ALTER TABLE
CREATE TABLE orders.order_coupons (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders.orders(id) ON DELETE CASCADE,
    promotion_id INT,  -- FK added in 04_billing.sql
    discount_amount NUMERIC(10,2) NOT NULL
);
CREATE INDEX idx_ocoup_order ON orders.order_coupons(order_id);
