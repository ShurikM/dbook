-- warehouse.warehouses
CREATE TABLE warehouse.warehouses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(20) UNIQUE NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(2) NOT NULL DEFAULT 'US',
    capacity INT,
    is_active BOOLEAN NOT NULL DEFAULT true
);

-- warehouse.locations
CREATE TABLE warehouse.locations (
    id SERIAL PRIMARY KEY,
    warehouse_id INT NOT NULL REFERENCES warehouse.warehouses(id) ON DELETE CASCADE,
    aisle VARCHAR(10),
    shelf VARCHAR(10),
    bin VARCHAR(10),
    zone VARCHAR(20)
);
CREATE INDEX idx_wl_warehouse ON warehouse.locations(warehouse_id);

-- warehouse.stock
CREATE TABLE warehouse.stock (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id),
    location_id INT NOT NULL REFERENCES warehouse.locations(id),
    quantity INT NOT NULL DEFAULT 0,
    last_counted_at TIMESTAMPTZ
);
CREATE INDEX idx_ws_product ON warehouse.stock(product_id);
CREATE INDEX idx_ws_location ON warehouse.stock(location_id);

-- warehouse.suppliers
CREATE TABLE warehouse.suppliers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    contact_email VARCHAR(255),
    phone VARCHAR(30),
    lead_time_days INT,
    rating NUMERIC(3,2)
);

-- warehouse.purchase_orders
CREATE TABLE warehouse.purchase_orders (
    id SERIAL PRIMARY KEY,
    supplier_id INT NOT NULL REFERENCES warehouse.suppliers(id),
    warehouse_id INT NOT NULL REFERENCES warehouse.warehouses(id),
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    total_cost NUMERIC(12,2),
    ordered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expected_at DATE,
    received_at TIMESTAMPTZ
);
CREATE INDEX idx_wpo_supplier ON warehouse.purchase_orders(supplier_id);
CREATE INDEX idx_wpo_warehouse ON warehouse.purchase_orders(warehouse_id);
CREATE INDEX idx_wpo_status ON warehouse.purchase_orders(status);

-- warehouse.purchase_order_items
CREATE TABLE warehouse.purchase_order_items (
    id SERIAL PRIMARY KEY,
    purchase_order_id INT NOT NULL REFERENCES warehouse.purchase_orders(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES catalog.products(id),
    quantity INT NOT NULL,
    unit_cost NUMERIC(10,2) NOT NULL
);
CREATE INDEX idx_wpoi_po ON warehouse.purchase_order_items(purchase_order_id);
CREATE INDEX idx_wpoi_product ON warehouse.purchase_order_items(product_id);

-- warehouse.transfers
CREATE TABLE warehouse.transfers (
    id SERIAL PRIMARY KEY,
    from_warehouse_id INT NOT NULL REFERENCES warehouse.warehouses(id),
    to_warehouse_id INT NOT NULL REFERENCES warehouse.warehouses(id),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    initiated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX idx_wt_from ON warehouse.transfers(from_warehouse_id);
CREATE INDEX idx_wt_to ON warehouse.transfers(to_warehouse_id);
CREATE INDEX idx_wt_status ON warehouse.transfers(status);

-- warehouse.transfer_items
CREATE TABLE warehouse.transfer_items (
    id SERIAL PRIMARY KEY,
    transfer_id INT NOT NULL REFERENCES warehouse.transfers(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES catalog.products(id),
    quantity INT NOT NULL
);
CREATE INDEX idx_wti_transfer ON warehouse.transfer_items(transfer_id);
CREATE INDEX idx_wti_product ON warehouse.transfer_items(product_id);

-- warehouse.shipping_rates
CREATE TABLE warehouse.shipping_rates (
    id SERIAL PRIMARY KEY,
    carrier VARCHAR(50) NOT NULL,
    service_level VARCHAR(50) NOT NULL,
    weight_min NUMERIC(8,2) NOT NULL DEFAULT 0,
    weight_max NUMERIC(8,2) NOT NULL,
    zone VARCHAR(20),
    rate NUMERIC(10,2) NOT NULL
);

-- warehouse.picking_lists
CREATE TABLE warehouse.picking_lists (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders.orders(id),
    warehouse_id INT NOT NULL REFERENCES warehouse.warehouses(id),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    assigned_to VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX idx_wpl_order ON warehouse.picking_lists(order_id);
CREATE INDEX idx_wpl_warehouse ON warehouse.picking_lists(warehouse_id);
CREATE INDEX idx_wpl_status ON warehouse.picking_lists(status);

-- catalog.inventory (lives in catalog schema but depends on warehouse)
CREATE TABLE catalog.inventory (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    warehouse_id INT NOT NULL REFERENCES warehouse.warehouses(id),
    quantity INT NOT NULL DEFAULT 0,
    reserved INT NOT NULL DEFAULT 0,
    reorder_point INT NOT NULL DEFAULT 10,
    last_restocked_at TIMESTAMPTZ
);
CREATE INDEX idx_ci_product ON catalog.inventory(product_id);
CREATE INDEX idx_ci_warehouse ON catalog.inventory(warehouse_id);
