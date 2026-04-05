-- catalog.brands
CREATE TABLE catalog.brands (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    logo_url VARCHAR(500),
    description TEXT
);

-- catalog.categories (self-referential hierarchy)
CREATE TABLE catalog.categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100) NOT NULL,
    parent_id INT REFERENCES catalog.categories(id),
    level INT NOT NULL DEFAULT 0,
    path TEXT,
    is_active BOOLEAN DEFAULT true
);
CREATE INDEX idx_cat_parent ON catalog.categories(parent_id);
CREATE INDEX idx_cat_slug ON catalog.categories(slug);

-- catalog.products
CREATE TABLE catalog.products (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) UNIQUE NOT NULL,
    title VARCHAR(300) NOT NULL,
    description TEXT,
    brand_id INT REFERENCES catalog.brands(id),
    category_id INT REFERENCES catalog.categories(id),
    price NUMERIC(10,2) NOT NULL,
    compare_at_price NUMERIC(10,2),
    weight NUMERIC(8,2),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_cp_category ON catalog.products(category_id);
CREATE INDEX idx_cp_brand ON catalog.products(brand_id);
CREATE INDEX idx_cp_status ON catalog.products(status);
CREATE INDEX idx_cp_price ON catalog.products(price);

-- catalog.product_images
CREATE TABLE catalog.product_images (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    url VARCHAR(500) NOT NULL,
    position INT DEFAULT 0,
    alt_text VARCHAR(200),
    is_primary BOOLEAN DEFAULT false
);
CREATE INDEX idx_cpi_product ON catalog.product_images(product_id);

-- catalog.reviews
CREATE TABLE catalog.reviews (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    account_id INT NOT NULL REFERENCES customers.accounts(id),
    rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(200),
    body TEXT,
    verified BOOLEAN DEFAULT false,
    helpful_count INT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_cr_product ON catalog.reviews(product_id);
CREATE INDEX idx_cr_account ON catalog.reviews(account_id);

-- catalog.product_attributes (key-value)
CREATE TABLE catalog.product_attributes (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    attribute_name VARCHAR(100) NOT NULL,
    attribute_value VARCHAR(500) NOT NULL
);
CREATE INDEX idx_pa_product ON catalog.product_attributes(product_id);

-- catalog.price_history
CREATE TABLE catalog.price_history (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    price NUMERIC(10,2) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by VARCHAR(100)
);
CREATE INDEX idx_ph_product ON catalog.price_history(product_id);

-- catalog.product_tags
CREATE TABLE catalog.product_tags (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    tag VARCHAR(50) NOT NULL
);
CREATE INDEX idx_pt_product ON catalog.product_tags(product_id);

-- catalog.related_products
CREATE TABLE catalog.related_products (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    related_product_id INT NOT NULL REFERENCES catalog.products(id) ON DELETE CASCADE,
    relationship_type VARCHAR(30) NOT NULL
);
CREATE INDEX idx_rp_product ON catalog.related_products(product_id);

-- Add FK from customers.wishlist_items to catalog.products
ALTER TABLE customers.wishlist_items
    ADD CONSTRAINT fk_wli_product FOREIGN KEY (product_id) REFERENCES catalog.products(id);
