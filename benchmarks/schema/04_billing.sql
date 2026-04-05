-- billing.promotions
CREATE TABLE billing.promotions (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(150) NOT NULL,
    type VARCHAR(20) NOT NULL,
    value NUMERIC(10,2) NOT NULL,
    min_order NUMERIC(10,2),
    max_discount NUMERIC(10,2),
    max_uses INT,
    used_count INT NOT NULL DEFAULT 0,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true
);

-- Add deferred FKs from orders schema to billing.promotions
ALTER TABLE orders.orders
    ADD CONSTRAINT fk_oo_promotion FOREIGN KEY (promotion_id) REFERENCES billing.promotions(id);

ALTER TABLE orders.order_coupons
    ADD CONSTRAINT fk_ocoup_promotion FOREIGN KEY (promotion_id) REFERENCES billing.promotions(id);

-- billing.tax_rates
CREATE TABLE billing.tax_rates (
    id SERIAL PRIMARY KEY,
    jurisdiction VARCHAR(100) NOT NULL,
    state VARCHAR(50),
    rate NUMERIC(5,4) NOT NULL,
    effective_from DATE NOT NULL,
    effective_until DATE
);

-- billing.billing_accounts
CREATE TABLE billing.billing_accounts (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    billing_email VARCHAR(255),
    company_name VARCHAR(200),
    tax_id VARCHAR(50),
    net_terms INT NOT NULL DEFAULT 0
);
CREATE INDEX idx_ba_account ON billing.billing_accounts(account_id);

-- billing.invoices
CREATE TABLE billing.invoices (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders.orders(id),
    account_id INT NOT NULL REFERENCES customers.accounts(id),
    invoice_number VARCHAR(50) UNIQUE NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    tax NUMERIC(10,2) NOT NULL DEFAULT 0,
    total NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    due_at TIMESTAMPTZ,
    paid_at TIMESTAMPTZ
);
CREATE INDEX idx_bi_order ON billing.invoices(order_id);
CREATE INDEX idx_bi_account ON billing.invoices(account_id);
CREATE INDEX idx_bi_status ON billing.invoices(status);

-- billing.payments
CREATE TABLE billing.payments (
    id SERIAL PRIMARY KEY,
    invoice_id INT NOT NULL REFERENCES billing.invoices(id),
    payment_method_id INT REFERENCES customers.payment_methods(id),
    amount NUMERIC(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    processor_ref VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    processed_at TIMESTAMPTZ,
    failure_reason TEXT
);
CREATE INDEX idx_bp_invoice ON billing.payments(invoice_id);
CREATE INDEX idx_bp_payment_method ON billing.payments(payment_method_id);
CREATE INDEX idx_bp_status ON billing.payments(status);

-- billing.refunds
CREATE TABLE billing.refunds (
    id SERIAL PRIMARY KEY,
    payment_id INT NOT NULL REFERENCES billing.payments(id),
    return_id INT REFERENCES orders.returns(id),
    amount NUMERIC(10,2) NOT NULL,
    reason TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    processed_at TIMESTAMPTZ,
    processor_ref VARCHAR(100)
);
CREATE INDEX idx_br_payment ON billing.refunds(payment_id);
CREATE INDEX idx_br_return ON billing.refunds(return_id);

-- billing.subscriptions
CREATE TABLE billing.subscriptions (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id) ON DELETE CASCADE,
    plan VARCHAR(50) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    billing_cycle VARCHAR(20) NOT NULL DEFAULT 'monthly',
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cancelled_at TIMESTAMPTZ,
    next_billing_at TIMESTAMPTZ,
    auto_renew BOOLEAN NOT NULL DEFAULT true
);
CREATE INDEX idx_bs_account ON billing.subscriptions(account_id);
CREATE INDEX idx_bs_status ON billing.subscriptions(status);

-- billing.subscription_payments
CREATE TABLE billing.subscription_payments (
    id SERIAL PRIMARY KEY,
    subscription_id INT NOT NULL REFERENCES billing.subscriptions(id) ON DELETE CASCADE,
    amount NUMERIC(10,2) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    processed_at TIMESTAMPTZ
);
CREATE INDEX idx_bsp_subscription ON billing.subscription_payments(subscription_id);

-- billing.gift_cards
CREATE TABLE billing.gift_cards (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    balance NUMERIC(10,2) NOT NULL,
    original_amount NUMERIC(10,2) NOT NULL,
    purchaser_id INT REFERENCES customers.accounts(id),
    recipient_email VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);
CREATE INDEX idx_bgc_purchaser ON billing.gift_cards(purchaser_id);
CREATE INDEX idx_bgc_status ON billing.gift_cards(status);

-- billing.gift_card_transactions
CREATE TABLE billing.gift_card_transactions (
    id SERIAL PRIMARY KEY,
    gift_card_id INT NOT NULL REFERENCES billing.gift_cards(id) ON DELETE CASCADE,
    order_id INT REFERENCES orders.orders(id),
    amount NUMERIC(10,2) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_bgct_gift_card ON billing.gift_card_transactions(gift_card_id);
CREATE INDEX idx_bgct_order ON billing.gift_card_transactions(order_id);

-- billing.credit_notes
CREATE TABLE billing.credit_notes (
    id SERIAL PRIMARY KEY,
    invoice_id INT NOT NULL REFERENCES billing.invoices(id),
    amount NUMERIC(10,2) NOT NULL,
    reason TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_bcn_invoice ON billing.credit_notes(invoice_id);

-- billing.payment_disputes
CREATE TABLE billing.payment_disputes (
    id SERIAL PRIMARY KEY,
    payment_id INT NOT NULL REFERENCES billing.payments(id),
    dispute_type VARCHAR(20) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'open',
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    resolution VARCHAR(30)
);
CREATE INDEX idx_bpd_payment ON billing.payment_disputes(payment_id);
CREATE INDEX idx_bpd_status ON billing.payment_disputes(status);

-- billing.ledger_entries
CREATE TABLE billing.ledger_entries (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id),
    entry_type VARCHAR(20) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    balance_after NUMERIC(12,2) NOT NULL,
    reference_type VARCHAR(20),
    reference_id INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_ble_account ON billing.ledger_entries(account_id);
CREATE INDEX idx_ble_created_at ON billing.ledger_entries(created_at);
