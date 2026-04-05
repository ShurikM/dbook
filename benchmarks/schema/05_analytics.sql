-- analytics.sessions
CREATE TABLE analytics.sessions (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) UNIQUE NOT NULL,
    account_id INT REFERENCES customers.accounts(id),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    page_count INT NOT NULL DEFAULT 0,
    device_type VARCHAR(20),
    utm_source VARCHAR(100),
    utm_medium VARCHAR(100)
);
CREATE INDEX idx_as_account ON analytics.sessions(account_id);
CREATE INDEX idx_as_started_at ON analytics.sessions(started_at);

-- analytics.page_views
CREATE TABLE analytics.page_views (
    id SERIAL PRIMARY KEY,
    account_id INT REFERENCES customers.accounts(id),
    session_id VARCHAR(100),
    page_type VARCHAR(50),
    page_id VARCHAR(100),
    referrer VARCHAR(500),
    user_agent TEXT,
    ip_address INET,
    device_type VARCHAR(20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_apv_account ON analytics.page_views(account_id);
CREATE INDEX idx_apv_session ON analytics.page_views(session_id);
CREATE INDEX idx_apv_created_at ON analytics.page_views(created_at);

-- analytics.search_queries
CREATE TABLE analytics.search_queries (
    id SERIAL PRIMARY KEY,
    account_id INT REFERENCES customers.accounts(id),
    query_text VARCHAR(500) NOT NULL,
    results_count INT NOT NULL DEFAULT 0,
    clicked_product_id INT REFERENCES catalog.products(id),
    category_filter VARCHAR(100),
    min_price NUMERIC(10,2),
    max_price NUMERIC(10,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_asq_account ON analytics.search_queries(account_id);
CREATE INDEX idx_asq_clicked ON analytics.search_queries(clicked_product_id);
CREATE INDEX idx_asq_created_at ON analytics.search_queries(created_at);

-- analytics.click_events
CREATE TABLE analytics.click_events (
    id SERIAL PRIMARY KEY,
    account_id INT REFERENCES customers.accounts(id),
    session_id VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,
    element_id VARCHAR(100),
    page_url VARCHAR(500),
    product_id INT REFERENCES catalog.products(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_ace_account ON analytics.click_events(account_id);
CREATE INDEX idx_ace_session ON analytics.click_events(session_id);
CREATE INDEX idx_ace_product ON analytics.click_events(product_id);
CREATE INDEX idx_ace_created_at ON analytics.click_events(created_at);

-- analytics.conversion_funnels
CREATE TABLE analytics.conversion_funnels (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    steps_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

-- analytics.daily_metrics
CREATE TABLE analytics.daily_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(12,4) NOT NULL,
    dimension VARCHAR(100),
    dimension_value VARCHAR(200)
);
CREATE INDEX idx_adm_date ON analytics.daily_metrics(date);
CREATE INDEX idx_adm_metric ON analytics.daily_metrics(metric_name);

-- analytics.ab_tests
CREATE TABLE analytics.ab_tests (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    variant VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(12,6) NOT NULL,
    sample_size INT NOT NULL,
    significance NUMERIC(5,4),
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ
);

-- analytics.recommendations
CREATE TABLE analytics.recommendations (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id),
    product_id INT NOT NULL REFERENCES catalog.products(id),
    algorithm VARCHAR(50) NOT NULL,
    score NUMERIC(5,4) NOT NULL,
    shown_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    clicked BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX idx_ar_account ON analytics.recommendations(account_id);
CREATE INDEX idx_ar_product ON analytics.recommendations(product_id);
CREATE INDEX idx_ar_shown_at ON analytics.recommendations(shown_at);

-- analytics.product_impressions
CREATE TABLE analytics.product_impressions (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog.products(id),
    session_id VARCHAR(100),
    position INT,
    page_type VARCHAR(50),
    clicked BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_api_product ON analytics.product_impressions(product_id);
CREATE INDEX idx_api_session ON analytics.product_impressions(session_id);
CREATE INDEX idx_api_created_at ON analytics.product_impressions(created_at);

-- analytics.cohort_analysis
CREATE TABLE analytics.cohort_analysis (
    id SERIAL PRIMARY KEY,
    cohort_date DATE NOT NULL,
    cohort_size INT NOT NULL,
    period INT NOT NULL,
    retention_rate NUMERIC(5,4) NOT NULL,
    revenue NUMERIC(12,2) NOT NULL DEFAULT 0
);
CREATE INDEX idx_aca_cohort_date ON analytics.cohort_analysis(cohort_date);
