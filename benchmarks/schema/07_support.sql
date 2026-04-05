-- support.teams
CREATE TABLE support.teams (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true
);

-- support.agents
CREATE TABLE support.agents (
    id SERIAL PRIMARY KEY,
    team_id INT NOT NULL REFERENCES support.teams(id),
    name VARCHAR(150) NOT NULL,
    email VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    max_concurrent_tickets INT NOT NULL DEFAULT 10
);
CREATE INDEX idx_sa_team ON support.agents(team_id);
CREATE INDEX idx_sa_status ON support.agents(status);

-- support.sla_policies
CREATE TABLE support.sla_policies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    priority VARCHAR(20) NOT NULL,
    first_response_hours INT NOT NULL,
    resolution_hours INT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true
);

-- support.tickets
CREATE TABLE support.tickets (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES customers.accounts(id),
    order_id INT REFERENCES orders.orders(id),
    category VARCHAR(30) NOT NULL,
    priority VARCHAR(20) NOT NULL DEFAULT 'medium',
    status VARCHAR(20) NOT NULL DEFAULT 'open',
    subject VARCHAR(300) NOT NULL,
    channel VARCHAR(20) NOT NULL DEFAULT 'web',
    assigned_agent_id INT REFERENCES support.agents(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ
);
CREATE INDEX idx_st_account ON support.tickets(account_id);
CREATE INDEX idx_st_order ON support.tickets(order_id);
CREATE INDEX idx_st_agent ON support.tickets(assigned_agent_id);
CREATE INDEX idx_st_status ON support.tickets(status);
CREATE INDEX idx_st_priority ON support.tickets(priority);
CREATE INDEX idx_st_created_at ON support.tickets(created_at);

-- support.ticket_messages
CREATE TABLE support.ticket_messages (
    id SERIAL PRIMARY KEY,
    ticket_id INT NOT NULL REFERENCES support.tickets(id) ON DELETE CASCADE,
    sender_type VARCHAR(20) NOT NULL,
    sender_id INT,
    body TEXT NOT NULL,
    is_internal BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_stm_ticket ON support.ticket_messages(ticket_id);

-- support.faq_articles
CREATE TABLE support.faq_articles (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    title VARCHAR(300) NOT NULL,
    body TEXT NOT NULL,
    helpful_count INT NOT NULL DEFAULT 0,
    view_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

-- support.escalations
CREATE TABLE support.escalations (
    id SERIAL PRIMARY KEY,
    ticket_id INT NOT NULL REFERENCES support.tickets(id) ON DELETE CASCADE,
    from_agent_id INT REFERENCES support.agents(id),
    to_agent_id INT REFERENCES support.agents(id),
    reason TEXT,
    escalated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_se_ticket ON support.escalations(ticket_id);
CREATE INDEX idx_se_from ON support.escalations(from_agent_id);
CREATE INDEX idx_se_to ON support.escalations(to_agent_id);

-- support.macros
CREATE TABLE support.macros (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    template_body TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    usage_count INT NOT NULL DEFAULT 0
);

-- support.knowledge_base
CREATE TABLE support.knowledge_base (
    id SERIAL PRIMARY KEY,
    title VARCHAR(300) NOT NULL,
    slug VARCHAR(200),
    content TEXT NOT NULL,
    category VARCHAR(50),
    parent_id INT REFERENCES support.knowledge_base(id),
    view_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
CREATE INDEX idx_skb_parent ON support.knowledge_base(parent_id);
CREATE INDEX idx_skb_category ON support.knowledge_base(category);

-- support.ticket_tags
CREATE TABLE support.ticket_tags (
    id SERIAL PRIMARY KEY,
    ticket_id INT NOT NULL REFERENCES support.tickets(id) ON DELETE CASCADE,
    tag VARCHAR(50) NOT NULL
);
CREATE INDEX idx_stt_ticket ON support.ticket_tags(ticket_id);
