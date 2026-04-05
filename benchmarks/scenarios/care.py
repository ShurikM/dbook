"""Care (support) agent scenarios."""

from .base import ScenarioSpec  # type: ignore[import-not-found]

CARE_SCENARIOS = [
    ScenarioSpec(
        id="C1",
        agent_type="care",
        question=(
            "Customer says 'my order hasn't arrived yet.' "
            "Look up order #34567, check shipment status, tracking number, carrier, and expected delivery."
        ),
        expected_tables=["orders.orders", "orders.shipments"],
        expected_columns=["tracking_number", "carrier", "status", "estimated_delivery", "shipped_at"],
        expected_facts=[
            "34567", "in_transit", "UPS", "tracking_number", "estimated_delivery",
        ],
        golden_sql="""
            SELECT o.id AS order_id, o.status AS order_status, o.placed_at,
                   s.carrier, s.tracking_number, s.estimated_delivery,
                   s.shipped_at, s.delivered_at, s.status AS shipment_status
            FROM orders.orders o
            LEFT JOIN orders.shipments s ON s.order_id = o.id
            WHERE o.id = 34567
        """,
        difficulty="easy",
    ),
    ScenarioSpec(
        id="C2",
        agent_type="care",
        question=(
            "Customer wants to return an item from order #45678. "
            "Find the order items, check if it was delivered (for return window), "
            "and whether a return already exists."
        ),
        expected_tables=[
            "orders.orders", "orders.order_items", "orders.shipments",
            "orders.returns",
        ],
        expected_columns=["product_id", "quantity", "unit_price", "delivered_at", "status"],
        expected_facts=[
            "45678", "product_id", "delivered_at", "return",
        ],
        golden_sql="""
            SELECT oi.id AS item_id, p.title AS product, oi.quantity, oi.unit_price, oi.status AS item_status,
                   s.delivered_at,
                   CASE WHEN s.delivered_at IS NOT NULL
                             AND s.delivered_at > NOW() - INTERVAL '30 days'
                        THEN 'eligible' ELSE 'expired'
                   END AS return_eligibility,
                   r.id AS existing_return_id, r.status AS return_status
            FROM orders.order_items oi
            JOIN orders.orders o ON oi.order_id = o.id
            JOIN catalog.products p ON oi.product_id = p.id
            LEFT JOIN orders.shipments s ON s.order_id = o.id
            LEFT JOIN orders.returns r ON r.order_item_id = oi.id
            WHERE o.id = 45678
        """,
        difficulty="medium",
    ),
    ScenarioSpec(
        id="C3",
        agent_type="care",
        question=(
            "Escalate ticket #8901. This customer has had 3 tickets in 2 weeks. "
            "Pull the full ticket history and all messages for context."
        ),
        expected_tables=[
            "support.tickets", "support.ticket_messages",
        ],
        expected_columns=["subject", "status", "priority", "body", "created_at", "category"],
        expected_facts=[
            "8901", "account_id", "subject", "body",
        ],
        golden_sql="""
            SELECT t.id AS ticket_id, t.subject, t.status, t.priority, t.category,
                   t.channel, t.created_at,
                   tm.body, tm.sender_type, tm.created_at AS message_at
            FROM support.tickets t
            LEFT JOIN support.ticket_messages tm ON tm.ticket_id = t.id
            WHERE t.account_id = (SELECT account_id FROM support.tickets WHERE id = 8901)
              AND t.created_at >= NOW() - INTERVAL '14 days'
            ORDER BY t.created_at DESC, tm.created_at ASC
        """,
        difficulty="hard",
    ),
    ScenarioSpec(
        id="C4",
        agent_type="care",
        question=(
            "Verify customer identity: they claim their email is alice@example.com. "
            "Check the account exists, show recent orders, and payment methods on file."
        ),
        expected_tables=[
            "customers.accounts", "orders.orders",
            "customers.payment_methods",
        ],
        expected_columns=["email", "name", "phone", "card_last_four", "status"],
        expected_facts=[
            "alice@example.com", "Alice", "account", "payment", "order",
        ],
        golden_sql="""
            SELECT a.id, a.name, a.email, a.phone, a.status, a.tier,
                   pm.type AS payment_type, pm.provider, pm.card_last_four,
                   (SELECT COUNT(*) FROM orders.orders o
                    WHERE o.account_id = a.id
                      AND o.placed_at > NOW() - INTERVAL '90 days') AS recent_orders,
                   (SELECT login_at FROM customers.login_history lh
                    WHERE lh.account_id = a.id
                    ORDER BY lh.login_at DESC LIMIT 1) AS last_login
            FROM customers.accounts a
            LEFT JOIN customers.payment_methods pm ON pm.account_id = a.id AND pm.is_default = true
            WHERE a.email = 'alice@example.com'
        """,
        difficulty="easy",
    ),
    ScenarioSpec(
        id="C5",
        agent_type="care",
        question=(
            "Customer asks 'why was I charged twice?' "
            "Find all payments for customer #4521's most recent order. "
            "Show amounts, dates, statuses, and processor references."
        ),
        expected_tables=[
            "billing.payments", "billing.invoices", "orders.orders",
        ],
        expected_columns=["amount", "processed_at", "status", "processor_ref", "failure_reason"],
        expected_facts=[
            "payment", "amount", "processed_at", "status", "processor_ref",
        ],
        golden_sql="""
            SELECT p.id AS payment_id, p.amount, p.status, p.processed_at,
                   p.processor_ref, p.failure_reason, p.currency,
                   i.invoice_number, o.id AS order_id
            FROM billing.payments p
            JOIN billing.invoices i ON p.invoice_id = i.id
            JOIN orders.orders o ON i.order_id = o.id
            WHERE o.id = (
                SELECT id FROM orders.orders
                WHERE account_id = 4521
                ORDER BY placed_at DESC LIMIT 1
            )
            ORDER BY p.processed_at
        """,
        difficulty="medium",
    ),
]
