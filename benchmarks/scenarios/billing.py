"""Billing agent scenarios."""

from .base import ScenarioSpec  # type: ignore[import-not-found]

BILLING_SCENARIOS = [
    ScenarioSpec(
        id="B1",
        agent_type="billing",
        question=(
            "Customer #4521 is disputing a charge of $89.99 from March 15, 2026. "
            "Find the payment, the related order, and check if there's already a refund or dispute on file."
        ),
        expected_tables=[
            "billing.payments", "billing.invoices", "orders.orders",
            "billing.refunds",
        ],
        expected_columns=["amount", "processed_at", "status", "processor_ref", "order_id", "dispute_type"],
        expected_facts=[
            "89.99", "2026-03-15", "invoice", "processor_ref", "status",
        ],
        golden_sql="""
            SELECT p.id AS payment_id, p.amount, p.processed_at, p.status AS payment_status,
                   p.processor_ref, i.order_id, i.invoice_number,
                   r.id AS refund_id, r.status AS refund_status, r.amount AS refund_amount,
                   d.id AS dispute_id, d.status AS dispute_status, d.dispute_type
            FROM billing.payments p
            JOIN billing.invoices i ON p.invoice_id = i.id
            LEFT JOIN billing.refunds r ON r.payment_id = p.id
            LEFT JOIN billing.payment_disputes d ON d.payment_id = p.id
            WHERE i.account_id = 4521
              AND p.amount = 89.99
              AND p.processed_at::date = '2026-03-15'
        """,
        difficulty="medium",
    ),
    ScenarioSpec(
        id="B2",
        agent_type="billing",
        question=(
            "Generate a billing summary for account #1200 covering the last 3 months. "
            "Show all invoices, total payments received, and outstanding balance."
        ),
        expected_tables=["billing.invoices", "billing.payments"],
        expected_columns=["invoice_number", "total", "status", "issued_at"],
        expected_facts=[
            "invoice_number",
            "paid_amount",
            "outstanding",
            "overdue",
            "issued_at",
        ],
        golden_sql="""
            SELECT i.id, i.invoice_number, i.total, i.status, i.issued_at, i.paid_at,
                   COALESCE(SUM(p.amount) FILTER (WHERE p.status = 'completed'), 0) AS paid_amount,
                   i.total - COALESCE(SUM(p.amount) FILTER (WHERE p.status = 'completed'), 0) AS outstanding
            FROM billing.invoices i
            LEFT JOIN billing.payments p ON p.invoice_id = i.id
            WHERE i.account_id = 1200
              AND i.issued_at >= NOW() - INTERVAL '3 months'
            GROUP BY i.id
            ORDER BY i.issued_at DESC
        """,
        difficulty="medium",
    ),
    ScenarioSpec(
        id="B3",
        agent_type="billing",
        question=(
            "Customer #4521 wants to cancel their Premium Annual subscription. "
            "What is the prorated refund amount based on time remaining? "
            "Are there any active promotions they would lose?"
        ),
        expected_tables=[
            "billing.subscriptions", "billing.subscription_payments",
            "billing.promotions", "orders.order_coupons",
        ],
        expected_columns=["plan", "price", "started_at", "next_billing_at", "billing_cycle", "auto_renew"],
        expected_facts=[
            "Premium Annual", "139", "active", "next_billing_at", "auto_renew",
        ],
        golden_sql="""
            SELECT s.id, s.plan, s.price, s.billing_cycle, s.status,
                   s.started_at, s.next_billing_at, s.auto_renew,
                   s.price * (1.0 - EXTRACT(EPOCH FROM (NOW() - s.started_at))
                              / EXTRACT(EPOCH FROM (s.next_billing_at - s.started_at))) AS prorated_refund
            FROM billing.subscriptions s
            WHERE s.account_id = 4521
              AND s.plan = 'Premium Annual'
              AND s.status = 'active'
        """,
        difficulty="hard",
    ),
    ScenarioSpec(
        id="B4",
        agent_type="billing",
        question=(
            "Apply promo code SPRING25 to order #78234. "
            "Verify the promo is valid, check minimum order requirement, and calculate the discount."
        ),
        expected_tables=["billing.promotions", "orders.orders"],
        expected_columns=["code", "value", "min_order", "valid_from", "valid_until", "is_active", "total", "type"],
        expected_facts=[
            "SPRING25", "percentage", "25", "min_order", "is_active",
        ],
        golden_sql="""
            SELECT p.code, p.name, p.type, p.value, p.min_order, p.max_discount,
                   p.valid_from, p.valid_until, p.is_active, p.used_count, p.max_uses,
                   o.total AS order_total,
                   LEAST(o.total * p.value / 100.0, p.max_discount) AS discount_amount,
                   CASE WHEN o.total >= p.min_order
                             AND p.is_active
                             AND p.valid_from <= NOW()
                             AND (p.valid_until IS NULL OR p.valid_until >= NOW())
                             AND (p.max_uses IS NULL OR p.used_count < p.max_uses)
                        THEN true ELSE false
                   END AS can_apply
            FROM billing.promotions p, orders.orders o
            WHERE p.code = 'SPRING25' AND o.id = 78234
        """,
        difficulty="easy",
    ),
    ScenarioSpec(
        id="B5",
        agent_type="billing",
        question=(
            "Reconcile gift card GC-4455: show the original balance, "
            "all transactions (loads, redemptions, refunds), and current balance."
        ),
        expected_tables=["billing.gift_cards", "billing.gift_card_transactions"],
        expected_columns=["code", "balance", "original_amount", "amount", "transaction_type"],
        expected_facts=[
            "GC-4455", "100", "balance", "transaction_type", "redeem",
        ],
        golden_sql="""
            SELECT gc.code, gc.original_amount, gc.balance, gc.status,
                   gct.transaction_type, gct.amount, gct.created_at
            FROM billing.gift_cards gc
            LEFT JOIN billing.gift_card_transactions gct ON gct.gift_card_id = gc.id
            WHERE gc.code = 'GC-4455'
            ORDER BY gct.created_at
        """,
        difficulty="easy",
    ),
]
