"""Billing agent — specializes in financial/billing domain."""

from agents.base_agent import BaseAgent


class BillingAgent(BaseAgent):
    DOMAIN_TERMS = [
        "payment", "invoice", "refund", "charge", "billing", "subscription",
        "promo", "promotion", "discount", "gift", "card", "balance",
        "amount", "price", "total", "credit", "dispute", "ledger",
    ]
    PRIORITY_SCHEMAS = ["billing", "orders"]
