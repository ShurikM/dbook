"""Care (support) agent — specializes in customer support domain."""

from agents.base_agent import BaseAgent


class CareAgent(BaseAgent):
    DOMAIN_TERMS = [
        "ticket", "support", "order", "shipment", "tracking", "return",
        "customer", "account", "email", "phone", "identity", "escalat",
        "delivery", "carrier", "message", "history", "login",
    ]
    PRIORITY_SCHEMAS = ["support", "orders", "customers"]
