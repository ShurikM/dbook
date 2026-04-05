"""Sales agent — specializes in product/inventory domain."""

from agents.base_agent import BaseAgent


class SalesAgent(BaseAgent):
    DOMAIN_TERMS = [
        "product", "inventory", "stock", "warehouse", "cart", "review",
        "rating", "category", "price", "brand", "asin", "search",
        "headphone", "electronic", "recommend", "bought", "together",
    ]
    PRIORITY_SCHEMAS = ["catalog", "orders", "warehouse"]
