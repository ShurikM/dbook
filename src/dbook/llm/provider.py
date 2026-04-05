"""LLM provider — built on agentlib's multi-provider LLM layer.

agentlib (https://github.com/barkain/agentlib) provides the unified LLM
client that supports Anthropic, OpenAI, Google, xAI, and DeepSeek.  dbook
wraps it with a stable ``LLMProvider`` protocol so that the rest of the
codebase (enricher, CLI, tests) stays unchanged.

agentlib is a hard dependency — if it is not installed, dbook will not work.
"""

from __future__ import annotations

import json
import logging
from typing import Protocol, runtime_checkable

from agentlib.llm import call_llm as _agentlib_call_llm  # type: ignore[import-untyped]
from agentlib.llm import LLMConfig as _AgentlibLLMConfig  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol — unchanged from before so all consumers keep working
# ---------------------------------------------------------------------------

@runtime_checkable
class LLMProvider(Protocol):
    """Abstract LLM provider interface."""

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        """Send a prompt and return the completion text."""
        ...


# ---------------------------------------------------------------------------
# agentlib-backed provider (the primary provider)
# ---------------------------------------------------------------------------

class AgentlibProvider:
    """LLM provider wrapping agentlib's ``call_llm``.

    Supports every provider that agentlib supports (Anthropic, OpenAI,
    Google/Gemini, xAI/Grok, DeepSeek) through a single interface.
    """

    _DEFAULT_MODELS = {
        "anthropic": "claude-haiku-4-5-20251001",
        "openai": "gpt-4o-mini",
        "google": "gemini-2.0-flash",
        "gemini": "gemini-2.0-flash",
        "xai": "grok-3-mini",
        "deepseek": "deepseek-chat",
    }

    def __init__(self, provider: str, api_key: str, model: str | None = None):
        # Map "gemini" -> "google" for agentlib compatibility
        agentlib_provider = "google" if provider == "gemini" else provider
        self._config = _AgentlibLLMConfig(
            provider=agentlib_provider,
            model=model or self._DEFAULT_MODELS.get(provider, ""),
            api_key=api_key,
        )

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        return _agentlib_call_llm(self._config, prompt, max_tokens=max_tokens)


# ---------------------------------------------------------------------------
# Mock provider — test-specific, always lives in dbook
# ---------------------------------------------------------------------------

class MockProvider:
    """Mock provider for testing — returns deterministic responses based on prompt content."""

    def __init__(self):
        self.call_count = 0
        self.prompts: list[str] = []

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        self.call_count += 1
        self.prompts.append(prompt)

        # Detect what kind of enrichment is being requested
        prompt_lower = prompt.lower()

        if "summarize this" in prompt_lower and "table" in prompt_lower:
            return self._mock_table_summary(prompt)
        elif "concept" in prompt_lower and "alias" in prompt_lower:
            return self._mock_concept_aliases(prompt)
        elif "schema" in prompt_lower and "business purpose" in prompt_lower and "data flow" in prompt_lower:
            return self._mock_schema_narrative(prompt)
        elif "purpose of each column" in prompt_lower or "describe each column" in prompt_lower:
            return self._mock_column_purposes(prompt)
        else:
            return "Mock LLM response for unknown prompt type."

    def _mock_table_summary(self, prompt: str) -> str:
        # Extract table name from the "Table: <name>" line in the prompt
        table_name = ""
        for line in prompt.split("\n"):
            if line.startswith("Table: "):
                table_name = line[7:].strip()
                break

        _SUMMARIES = {
            # Original 13-table DB
            "auth_users": "Core user accounts table storing authentication credentials, profile information, and account status. Created during user registration and referenced by sessions, orders, and analytics events.",
            "billing_orders": "Customer purchase orders tracking order lifecycle from creation through fulfillment. Links customers to their purchases with status tracking and discount application.",
            "billing_payments": "Payment transaction records for settled invoices. Tracks payment method, amount, and processing timestamp.",
            "billing_invoices": "Invoice records generated from customer orders. Tracks billing contact, amounts, and payment status through draft-sent-paid lifecycle.",
            "analytics_events": "User interaction event log capturing page views, clicks, purchases, and signups. Used for behavioral analytics and conversion tracking.",
            "analytics_daily_revenue": "Pre-aggregated daily revenue metrics summarizing order volume and average order values. Used for financial dashboards and trend analysis.",
            "auth_sessions": "Active user login sessions tracking authentication tokens, client IP addresses, and session expiry. Used for session management and security monitoring.",
            "auth_roles": "Role-based access control (RBAC) role definitions. Contains the available permission roles that can be assigned to users.",
            "billing_products": "Product catalog containing items available for purchase. Stores pricing, categorization, and availability status.",
            "billing_order_items": "Individual line items within customer orders. Links products to orders with quantity and unit pricing.",
            "billing_discounts": "Promotional discount codes with percentage-based savings. Includes validity periods and usage limits.",
            "analytics_funnels": "Conversion funnel definitions tracking multi-step user journeys. Measures drop-off rates between funnel stages.",
            # Amazon e-commerce tables
            "customers_accounts": "Core customer accounts with authentication credentials, profile data, and account status. Stores customer history and is used for identity verification, account verification, and security check workflows.",
            "customers_addresses": "Customer shipping and billing addresses linked to accounts with default address selection.",
            "customers_payment_methods": "Stored payment methods (credit card, debit, PayPal) for customer accounts with card details.",
            "customers_preferences": "Customer preferences for language, currency, and notification settings.",
            "catalog_categories": "Hierarchical product category tree with parent-child relationships and category paths.",
            "catalog_products": "Product catalog with ASIN identifiers, pricing, brand, and category classification. Includes top product and popular items data; used alongside reviews to determine bestseller rankings.",
            "catalog_product_images": "Product image gallery storing URLs, display positions, and accessibility alt text.",
            "catalog_reviews": "Customer product reviews with star rating scores, review text, and verified purchase status. Used to identify bestseller products and popular items by aggregating customer review ratings.",
            "catalog_inventory": "Real-time stock levels per product per warehouse tracking quantity available, reserved, and reorder points.",
            "orders_carts": "Shopping cart sessions tracking active, converted, and abandoned carts for customer accounts.",
            "orders_cart_items": "Individual items in shopping carts with product references, quantities, and unit prices.",
            "orders_orders": "Customer purchase orders tracking order history and lifecycle from pending through processing, shipped, and delivered.",
            "orders_order_items": "Line items within orders linking products to orders with quantity, pricing, and item-level status. Key for 'also bought' and 'frequently bought together' recommendation analysis.",
            "orders_shipments": "Shipment tracking records with carrier, tracking number, ship date, and delivery status.",
            "orders_returns": "Product return requests with reason, approval status, and refund amount for returned order items.",
            "billing_refunds": "Refund transactions linked to payments and returns, tracking refund amount, reason, and processing status.",
            "billing_subscriptions": "Recurring subscription plans (Prime, Music, Kindle) with billing cycle, price, and next billing date.",
            "billing_subscription_payments": "Subscription billing history tracking periodic payments with period start/end dates.",
            "billing_gift_cards": "Gift card inventory with unique codes, balance tracking, purchaser and recipient information.",
            "billing_promotions": "Promotional discount codes with type (percentage/fixed), minimum order requirements, and usage limits.",
            "analytics_page_views": "Page view tracking with session, page type, referrer, and visitor IP for web analytics.",
            "analytics_search_queries": "Search query log tracking what customers search for, result counts, and which products they click. Supports search-to-purchase conversion rate analysis.",
            "analytics_click_events": "Click event stream capturing add-to-cart, buy-now, wishlist, and share interactions.",
            "analytics_conversion_funnels": "Conversion funnel definitions for measuring conversion rate, purchase funnel progression, and drop-off analysis from search to checkout.",
            "analytics_daily_metrics": "Pre-aggregated daily business metrics (revenue, orders, visitors, conversion rate) with dimensional breakdowns.",
            "analytics_ab_tests": "A/B test experiment results with variant performance metrics, sample sizes, and statistical significance.",
            "warehouse_warehouses": "Fulfillment center locations with warehouse codes, geographic location, and storage capacity.",
            "warehouse_picking_lists": "Order picking assignments linking orders to warehouses with worker assignment and pick status.",
            "warehouse_shipping_rates": "Carrier shipping rate tables by service level, weight range, and delivery zone.",
            "support_tickets": "Customer support tickets tracking customer history and open cases. Linked to accounts and orders with category, priority, and resolution status. Used for support requests and case management.",
            "support_ticket_messages": "Support ticket conversation messages from customers and agents with message body and timestamps.",
            "support_faq_articles": "Self-service FAQ knowledge base articles organized by category (returns, billing, shipping, account).",
        }
        return _SUMMARIES.get(
            table_name,
            "Database table storing structured records for application data management and querying.",
        )

    def _mock_concept_aliases(self, prompt: str) -> str:
        return json.dumps({
            "user": ["customer", "client", "account holder", "member"],
            "email": ["email address", "contact email", "e-mail"],
            "order": ["purchase", "transaction", "buy"],
            "payment": ["transaction", "charge", "settlement", "remittance"],
            "invoice": ["bill", "receipt", "statement"],
            "product": ["item", "merchandise", "goods", "sku"],
            "event": ["action", "interaction", "activity", "tracking event"],
            "session": ["login session", "auth session", "user session"],
            "role": ["permission", "access level", "authorization"],
            "revenue": ["income", "sales", "earnings", "proceeds"],
            "discount": ["coupon", "promotion", "promo code", "voucher"],
            "funnel": ["conversion path", "user journey", "pipeline"],
            "ip": ["IP address", "client IP", "remote address"],
            "phone": ["telephone", "mobile number", "contact number"],
            "name": ["full name", "person name", "display name"],
            "address": ["location", "street address", "mailing address"],
            "price": ["cost", "rate", "amount", "fee"],
            "status": ["state", "condition", "phase"],
            "created": ["creation date", "date added", "registered"],
            "updated": ["last modified", "modification date", "changed"],
        })

    def _mock_schema_narrative(self, prompt: str) -> str:
        if "auth" in prompt.lower():
            return "The authentication schema implements user identity management with RBAC. Users register with email/password, receive role assignments, and create sessions upon login. Sessions track client metadata for security auditing."
        elif "billing" in prompt.lower():
            return "The billing schema implements a standard e-commerce order lifecycle: users create orders containing product line items, which generate invoices settled via payments. Discount codes can be applied at the order level. The flow is: order \u2192 order_items \u2192 invoice \u2192 payment."
        elif "analytics" in prompt.lower():
            return "The analytics schema captures user behavior and business metrics. Raw interaction events feed into pre-aggregated daily revenue summaries. Conversion funnels define multi-step user journeys for measuring drop-off rates."
        elif "inventory" in prompt.lower():
            return "The inventory schema manages physical goods across warehouse locations. Stock levels are tracked per location, with transfers between warehouses and purchase orders from suppliers. Returns and adjustments maintain accurate counts."
        elif "support" in prompt.lower():
            return "The support schema handles customer service workflows. Tickets flow through SLA-governed queues, with agent teams, knowledge base articles for self-service, and escalation paths for complex issues."
        else:
            return "This schema organizes related database tables for a specific business domain, with foreign key relationships defining the data flow between entities."

    def _mock_column_purposes(self, prompt: str) -> str:
        # Return a JSON dict of column_name -> purpose
        purposes = {}
        if "auth_users" in prompt:
            purposes = {
                "id": "Unique user identifier, auto-incremented primary key",
                "email": "User's email address, used for login and notifications",
                "name": "User's display name shown in the UI",
                "phone": "Optional contact phone number",
                "password_hash": "Bcrypt-hashed password for authentication",
                "is_active": "Account activation status, false for deactivated accounts",
                "created_at": "Timestamp of account registration",
                "updated_at": "Timestamp of last profile modification",
            }
        elif "billing_orders" in prompt:
            purposes = {
                "id": "Unique order identifier",
                "user_id": "Reference to the customer who placed the order",
                "total": "Order total amount after discounts",
                "status": "Order lifecycle state: pending \u2192 confirmed \u2192 shipped \u2192 delivered",
                "discount_id": "Applied discount code, null if no discount",
                "created_at": "Timestamp when order was placed",
                "updated_at": "Timestamp of last status change",
            }
        else:
            # Generic: extract column names from prompt and give generic purposes
            purposes = {"_default": "Stores relevant data for this table's domain"}
        return json.dumps(purposes)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_provider(provider_name: str, api_key: str, model: str | None = None) -> LLMProvider:
    """Factory to create a provider by name.

    Uses agentlib's unified ``call_llm`` for all real providers.
    MockProvider is available for testing without API keys.
    """
    if provider_name == "mock":
        return MockProvider()

    if provider_name in ("anthropic", "openai", "gemini", "google", "xai", "deepseek"):
        return AgentlibProvider(provider=provider_name, api_key=api_key, model=model)

    raise ValueError(f"Unknown provider '{provider_name}'. Available: anthropic, openai, gemini, google, xai, deepseek, mock")
