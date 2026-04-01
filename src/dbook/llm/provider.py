"""LLM provider abstraction with implementations."""

from __future__ import annotations

import json
import logging
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class LLMProvider(Protocol):
    """Abstract LLM provider interface."""

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        """Send a prompt and return the completion text."""
        ...


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
        # Extract table name from prompt
        if "auth_users" in prompt:
            return "Core user accounts table storing authentication credentials, profile information, and account status. Created during user registration and referenced by sessions, orders, and analytics events."
        elif "billing_orders" in prompt:
            return "Customer purchase orders tracking order lifecycle from creation through fulfillment. Links customers to their purchases with status tracking and discount application."
        elif "billing_payments" in prompt:
            return "Payment transaction records for settled invoices. Tracks payment method, amount, and processing timestamp."
        elif "billing_invoices" in prompt:
            return "Invoice records generated from customer orders. Tracks billing contact, amounts, and payment status through draft-sent-paid lifecycle."
        elif "analytics_events" in prompt:
            return "User interaction event log capturing page views, clicks, purchases, and signups. Used for behavioral analytics and conversion tracking."
        elif "analytics_daily_revenue" in prompt:
            return "Pre-aggregated daily revenue metrics summarizing order volume and average order values. Used for financial dashboards and trend analysis."
        elif "auth_sessions" in prompt:
            return "Active user login sessions tracking authentication tokens, client IP addresses, and session expiry. Used for session management and security monitoring."
        elif "auth_roles" in prompt:
            return "Role-based access control (RBAC) role definitions. Contains the available permission roles that can be assigned to users."
        elif "billing_products" in prompt:
            return "Product catalog containing items available for purchase. Stores pricing, categorization, and availability status."
        elif "billing_order_items" in prompt:
            return "Individual line items within customer orders. Links products to orders with quantity and unit pricing."
        elif "billing_discounts" in prompt:
            return "Promotional discount codes with percentage-based savings. Includes validity periods and usage limits."
        elif "analytics_funnels" in prompt:
            return "Conversion funnel definitions tracking multi-step user journeys. Measures drop-off rates between funnel stages."
        else:
            # Generic summary for any table
            return "Database table storing structured records for application data management and querying."

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
            return "The billing schema implements a standard e-commerce order lifecycle: users create orders containing product line items, which generate invoices settled via payments. Discount codes can be applied at the order level. The flow is: order → order_items → invoice → payment."
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
                "status": "Order lifecycle state: pending → confirmed → shipped → delivered",
                "discount_id": "Applied discount code, null if no discount",
                "created_at": "Timestamp when order was placed",
                "updated_at": "Timestamp of last status change",
            }
        else:
            # Generic: extract column names from prompt and give generic purposes
            purposes = {"_default": "Stores relevant data for this table's domain"}
        return json.dumps(purposes)


class AnthropicProvider:
    """Anthropic Claude provider."""

    def __init__(self, api_key: str, model: str = "claude-haiku-4-5-20251001"):
        self.api_key = api_key
        self.model = model

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        try:
            import anthropic  # type: ignore[import-untyped]
            client = anthropic.Anthropic(api_key=self.api_key)
            response = client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text
        except ImportError:
            raise ImportError("anthropic package required. Install with: pip install anthropic")


class OpenAIProvider:
    """OpenAI provider."""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        try:
            import openai  # type: ignore[import-untyped]  # pyright: ignore[reportMissingImports]
            client = openai.OpenAI(api_key=self.api_key)
            response = client.chat.completions.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.choices[0].message.content or ""
        except ImportError:
            raise ImportError("openai package required. Install with: pip install openai")


class GeminiProvider:
    """Google Gemini provider."""

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        self.api_key = api_key
        self.model = model

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        try:
            import requests
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
            response = requests.post(
                url,
                params={"key": self.api_key},
                json={"contents": [{"parts": [{"text": prompt}]}],
                       "generationConfig": {"maxOutputTokens": max_tokens}},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except ImportError:
            raise ImportError("requests package required for Gemini provider")


def create_provider(provider_name: str, api_key: str, model: str | None = None) -> LLMProvider:
    """Factory to create a provider by name."""
    providers = {
        "anthropic": AnthropicProvider,
        "openai": OpenAIProvider,
        "gemini": GeminiProvider,
        "mock": MockProvider,
    }

    if provider_name not in providers:
        raise ValueError(f"Unknown provider '{provider_name}'. Available: {list(providers.keys())}")

    cls = providers[provider_name]
    if provider_name == "mock":
        return cls()

    kwargs = {"api_key": api_key}
    if model:
        kwargs["model"] = model
    return cls(**kwargs)
