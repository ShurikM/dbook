from .base_agent import BaseAgent  # type: ignore[reportMissingImports]  # noqa: F401
from .billing_agent import BillingAgent  # type: ignore[reportMissingImports]
from .care_agent import CareAgent  # type: ignore[reportMissingImports]
from .sales_agent import SalesAgent  # type: ignore[reportMissingImports]

__all__ = ["BaseAgent", "BillingAgent", "CareAgent", "SalesAgent", "AGENT_MAP"]

AGENT_MAP = {
    "billing": BillingAgent,
    "care": CareAgent,
    "sales": SalesAgent,
}
