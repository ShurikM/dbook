from .base import ScenarioSpec, ScenarioResult  # type: ignore[import-not-found]
from .billing import BILLING_SCENARIOS  # type: ignore[import-not-found]
from .care import CARE_SCENARIOS  # type: ignore[import-not-found]
from .sales import SALES_SCENARIOS  # type: ignore[import-not-found]

ALL_SCENARIOS = BILLING_SCENARIOS + CARE_SCENARIOS + SALES_SCENARIOS
