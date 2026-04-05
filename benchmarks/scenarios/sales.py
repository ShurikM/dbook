"""Sales agent scenarios."""

from .base import ScenarioSpec  # type: ignore[import-not-found]

SALES_SCENARIOS = [
    ScenarioSpec(
        id="S1",
        agent_type="sales",
        question=(
            "Customer is browsing the Electronics category. "
            "Show the top 10 highest-rated products that are currently in stock."
        ),
        expected_tables=[
            "catalog.products", "catalog.reviews", "catalog.categories",
            "catalog.inventory",
        ],
        expected_columns=["title", "price", "rating", "quantity"],
        expected_facts=[
            "title", "rating", "price", "quantity", "category",
        ],
        golden_sql="""
            SELECT p.id, p.title, p.price,
                   AVG(r.rating) AS avg_rating, COUNT(r.id) AS review_count,
                   SUM(ci.quantity - ci.reserved) AS total_available
            FROM catalog.products p
            JOIN catalog.categories c ON p.category_id = c.id
            LEFT JOIN catalog.reviews r ON r.product_id = p.id
            LEFT JOIN catalog.inventory ci ON ci.product_id = p.id
            WHERE (c.name ILIKE '%electronics%' OR c.path ILIKE '%Electronics%')
              AND p.status = 'active'
            GROUP BY p.id
            HAVING SUM(ci.quantity - ci.reserved) > 0
            ORDER BY avg_rating DESC NULLS LAST
            LIMIT 10
        """,
        difficulty="hard",
    ),
    ScenarioSpec(
        id="S2",
        agent_type="sales",
        question=(
            "Check if product with ASIN B00X4WHP5E is available. "
            "Show inventory levels across all warehouses, reserved quantity, and reorder status."
        ),
        expected_tables=["catalog.products", "catalog.inventory", "warehouse.warehouses"],
        expected_columns=["quantity", "reserved", "reorder_point", "warehouse"],
        expected_facts=[
            "B00X4WHP5E", "quantity", "reserved", "reorder_point",
        ],
        golden_sql="""
            SELECT w.name AS warehouse_name, w.code AS warehouse_code,
                   ci.quantity, ci.reserved,
                   ci.quantity - ci.reserved AS available,
                   ci.reorder_point,
                   CASE WHEN ci.quantity - ci.reserved <= ci.reorder_point
                        THEN 'reorder needed' ELSE 'sufficient'
                   END AS stock_status
            FROM catalog.products p
            JOIN catalog.inventory ci ON ci.product_id = p.id
            JOIN warehouse.warehouses w ON ci.warehouse_id = w.id
            WHERE p.asin = 'B00X4WHP5E'
            ORDER BY w.name
        """,
        difficulty="easy",
    ),
    ScenarioSpec(
        id="S3",
        agent_type="sales",
        question=(
            "Customer has items in cart #9876. "
            "Calculate the subtotal, check if all items are in stock, "
            "and estimate tax based on applicable rates."
        ),
        expected_tables=[
            "orders.carts", "orders.cart_items", "catalog.products",
            "catalog.inventory",
        ],
        expected_columns=["product_id", "quantity", "unit_price"],
        expected_facts=[
            "9876", "product", "quantity", "price",
        ],
        golden_sql="""
            SELECT ci.product_id, p.title, ci.quantity, ci.unit_price,
                   ci.quantity * ci.unit_price AS line_total,
                   COALESCE(SUM(inv.quantity - inv.reserved), 0) AS total_stock,
                   CASE WHEN COALESCE(SUM(inv.quantity - inv.reserved), 0) >= ci.quantity
                        THEN 'in stock' ELSE 'insufficient'
                   END AS stock_status
            FROM orders.cart_items ci
            JOIN catalog.products p ON ci.product_id = p.id
            LEFT JOIN catalog.inventory inv ON inv.product_id = p.id
            WHERE ci.cart_id = 9876
            GROUP BY ci.id, ci.product_id, p.title, ci.quantity, ci.unit_price
        """,
        difficulty="medium",
    ),
    ScenarioSpec(
        id="S4",
        agent_type="sales",
        question=(
            "Find products that are frequently bought together with product #234. "
            "Use order history to find co-occurring products, ranked by frequency."
        ),
        expected_tables=["orders.order_items", "orders.orders", "catalog.products"],
        expected_columns=["product_id", "title"],
        expected_facts=[
            "product_id", "234", "count", "title",
        ],
        golden_sql="""
            SELECT p.id, p.title, p.price, COUNT(*) AS co_occurrence
            FROM orders.order_items oi2
            JOIN catalog.products p ON oi2.product_id = p.id
            WHERE oi2.order_id IN (
                SELECT oi.order_id FROM orders.order_items oi WHERE oi.product_id = 234
            )
            AND oi2.product_id != 234
            GROUP BY p.id, p.title, p.price
            ORDER BY co_occurrence DESC
            LIMIT 10
        """,
        difficulty="hard",
    ),
    ScenarioSpec(
        id="S5",
        agent_type="sales",
        question=(
            "Customer is searching for 'wireless headphones under $100'. "
            "Find matching products with their average review ratings."
        ),
        expected_tables=["catalog.products", "catalog.reviews"],
        expected_columns=["title", "price", "rating"],
        expected_facts=[
            "title", "price", "rating", "wireless",
        ],
        golden_sql="""
            SELECT p.id, p.title, p.price, b.name AS brand,
                   AVG(r.rating) AS avg_rating, COUNT(r.id) AS review_count
            FROM catalog.products p
            LEFT JOIN catalog.brands b ON p.brand_id = b.id
            LEFT JOIN catalog.reviews r ON r.product_id = p.id
            WHERE (p.title ILIKE '%wireless%headphone%'
                   OR p.description ILIKE '%wireless%headphone%')
              AND p.price < 100
              AND p.status = 'active'
            GROUP BY p.id, p.title, p.price, b.name
            ORDER BY avg_rating DESC NULLS LAST, review_count DESC
        """,
        difficulty="medium",
    ),
]
