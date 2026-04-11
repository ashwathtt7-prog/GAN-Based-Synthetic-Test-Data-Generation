"""
Small relational demo database generator.
Creates a compact 4-table SQLite source that is easy to demo live.
"""

import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text

fake = Faker("en_US")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DB_PATH = Path("datasets/demo_showcase.db")
DDL_PATH = Path("datasets/ddl/demo_showcase_domain.sql")


def setup_database():
    if DB_PATH.exists():
        DB_PATH.unlink()

    engine = create_engine(f"sqlite:///{DB_PATH}")
    sql = DDL_PATH.read_text()
    statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    return engine


def build_customers(count: int = 120) -> pd.DataFrame:
    segments = ["Consumer", "Small Business", "Enterprise"]
    rows = []
    for customer_id in range(1, count + 1):
        created_at = fake.date_between(start_date="-2y", end_date="-30d")
        rows.append({
            "DEMO_CUSTOMER_ID": customer_id,
            "CUSTOMER_NAME": fake.name(),
            "CUSTOMER_EMAIL": fake.email(),
            "CUSTOMER_SEGMENT": random.choices(segments, weights=[0.65, 0.25, 0.10])[0],
            "CUSTOMER_CITY": fake.city(),
            "CUSTOMER_CREATED_AT": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def build_products(count: int = 18) -> pd.DataFrame:
    categories = ["Hardware", "Software", "Subscription", "Services"]
    rows = []
    for product_id in range(1, count + 1):
        rows.append({
            "DEMO_PRODUCT_ID": product_id,
            "PRODUCT_NAME": f"{random.choice(['Starter', 'Pro', 'Elite', 'Flex'])} {random.choice(['Kit', 'Bundle', 'Suite', 'License'])} {product_id}",
            "PRODUCT_CATEGORY": random.choice(categories),
            "UNIT_PRICE": round(random.uniform(19, 499), 2),
            "IS_ACTIVE": random.choices(["Y", "N"], weights=[0.9, 0.1])[0],
            "PRODUCT_CREATED_AT": fake.date_between(start_date="-3y", end_date="-60d").strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def build_orders(customers: pd.DataFrame, products: pd.DataFrame, count: int = 320) -> tuple[pd.DataFrame, pd.DataFrame]:
    order_rows = []
    item_rows = []
    order_statuses = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
    channels = ["Web", "Partner", "Field Sales"]
    item_id = 1

    product_prices = products.set_index("DEMO_PRODUCT_ID")["UNIT_PRICE"].to_dict()
    product_ids = products["DEMO_PRODUCT_ID"].tolist()
    customer_ids = customers["DEMO_CUSTOMER_ID"].tolist()

    for order_id in range(1, count + 1):
        order_date = fake.date_time_between(start_date="-180d", end_date="-2d")
        status = random.choices(order_statuses, weights=[0.2, 0.25, 0.45, 0.1])[0]
        ship_date = None
        if status in {"SHIPPED", "DELIVERED"}:
            ship_date = order_date + timedelta(days=random.randint(1, 7))

        num_items = random.randint(1, 4)
        line_total_sum = 0.0
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            unit_price = float(product_prices[product_id])
            discount_pct = random.choice([0, 0, 5, 10, 15])
            line_total = round(quantity * unit_price * (1 - discount_pct / 100), 2)
            line_total_sum += line_total
            item_rows.append({
                "DEMO_ORDER_ITEM_ID": item_id,
                "DEMO_ORDER_ID": order_id,
                "DEMO_PRODUCT_ID": product_id,
                "QUANTITY": quantity,
                "UNIT_PRICE": unit_price,
                "LINE_TOTAL": line_total,
                "DISCOUNT_PCT": float(discount_pct),
            })
            item_id += 1

        order_rows.append({
            "DEMO_ORDER_ID": order_id,
            "DEMO_CUSTOMER_ID": random.choice(customer_ids),
            "ORDER_NUMBER": f"ORD-{2026}{order_id:05d}",
            "ORDER_STATUS": status,
            "ORDER_DATE": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "SHIP_DATE": ship_date.strftime("%Y-%m-%d %H:%M:%S") if ship_date else None,
            "ORDER_TOTAL": round(line_total_sum, 2),
            "SALES_CHANNEL": random.choice(channels),
        })

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)


def main():
    logger.info("Creating demo showcase database at %s", DB_PATH)
    engine = setup_database()
    customers = build_customers()
    products = build_products()
    orders, order_items = build_orders(customers, products)

    customers.to_sql("DEMO_CUSTOMER", engine, if_exists="append", index=False)
    products.to_sql("DEMO_PRODUCT", engine, if_exists="append", index=False)
    orders.to_sql("DEMO_ORDER", engine, if_exists="append", index=False)
    order_items.to_sql("DEMO_ORDER_ITEM", engine, if_exists="append", index=False)

    logger.info("Demo database created with %s customers, %s products, %s orders, %s order items",
                len(customers), len(products), len(orders), len(order_items))


if __name__ == "__main__":
    main()
