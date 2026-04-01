"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """

    data = {
        "customers":pd.read_sql("select * from customers", engine),
          "order_items":pd.read_sql("select * from order_items", engine),
          "orders":pd.read_sql("select * from orders", engine),
          "products":pd.read_sql("select * from products", engine)
        }

    return data
   


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    # TODO: Implement transformation
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # 1. Join
    df = orders.merge(order_items, on="order_id") \
               .merge(products, on="product_id")

    # 2. Compute line_total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # 3. Filter cancelled
    df = df[df["status"] != "cancelled"]

    # 4. Filter suspicious quantities (FIXED)
    df = df[df["quantity"] <= 100]

    # 5. Join customers
    df = df.merge(customers, on="customer_id")

    # 6. Aggregations
    total_orders = df.groupby("customer_id")["order_id"].nunique()
    total_revenue = df.groupby("customer_id")["line_total"].sum()
    avg_order_value = total_revenue / total_orders

    # 7. Top category
    category = df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_category = category.loc[
        category.groupby("customer_id")["line_total"].idxmax()
    ]

    # 8. Final summary
    summary = customers[["customer_id", "customer_name", "city"]] \
        .merge(total_orders.rename("total_orders"), on="customer_id") \
        .merge(total_revenue.rename("total_revenue"), on="customer_id") \
        .merge(avg_order_value.rename("avg_order_value"), on="customer_id") \
        .merge(top_category[["customer_id", "category"]], on="customer_id")

    summary = summary.rename(columns={"category": "top_category"})

    return summary



def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    # TODO: Implement validation
    results = {}

    # 1. No nulls
    results["no_nulls"] = df["customer_id"].notnull().all() and df["customer_name"].notnull().all()

    # 2. total_revenue > 0
    results["positive_revenue"] = (df["total_revenue"] > 0).all()

    # 3. No duplicate customer_id
    results["no_duplicates"] = not df["customer_id"].duplicated().any()

    # 4. total_orders > 0
    results["valid_orders"] = (df["total_orders"] > 0).all()

    # Print results
    for check, status in results.items():
        print(f"{check}: {'Data validation: PASS' if status else 'FAIL'}")

    # Raise error if any check fails
    if not all(results.values()):
        raise ValueError("Data validation failed")

    return results


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    # TODO: Implement loading
    

    #Load customer summary to PostgreSQL table and CSV file.

    # 1. Load to PostgreSQL
    df.to_sql(
        "customer_analytics",   # اسم الجدول الجديد
        engine,
        if_exists="replace",    # يحذف القديم ويعمل جديد
        index=False
    )

    # 2. تأكد أن الفولدر موجود
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # 3. حفظ CSV
    df.to_csv(csv_path, index=False)

    # 4. طباعة عدد الصفوف
    print(f"Loaded {len(df)} rows into database and CSV")


def main():
    """Orchestrate the ETL pipeline"""

    print("Starting ETL pipeline...")

    # 1. Create engine
    DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    
    engine = create_engine(DATABASE_URL)

    # 2. Extract
    print("Extracting data...")
    data = extract(engine)

    # 3. Transform

    print("Transforming data...")
    df = transform(data)

    # 4. Validate
    print("Validating data...")
    validate(df)

    # 5. Load
    print("Loading data...")
    load(df, engine, "output/customer_analytics.csv")

    print("ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()
