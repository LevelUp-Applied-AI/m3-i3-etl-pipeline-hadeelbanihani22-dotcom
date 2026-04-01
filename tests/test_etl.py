"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
from etl_pipeline import transform,  validate


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    # TODO: Implement
   
  

    data = {
        "customers": pd.DataFrame({
            "customer_id": [1],
            "customer_name": ["Hadeel"],
            "city": ["Amman"]
        }),
        "products": pd.DataFrame({
            "product_id": [1],
            "category": ["Electronics"],
            "unit_price": [100]
        }),
        "orders": pd.DataFrame({
            "order_id": [1],
            "customer_id": [1],
            "status": ["cancelled"]   #هون الفكره انه نشيك على الكانسليشن
        }),
        "order_items": pd.DataFrame({
            "order_id": [1],
            "product_id": [1],
            "quantity": [1]
        })
    }

    result = transform(data)

    assert result.empty


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    # TODO: Implement
    

    data = {
        "customers": pd.DataFrame({
            "customer_id": [1],
            "customer_name": ["Hadeel"],
            "city": ["Amman"]
        }),
        "products": pd.DataFrame({
            "product_id": [1],
            "category": ["Electronics"],
            "unit_price": [100]
        }),
        "orders": pd.DataFrame({
            "order_id": [1],
            "customer_id": [1],
            "status": ["completed"]
        }),
        "order_items": pd.DataFrame({
            "order_id": [1],
            "product_id": [1],
            "quantity": [200]   #  suspicious
        })
    }

    result = transform(data)

    assert result.empty


def test_validate_catches_nulls():
    """Confirm validate() raises ValueError when nulls exist"""

    import pandas as pd
    from etl_pipeline import validate

    df = pd.DataFrame({
        "customer_id": [None],
        "customer_name": ["Hadeel"],
        "total_orders": [1],
        "total_revenue": [100],
        "avg_order_value": [100],
        "top_category": ["Electronics"]
    })

    try:
        validate(df)
        assert False, "Expected ValueError but none was raised"
    except ValueError:
        assert True
