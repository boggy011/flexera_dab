"""
Validation utilities for configuration and data quality.

Provides schema validation, configuration validation,
and data quality checks.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from datalib.utils.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


logger = get_logger(__name__)


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


def validate_config(config: Dict[str, Any], required_fields: List[str]) -> bool:
    """
    Validate configuration has required fields.
    
    Args:
        config: Configuration dictionary
        required_fields: List of required field names (supports dot notation)
        
    Returns:
        True if valid
        
    Raises:
        ValidationError: If validation fails
    """
    missing = []
    
    for field in required_fields:
        parts = field.split(".")
        value = config
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                missing.append(field)
                break
    
    if missing:
        raise ValidationError(f"Missing required fields: {missing}")
    
    return True


def validate_schema(
    df: "DataFrame",
    expected_schema: "StructType",
    strict: bool = False
) -> Dict[str, Any]:
    """
    Validate DataFrame schema against expected schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected Spark schema
        strict: If True, schemas must match exactly
        
    Returns:
        Dictionary with validation results
    """
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    
    result = {
        "valid": True,
        "missing_columns": [],
        "extra_columns": [],
        "type_mismatches": [],
    }
    
    # Check for missing columns
    for name, dtype in expected_fields.items():
        if name not in actual_fields:
            result["missing_columns"].append(name)
            result["valid"] = False
        elif str(actual_fields[name]) != str(dtype):
            result["type_mismatches"].append({
                "column": name,
                "expected": str(dtype),
                "actual": str(actual_fields[name]),
            })
            if strict:
                result["valid"] = False
    
    # Check for extra columns
    if strict:
        for name in actual_fields:
            if name not in expected_fields:
                result["extra_columns"].append(name)
                result["valid"] = False
    
    return result


def run_quality_checks(
    df: "DataFrame",
    checks: List[Dict[str, Any]],
    fail_on_error: bool = False
) -> Dict[str, Any]:
    """
    Run data quality checks on DataFrame.
    
    Supported check types:
    - not_null: Check column is not null
    - unique: Check column values are unique
    - range: Check values in numeric range
    - regex: Check values match regex pattern
    - values: Check values in allowed list
    - custom: Custom SQL expression
    
    Args:
        df: DataFrame to validate
        checks: List of check configurations
        fail_on_error: Raise exception on first failure
        
    Returns:
        Dictionary with check results
    """
    from pyspark.sql import functions as F
    
    results = {
        "passed": 0,
        "failed": 0,
        "checks": [],
        "validated_df": df,
    }
    
    total_rows = df.count()
    
    for check in checks:
        check_type = check.get("type")
        column = check.get("column")
        check_name = check.get("name", f"{check_type}_{column}")
        
        check_result = {
            "name": check_name,
            "type": check_type,
            "column": column,
            "passed": False,
            "failed_count": 0,
            "message": "",
        }
        
        try:
            if check_type == "not_null":
                failed_count = df.filter(F.col(column).isNull()).count()
                check_result["passed"] = failed_count == 0
                check_result["failed_count"] = failed_count
                check_result["message"] = f"{failed_count} null values found"
                
            elif check_type == "unique":
                duplicate_count = df.groupBy(column).count().filter(F.col("count") > 1).count()
                check_result["passed"] = duplicate_count == 0
                check_result["failed_count"] = duplicate_count
                check_result["message"] = f"{duplicate_count} duplicate groups found"
                
            elif check_type == "range":
                min_val = check.get("min")
                max_val = check.get("max")
                
                condition = F.lit(True)
                if min_val is not None:
                    condition = condition & (F.col(column) >= min_val)
                if max_val is not None:
                    condition = condition & (F.col(column) <= max_val)
                
                failed_count = df.filter(~condition).count()
                check_result["passed"] = failed_count == 0
                check_result["failed_count"] = failed_count
                check_result["message"] = f"{failed_count} values out of range [{min_val}, {max_val}]"
                
            elif check_type == "regex":
                pattern = check.get("pattern")
                failed_count = df.filter(~F.col(column).rlike(pattern)).count()
                check_result["passed"] = failed_count == 0
                check_result["failed_count"] = failed_count
                check_result["message"] = f"{failed_count} values don't match pattern"
                
            elif check_type == "values":
                allowed_values = check.get("allowed", [])
                failed_count = df.filter(~F.col(column).isin(allowed_values)).count()
                check_result["passed"] = failed_count == 0
                check_result["failed_count"] = failed_count
                check_result["message"] = f"{failed_count} values not in allowed list"
                
            elif check_type == "custom":
                expression = check.get("expression")
                failed_count = df.filter(~F.expr(expression)).count()
                check_result["passed"] = failed_count == 0
                check_result["failed_count"] = failed_count
                check_result["message"] = f"{failed_count} rows failed custom check"
                
            elif check_type == "row_count":
                min_rows = check.get("min", 0)
                max_rows = check.get("max", float("inf"))
                check_result["passed"] = min_rows <= total_rows <= max_rows
                check_result["failed_count"] = 0 if check_result["passed"] else 1
                check_result["message"] = f"Row count: {total_rows} (expected: {min_rows}-{max_rows})"
                
            else:
                check_result["message"] = f"Unknown check type: {check_type}"
                check_result["failed_count"] = 0
            
            # Apply threshold if specified
            threshold = check.get("threshold", 0)
            if threshold > 0 and check_result["failed_count"] > 0:
                failure_rate = check_result["failed_count"] / total_rows
                check_result["passed"] = failure_rate <= threshold
            
        except Exception as e:
            check_result["message"] = f"Check error: {str(e)}"
            check_result["passed"] = False
        
        if check_result["passed"]:
            results["passed"] += 1
        else:
            results["failed"] += 1
            if fail_on_error:
                raise ValidationError(f"Quality check failed: {check_name} - {check_result['message']}")
        
        results["checks"].append(check_result)
        logger.info(f"Quality check '{check_name}': {'PASSED' if check_result['passed'] else 'FAILED'}")
    
    return results


def check_data_freshness(
    df: "DataFrame",
    timestamp_column: str,
    max_age_hours: int = 24
) -> Dict[str, Any]:
    """
    Check data freshness based on timestamp column.
    
    Args:
        df: DataFrame to check
        timestamp_column: Column containing timestamps
        max_age_hours: Maximum allowed age in hours
        
    Returns:
        Dictionary with freshness check results
    """
    from pyspark.sql import functions as F
    from datetime import datetime, timedelta
    
    cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
    
    latest_record = df.agg(F.max(timestamp_column)).collect()[0][0]
    oldest_record = df.agg(F.min(timestamp_column)).collect()[0][0]
    
    is_fresh = latest_record and latest_record >= cutoff_time
    
    return {
        "is_fresh": is_fresh,
        "latest_record": str(latest_record),
        "oldest_record": str(oldest_record),
        "max_age_hours": max_age_hours,
        "cutoff_time": str(cutoff_time),
    }
