"""
Common transformation implementations.

Provides a library of reusable transformations for common data
processing operations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType, DecimalType
)

from datalib.transformations.base import Transformation, TransformationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


# Type mapping for cast operations
TYPE_MAPPING = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


class AddTimestampColumn(Transformation):
    """
    Add a timestamp column with current time or specified value.
    
    Example:
        >>> trans = AddTimestampColumn(column_name="processed_at")
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        column_name: str = "ingestion_timestamp",
        value: Optional[datetime] = None,
        format: str = "yyyy-MM-dd HH:mm:ss"
    ):
        """
        Initialize transformation.
        
        Args:
            column_name: Name of timestamp column to add
            value: Specific timestamp value (uses current_timestamp if None)
            format: Timestamp format for string conversion
        """
        self.column_name = column_name
        self.value = value
        self.format = format
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Add timestamp column to DataFrame."""
        if self.value:
            return df.withColumn(
                self.column_name,
                F.lit(self.value).cast(TimestampType())
            )
        return df.withColumn(self.column_name, F.current_timestamp())


class CastColumns(Transformation):
    """
    Cast columns to specified data types.
    
    Example:
        >>> trans = CastColumns({
        ...     "amount": "double",
        ...     "count": "integer",
        ...     "is_active": "boolean"
        ... })
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        column_types: Dict[str, str],
        strict: bool = False
    ):
        """
        Initialize transformation.
        
        Args:
            column_types: Mapping of column names to target types
            strict: If True, raise error if column doesn't exist
        """
        self.column_types = column_types
        self.strict = strict
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Cast columns to specified types."""
        for column, type_name in self.column_types.items():
            if column not in df.columns:
                if self.strict:
                    raise TransformationError(f"Column not found: {column}")
                continue
            
            target_type = TYPE_MAPPING.get(type_name.lower())
            if target_type is None:
                # Try decimal with precision
                if type_name.lower().startswith("decimal"):
                    # Parse decimal(p,s) format
                    import re
                    match = re.match(r"decimal\((\d+),(\d+)\)", type_name.lower())
                    if match:
                        precision, scale = int(match.group(1)), int(match.group(2))
                        target_type = DecimalType(precision, scale)
                    else:
                        target_type = DecimalType(38, 10)
                else:
                    raise TransformationError(f"Unknown type: {type_name}")
            
            df = df.withColumn(column, F.col(column).cast(target_type))
        
        return df


class RenameColumns(Transformation):
    """
    Rename columns in DataFrame.
    
    Example:
        >>> trans = RenameColumns({
        ...     "old_name": "new_name",
        ...     "another_old": "another_new"
        ... })
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        column_mapping: Dict[str, str],
        strict: bool = False
    ):
        """
        Initialize transformation.
        
        Args:
            column_mapping: Mapping of old names to new names
            strict: If True, raise error if column doesn't exist
        """
        self.column_mapping = column_mapping
        self.strict = strict
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Rename columns according to mapping."""
        for old_name, new_name in self.column_mapping.items():
            if old_name not in df.columns:
                if self.strict:
                    raise TransformationError(f"Column not found: {old_name}")
                continue
            df = df.withColumnRenamed(old_name, new_name)
        return df


class FilterRows(Transformation):
    """
    Filter rows based on a SQL expression.
    
    Example:
        >>> trans = FilterRows("amount > 0 AND status = 'active'")
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        condition: str,
        negate: bool = False
    ):
        """
        Initialize transformation.
        
        Args:
            condition: SQL filter expression
            negate: If True, keep rows that don't match
        """
        self.condition = condition
        self.negate = negate
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Filter rows based on condition."""
        if self.negate:
            return df.filter(~F.expr(self.condition))
        return df.filter(self.condition)


class DeduplicateRows(Transformation):
    """
    Remove duplicate rows.
    
    Example:
        >>> trans = DeduplicateRows(
        ...     subset=["customer_id", "order_id"],
        ...     keep="first",
        ...     order_by=["created_at"]
        ... )
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        subset: Optional[List[str]] = None,
        keep: str = "first",
        order_by: Optional[List[str]] = None,
        order_desc: bool = True
    ):
        """
        Initialize transformation.
        
        Args:
            subset: Columns to consider for duplicates (all if None)
            keep: Which duplicate to keep ('first', 'last', 'none')
            order_by: Columns to order by when keeping first/last
            order_desc: Order descending (for keeping latest)
        """
        self.subset = subset
        self.keep = keep
        self.order_by = order_by
        self.order_desc = order_desc
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Remove duplicate rows."""
        if self.keep == "none":
            return df.dropDuplicates(self.subset)
        
        if self.order_by and self.keep in ("first", "last"):
            from pyspark.sql.window import Window
            
            partition_cols = self.subset or df.columns
            window = Window.partitionBy(partition_cols)
            
            order_cols = [
                F.col(c).desc() if self.order_desc else F.col(c)
                for c in self.order_by
            ]
            
            if self.keep == "last":
                order_cols = [
                    F.col(c) if self.order_desc else F.col(c).desc()
                    for c in self.order_by
                ]
            
            window = window.orderBy(*order_cols)
            
            return (
                df.withColumn("_row_num", F.row_number().over(window))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )
        
        return df.dropDuplicates(self.subset)


class SelectColumns(Transformation):
    """
    Select specific columns from DataFrame.
    
    Example:
        >>> trans = SelectColumns(["id", "name", "email"])
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        columns: List[str],
        strict: bool = False
    ):
        """
        Initialize transformation.
        
        Args:
            columns: List of column names to select
            strict: If True, raise error if column doesn't exist
        """
        self.columns = columns
        self.strict = strict
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Select specified columns."""
        if self.strict:
            missing = set(self.columns) - set(df.columns)
            if missing:
                raise TransformationError(f"Columns not found: {missing}")
        
        existing_columns = [c for c in self.columns if c in df.columns]
        return df.select(existing_columns)


class DropColumns(Transformation):
    """
    Drop specified columns from DataFrame.
    
    Example:
        >>> trans = DropColumns(["temp_column", "internal_id"])
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        columns: List[str],
        ignore_missing: bool = True
    ):
        """
        Initialize transformation.
        
        Args:
            columns: List of column names to drop
            ignore_missing: If True, ignore columns that don't exist
        """
        self.columns = columns
        self.ignore_missing = ignore_missing
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Drop specified columns."""
        for column in self.columns:
            if column in df.columns:
                df = df.drop(column)
            elif not self.ignore_missing:
                raise TransformationError(f"Column not found: {column}")
        return df


class FillNulls(Transformation):
    """
    Fill null values in specified columns.
    
    Example:
        >>> trans = FillNulls({
        ...     "amount": 0.0,
        ...     "status": "unknown",
        ...     "count": 0
        ... })
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        fill_values: Dict[str, Any],
        subset: Optional[List[str]] = None
    ):
        """
        Initialize transformation.
        
        Args:
            fill_values: Mapping of column names to fill values
            subset: Optional subset of columns (uses all if None)
        """
        self.fill_values = fill_values
        self.subset = subset
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Fill null values."""
        if self.subset:
            fill_values = {
                k: v for k, v in self.fill_values.items()
                if k in self.subset
            }
        else:
            fill_values = self.fill_values
        
        return df.fillna(fill_values)


class StandardizeStrings(Transformation):
    """
    Standardize string columns (trim, case, etc.).
    
    Example:
        >>> trans = StandardizeStrings(
        ...     columns=["name", "email"],
        ...     lowercase=True,
        ...     trim=True
        ... )
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        columns: List[str],
        trim: bool = True,
        lowercase: bool = False,
        uppercase: bool = False,
        remove_extra_spaces: bool = False
    ):
        """
        Initialize transformation.
        
        Args:
            columns: Columns to standardize
            trim: Trim leading/trailing whitespace
            lowercase: Convert to lowercase
            uppercase: Convert to uppercase
            remove_extra_spaces: Remove multiple consecutive spaces
        """
        self.columns = columns
        self.trim = trim
        self.lowercase = lowercase
        self.uppercase = uppercase
        self.remove_extra_spaces = remove_extra_spaces
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Standardize string columns."""
        for column in self.columns:
            if column not in df.columns:
                continue
            
            col_expr = F.col(column)
            
            if self.trim:
                col_expr = F.trim(col_expr)
            
            if self.lowercase:
                col_expr = F.lower(col_expr)
            elif self.uppercase:
                col_expr = F.upper(col_expr)
            
            if self.remove_extra_spaces:
                col_expr = F.regexp_replace(col_expr, r"\s+", " ")
            
            df = df.withColumn(column, col_expr)
        
        return df


class AddDerivedColumn(Transformation):
    """
    Add a derived column using a SQL expression.
    
    Example:
        >>> trans = AddDerivedColumn(
        ...     column_name="total_amount",
        ...     expression="quantity * unit_price"
        ... )
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        column_name: str,
        expression: str
    ):
        """
        Initialize transformation.
        
        Args:
            column_name: Name of new column
            expression: SQL expression for column value
        """
        self.column_name = column_name
        self.expression = expression
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Add derived column."""
        return df.withColumn(self.column_name, F.expr(self.expression))


class HashColumn(Transformation):
    """
    Create a hash column from multiple source columns.
    
    Useful for creating surrogate keys or change detection hashes.
    
    Example:
        >>> trans = HashColumn(
        ...     column_name="row_hash",
        ...     source_columns=["id", "name", "email"],
        ...     algorithm="sha256"
        ... )
        >>> df = trans.transform(df)
    """
    
    def __init__(
        self,
        column_name: str,
        source_columns: List[str],
        algorithm: str = "sha256"
    ):
        """
        Initialize transformation.
        
        Args:
            column_name: Name of hash column
            source_columns: Columns to include in hash
            algorithm: Hash algorithm (md5, sha1, sha256)
        """
        self.column_name = column_name
        self.source_columns = source_columns
        self.algorithm = algorithm
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Create hash column."""
        # Concatenate source columns
        concat_cols = F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in self.source_columns]
        )
        
        # Apply hash function
        if self.algorithm == "md5":
            hash_expr = F.md5(concat_cols)
        elif self.algorithm == "sha1":
            hash_expr = F.sha1(concat_cols)
        else:  # sha256
            hash_expr = F.sha2(concat_cols, 256)
        
        return df.withColumn(self.column_name, hash_expr)
