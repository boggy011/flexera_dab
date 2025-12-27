"""
Unit tests for the transformations module.
"""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import functions as F

from datalib.transformations.base import (
    Transformation,
    ChainedTransformation,
    ConditionalTransformation,
    TransformationError,
)
from datalib.transformations.common import (
    AddTimestampColumn,
    CastColumns,
    RenameColumns,
    FilterRows,
    DeduplicateRows,
    SelectColumns,
    DropColumns,
    FillNulls,
    StandardizeStrings,
)


class TestAddTimestampColumn:
    """Tests for AddTimestampColumn transformation."""
    
    def test_add_default_timestamp(self):
        """Test adding timestamp with default column name."""
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        
        transform = AddTimestampColumn()
        result = transform.transform(mock_df)
        
        mock_df.withColumn.assert_called_once()
        call_args = mock_df.withColumn.call_args
        assert call_args[0][0] == "ingestion_timestamp"
    
    def test_add_custom_column_name(self):
        """Test adding timestamp with custom column name."""
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        
        transform = AddTimestampColumn(column_name="created_at")
        result = transform.transform(mock_df)
        
        call_args = mock_df.withColumn.call_args
        assert call_args[0][0] == "created_at"


class TestCastColumns:
    """Tests for CastColumns transformation."""
    
    def test_cast_single_column(self):
        """Test casting a single column."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.withColumn.return_value = mock_df
        
        transform = CastColumns({"id": "string"})
        result = transform.transform(mock_df)
        
        mock_df.withColumn.assert_called_once()
    
    def test_cast_multiple_columns(self):
        """Test casting multiple columns."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "amount", "count"]
        mock_df.withColumn.return_value = mock_df
        
        transform = CastColumns({
            "id": "string",
            "amount": "double",
            "count": "integer"
        })
        result = transform.transform(mock_df)
        
        assert mock_df.withColumn.call_count == 3
    
    def test_cast_missing_column_not_strict(self):
        """Test that missing column is skipped when not strict."""
        mock_df = MagicMock()
        mock_df.columns = ["id"]
        mock_df.withColumn.return_value = mock_df
        
        transform = CastColumns({"missing_col": "string"}, strict=False)
        result = transform.transform(mock_df)
        
        mock_df.withColumn.assert_not_called()
    
    def test_cast_missing_column_strict(self):
        """Test that missing column raises error when strict."""
        mock_df = MagicMock()
        mock_df.columns = ["id"]
        
        transform = CastColumns({"missing_col": "string"}, strict=True)
        
        with pytest.raises(TransformationError) as exc_info:
            transform.transform(mock_df)
        assert "not found" in str(exc_info.value)


class TestRenameColumns:
    """Tests for RenameColumns transformation."""
    
    def test_rename_single_column(self):
        """Test renaming a single column."""
        mock_df = MagicMock()
        mock_df.columns = ["old_name"]
        mock_df.withColumnRenamed.return_value = mock_df
        
        transform = RenameColumns({"old_name": "new_name"})
        result = transform.transform(mock_df)
        
        mock_df.withColumnRenamed.assert_called_once_with("old_name", "new_name")
    
    def test_rename_multiple_columns(self):
        """Test renaming multiple columns."""
        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2"]
        mock_df.withColumnRenamed.return_value = mock_df
        
        transform = RenameColumns({
            "col1": "new_col1",
            "col2": "new_col2"
        })
        result = transform.transform(mock_df)
        
        assert mock_df.withColumnRenamed.call_count == 2


class TestFilterRows:
    """Tests for FilterRows transformation."""
    
    def test_filter_with_condition(self):
        """Test filtering with a condition."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        
        transform = FilterRows("amount > 0")
        result = transform.transform(mock_df)
        
        mock_df.filter.assert_called_once_with("amount > 0")
    
    def test_filter_negated(self):
        """Test filtering with negated condition."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        
        transform = FilterRows("status = 'deleted'", negate=True)
        result = transform.transform(mock_df)
        
        mock_df.filter.assert_called_once()


class TestDeduplicateRows:
    """Tests for DeduplicateRows transformation."""
    
    def test_deduplicate_all_columns(self):
        """Test deduplication on all columns."""
        mock_df = MagicMock()
        mock_df.dropDuplicates.return_value = mock_df
        
        transform = DeduplicateRows(keep="none")
        result = transform.transform(mock_df)
        
        mock_df.dropDuplicates.assert_called_once()
    
    def test_deduplicate_subset(self):
        """Test deduplication on subset of columns."""
        mock_df = MagicMock()
        mock_df.dropDuplicates.return_value = mock_df
        
        transform = DeduplicateRows(subset=["id", "name"], keep="none")
        result = transform.transform(mock_df)
        
        mock_df.dropDuplicates.assert_called_once_with(["id", "name"])


class TestSelectColumns:
    """Tests for SelectColumns transformation."""
    
    def test_select_existing_columns(self):
        """Test selecting existing columns."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "email"]
        mock_df.select.return_value = mock_df
        
        transform = SelectColumns(["id", "name"])
        result = transform.transform(mock_df)
        
        mock_df.select.assert_called_once_with(["id", "name"])
    
    def test_select_missing_column_strict(self):
        """Test error when selecting missing column in strict mode."""
        mock_df = MagicMock()
        mock_df.columns = ["id"]
        
        transform = SelectColumns(["id", "missing"], strict=True)
        
        with pytest.raises(TransformationError):
            transform.transform(mock_df)


class TestDropColumns:
    """Tests for DropColumns transformation."""
    
    def test_drop_existing_columns(self):
        """Test dropping existing columns."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "temp", "internal"]
        mock_df.drop.return_value = mock_df
        
        transform = DropColumns(["temp", "internal"])
        result = transform.transform(mock_df)
        
        assert mock_df.drop.call_count == 2
    
    def test_drop_missing_column_ignored(self):
        """Test that missing column is ignored by default."""
        mock_df = MagicMock()
        mock_df.columns = ["id"]
        mock_df.drop.return_value = mock_df
        
        transform = DropColumns(["missing"])
        result = transform.transform(mock_df)
        
        mock_df.drop.assert_not_called()


class TestChainedTransformation:
    """Tests for ChainedTransformation."""
    
    def test_chain_multiple_transformations(self):
        """Test chaining multiple transformations."""
        mock_df = MagicMock()
        
        # Create mock transformations
        trans1 = MagicMock(spec=Transformation)
        trans1.transform.return_value = mock_df
        
        trans2 = MagicMock(spec=Transformation)
        trans2.transform.return_value = mock_df
        
        chain = ChainedTransformation([trans1, trans2])
        result = chain.transform(mock_df)
        
        trans1.transform.assert_called_once()
        trans2.transform.assert_called_once()
    
    def test_append_transformation(self):
        """Test appending transformation to chain."""
        trans1 = MagicMock(spec=Transformation)
        chain = ChainedTransformation([trans1])
        
        trans2 = MagicMock(spec=Transformation)
        result = chain.append(trans2)
        
        assert len(chain.transformations) == 2
        assert result == chain  # Returns self for chaining


class TestConditionalTransformation:
    """Tests for ConditionalTransformation."""
    
    def test_apply_when_condition_true(self):
        """Test transformation applied when condition is true."""
        mock_df = MagicMock()
        mock_df.columns = ["target_col"]
        
        inner_trans = MagicMock(spec=Transformation)
        inner_trans.transform.return_value = mock_df
        
        conditional = ConditionalTransformation(
            transformation=inner_trans,
            condition=lambda df: "target_col" in df.columns
        )
        result = conditional.transform(mock_df)
        
        inner_trans.transform.assert_called_once()
    
    def test_skip_when_condition_false(self):
        """Test transformation skipped when condition is false."""
        mock_df = MagicMock()
        mock_df.columns = ["other_col"]
        
        inner_trans = MagicMock(spec=Transformation)
        
        conditional = ConditionalTransformation(
            transformation=inner_trans,
            condition=lambda df: "target_col" in df.columns
        )
        result = conditional.transform(mock_df)
        
        inner_trans.transform.assert_not_called()
        assert result == mock_df
