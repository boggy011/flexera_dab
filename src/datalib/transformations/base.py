"""
Base transformation classes and interfaces.

Provides the abstract base class for all transformations
and common utilities.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class TransformationError(Exception):
    """Raised when a transformation fails."""
    pass


class Transformation(ABC):
    """
    Abstract base class for all transformations.
    
    All transformations must implement the `transform` method.
    Transformations should be stateless and idempotent.
    
    Example:
        >>> class MyTransform(Transformation):
        ...     def __init__(self, column: str):
        ...         self.column = column
        ...
        ...     def transform(self, df: DataFrame) -> DataFrame:
        ...         return df.withColumn(self.column, upper(col(self.column)))
    """
    
    @abstractmethod
    def transform(self, df: "DataFrame") -> "DataFrame":
        """
        Apply transformation to DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
            
        Raises:
            TransformationError: If transformation fails
        """
        pass
    
    def validate(self, df: "DataFrame") -> bool:
        """
        Validate that transformation can be applied.
        
        Override this method to add validation logic.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if transformation can be applied
        """
        return True
    
    def __repr__(self) -> str:
        """Return string representation of transformation."""
        attrs = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}({attrs})"


class ChainedTransformation(Transformation):
    """
    Chain multiple transformations together.
    
    Example:
        >>> chain = ChainedTransformation([
        ...     RenameColumns({"old_name": "new_name"}),
        ...     CastColumns({"new_name": "string"}),
        ...     FilterRows("new_name IS NOT NULL"),
        ... ])
        >>> result = chain.transform(df)
    """
    
    def __init__(self, transformations: List[Transformation]):
        """
        Initialize chained transformation.
        
        Args:
            transformations: List of transformations to apply in order
        """
        self.transformations = transformations
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Apply all transformations in sequence."""
        result = df
        for transformation in self.transformations:
            result = transformation.transform(result)
        return result
    
    def append(self, transformation: Transformation) -> "ChainedTransformation":
        """
        Add a transformation to the chain.
        
        Args:
            transformation: Transformation to add
            
        Returns:
            Self for method chaining
        """
        self.transformations.append(transformation)
        return self


class ConditionalTransformation(Transformation):
    """
    Apply transformation conditionally based on a predicate.
    
    Example:
        >>> transform = ConditionalTransformation(
        ...     transformation=CastColumns({"amount": "double"}),
        ...     condition=lambda df: "amount" in df.columns
        ... )
    """
    
    def __init__(
        self,
        transformation: Transformation,
        condition: callable
    ):
        """
        Initialize conditional transformation.
        
        Args:
            transformation: Transformation to apply
            condition: Callable that takes DataFrame and returns bool
        """
        self.transformation = transformation
        self.condition = condition
    
    def transform(self, df: "DataFrame") -> "DataFrame":
        """Apply transformation if condition is met."""
        if self.condition(df):
            return self.transformation.transform(df)
        return df
