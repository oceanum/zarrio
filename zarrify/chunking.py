"""
Chunking analysis and validation for zarrify.
"""

import logging
import math
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ChunkRecommendation:
    """Recommendation for chunking strategy."""
    chunks: Dict[str, int]
    strategy: str
    estimated_chunk_size_mb: float
    warnings: list
    notes: list


class ChunkAnalyzer:
    """Analyzes and recommends chunking strategies for climate data."""
    
    # Target chunk size ranges (MB)
    MIN_CHUNK_SIZE_MB = 1
    TARGET_CHUNK_SIZE_MB = 50
    MAX_CHUNK_SIZE_MB = 100
    
    # Warning thresholds
    SMALL_CHUNK_WARNING_MB = 1
    LARGE_CHUNK_WARNING_MB = 100
    
    def __init__(self):
        """Initialize chunk analyzer."""
        pass
    
    def analyze_chunking(
        self,
        dimensions: Dict[str, int],
        dtype_size_bytes: int = 4,  # float32 default
        access_pattern: str = "balanced"
    ) -> ChunkRecommendation:
        """
        Analyze dimensions and recommend chunking strategy.
        
        Args:
            dimensions: Dictionary of dimension names and sizes
            dtype_size_bytes: Size of data type in bytes (default: 4 for float32)
            access_pattern: Expected access pattern ("temporal", "spatial", "balanced")
            
        Returns:
            ChunkRecommendation with recommended strategy
        """
        warnings = []
        notes = []
        
        # Calculate total data size
        total_elements = 1
        for size in dimensions.values():
            total_elements *= size
        
        total_size_mb = total_elements * dtype_size_bytes / (1024**2)
        
        # Recommend chunking strategy
        if access_pattern == "temporal":
            chunks, chunk_size_mb = self._recommend_temporal_chunking(
                dimensions, dtype_size_bytes
            )
            strategy = "temporal_focus"
            notes.append("Optimized for time series analysis")
        elif access_pattern == "spatial":
            chunks, chunk_size_mb = self._recommend_spatial_chunking(
                dimensions, dtype_size_bytes
            )
            strategy = "spatial_focus"
            notes.append("Optimized for spatial analysis")
        else:  # balanced
            chunks, chunk_size_mb = self._recommend_balanced_chunking(
                dimensions, dtype_size_bytes
            )
            strategy = "balanced"
            notes.append("Balanced for mixed access patterns")
        
        # Check for warnings
        if chunk_size_mb < self.SMALL_CHUNK_WARNING_MB:
            warnings.append(
                f"Chunk size ({chunk_size_mb:.1f} MB) is very small. "
                f"Consider increasing for better I/O performance."
            )
        
        if chunk_size_mb > self.LARGE_CHUNK_WARNING_MB:
            warnings.append(
                f"Chunk size ({chunk_size_mb:.1f} MB) is large. "
                f"This may cause memory issues with limited systems."
            )
        
        # Add general notes
        notes.append(f"Total dataset size: {total_size_mb:.1f} MB")
        notes.append(f"Recommended chunk size: {chunk_size_mb:.1f} MB")
        
        return ChunkRecommendation(
            chunks=chunks,
            strategy=strategy,
            estimated_chunk_size_mb=chunk_size_mb,
            warnings=warnings,
            notes=notes
        )
    
    def _recommend_temporal_chunking(
        self,
        dimensions: Dict[str, int],
        dtype_size_bytes: int
    ) -> Tuple[Dict[str, int], float]:
        """Recommend chunking optimized for temporal access."""
        chunks = {}
        time_dim = self._find_time_dimension(dimensions)
        
        if time_dim:
            # Favor larger time chunks for temporal analysis
            time_size = dimensions[time_dim]
            time_chunk = min(100, max(10, time_size // 10))  # 10% of time steps
            chunks[time_dim] = time_chunk
            
            # Calculate spatial chunks to achieve target chunk size
            # For N spatial dimensions, we want: time_chunk * spatial_chunk^N * dtype_size_bytes ≈ target_elements
            target_elements = int(
                self.TARGET_CHUNK_SIZE_MB * (1024**2) // dtype_size_bytes
            )
            
            # Count spatial dimensions
            spatial_dims = [dim for dim in dimensions.keys() if dim != time_dim]
            num_spatial_dims = len(spatial_dims)
            
            if num_spatial_dims > 0:
                # Calculate spatial chunk size to achieve target
                spatial_elements_per_dim = target_elements / time_chunk
                spatial_chunk_per_dim = int(spatial_elements_per_dim ** (1/num_spatial_dims))
                
                # Apply spatial chunks, but respect dimension limits
                for dim in spatial_dims:
                    size = dimensions[dim]
                    # Use the smaller of calculated chunk or dimension size
                    spatial_chunk = min(spatial_chunk_per_dim, size)
                    # Ensure minimum chunk size
                    spatial_chunk = max(10, spatial_chunk)
                    chunks[dim] = spatial_chunk
            else:
                # No spatial dimensions
                pass
        else:
            # No time dimension, distribute evenly
            chunks = self._even_chunk_distribution(dimensions, dtype_size_bytes)
        
        chunk_size_mb = self._calculate_chunk_size_mb(chunks, dimensions, dtype_size_bytes)
        return chunks, chunk_size_mb
    
    def _recommend_spatial_chunking(
        self,
        dimensions: Dict[str, int],
        dtype_size_bytes: int
    ) -> Tuple[Dict[str, int], float]:
        """Recommend chunking optimized for spatial access."""
        chunks = {}
        time_dim = self._find_time_dimension(dimensions)
        
        if time_dim:
            # Smaller time chunks for spatial analysis
            time_size = dimensions[time_dim]
            time_chunk = min(20, max(5, time_size // 50))  # 2% of time steps
            chunks[time_dim] = time_chunk
            
            # Calculate spatial chunks to achieve target chunk size
            # For N spatial dimensions, we want: time_chunk * spatial_chunk^N * dtype_size_bytes ≈ target_elements
            target_elements = int(
                self.TARGET_CHUNK_SIZE_MB * (1024**2) // dtype_size_bytes
            )
            
            # Count spatial dimensions
            spatial_dims = [dim for dim in dimensions.keys() if dim != time_dim]
            num_spatial_dims = len(spatial_dims)
            
            if num_spatial_dims > 0:
                # Calculate spatial chunk size to achieve target
                spatial_elements_per_dim = target_elements / time_chunk
                spatial_chunk_per_dim = int(spatial_elements_per_dim ** (1/num_spatial_dims))
                
                # Apply spatial chunks with larger minimum sizes for spatial access
                for dim in spatial_dims:
                    size = dimensions[dim]
                    # Use the smaller of calculated chunk or dimension size
                    spatial_chunk = min(spatial_chunk_per_dim, size)
                    # Ensure larger minimum chunk size for spatial access
                    spatial_chunk = max(50, spatial_chunk)
                    chunks[dim] = spatial_chunk
            else:
                # No spatial dimensions
                pass
        else:
            # No time dimension, favor larger spatial chunks
            chunks = self._even_chunk_distribution(dimensions, dtype_size_bytes, large_chunks=True)
        
        chunk_size_mb = self._calculate_chunk_size_mb(chunks, dimensions, dtype_size_bytes)
        return chunks, chunk_size_mb
    
    def _recommend_balanced_chunking(
        self,
        dimensions: Dict[str, int],
        dtype_size_bytes: int
    ) -> Tuple[Dict[str, int], float]:
        """Recommend balanced chunking strategy."""
        chunks = {}
        time_dim = self._find_time_dimension(dimensions)
        
        if time_dim:
            # Balanced approach
            time_size = dimensions[time_dim]
            time_chunk = min(50, max(10, time_size // 20))  # 5% of time steps
            chunks[time_dim] = time_chunk
            
            # Calculate spatial chunks to achieve target chunk size
            # For N spatial dimensions, we want: time_chunk * spatial_chunk^N * dtype_size_bytes ≈ target_elements
            target_elements = int(
                self.TARGET_CHUNK_SIZE_MB * (1024**2) // dtype_size_bytes
            )
            
            # Count spatial dimensions
            spatial_dims = [dim for dim in dimensions.keys() if dim != time_dim]
            num_spatial_dims = len(spatial_dims)
            
            if num_spatial_dims > 0:
                # Calculate spatial chunk size to achieve target
                spatial_elements_per_dim = target_elements / time_chunk
                spatial_chunk_per_dim = int(spatial_elements_per_dim ** (1/num_spatial_dims))
                
                # Apply spatial chunks with moderate minimum sizes for balanced access
                for dim in spatial_dims:
                    size = dimensions[dim]
                    # Use the smaller of calculated chunk or dimension size
                    spatial_chunk = min(spatial_chunk_per_dim, size)
                    # Ensure moderate minimum chunk size for balanced access
                    spatial_chunk = max(30, spatial_chunk)
                    chunks[dim] = spatial_chunk
            else:
                # No spatial dimensions
                pass
        else:
            # No time dimension, moderate chunk sizes
            chunks = self._even_chunk_distribution(dimensions, dtype_size_bytes)
        
        chunk_size_mb = self._calculate_chunk_size_mb(chunks, dimensions, dtype_size_bytes)
        return chunks, chunk_size_mb
    
    def _find_time_dimension(self, dimensions: Dict[str, int]) -> Optional[str]:
        """Find the time dimension based on common names."""
        time_names = ["time", "t", "time_counter", "forecast_time", "datetime"]
        for dim in dimensions.keys():
            if dim.lower() in time_names:
                return dim
        return None
    
    def _even_chunk_distribution(
        self,
        dimensions: Dict[str, int],
        dtype_size_bytes: int,
        large_chunks: bool = False
    ) -> Dict[str, int]:
        """Distribute chunks evenly across all dimensions."""
        chunks = {}
        ndims = len(dimensions)
        
        # Aim for chunks of ~50MB
        target_elements = int(self.TARGET_CHUNK_SIZE_MB * (1024**2) // dtype_size_bytes)
        
        # Calculate chunk size per dimension
        elements_per_dim = int(target_elements ** (1/ndims))
        
        for dim, size in dimensions.items():
            if large_chunks:
                chunk_size = min(100, max(30, elements_per_dim))
            else:
                chunk_size = min(50, max(10, elements_per_dim))
            chunks[dim] = min(chunk_size, size)
        
        return chunks
    
    def _calculate_chunk_size_mb(
        self,
        chunks: Dict[str, int],
        dimensions: Dict[str, int],
        dtype_size_bytes: int
    ) -> float:
        """Calculate the actual chunk size in MB."""
        elements = 1
        for dim, chunk_size in chunks.items():
            if dim in dimensions:
                elements *= min(chunk_size, dimensions[dim])
        
        return elements * dtype_size_bytes / (1024**2)
    
    def validate_user_chunking(
        self,
        user_chunks: Dict[str, int],
        dimensions: Dict[str, int],
        dtype_size_bytes: int = 4
    ) -> Dict[str, Any]:
        """
        Validate user-provided chunking and provide feedback.
        
        Args:
            user_chunks: User-provided chunk sizes
            dimensions: Dataset dimensions
            dtype_size_bytes: Size of data type in bytes
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            "valid": True,
            "warnings": [],
            "recommendations": [],
            "chunk_size_mb": 0.0
        }
        
        # Calculate chunk size
        elements = 1
        for dim, chunk_size in user_chunks.items():
            if dim in dimensions:
                elements *= min(chunk_size, dimensions[dim])
            else:
                validation_results["warnings"].append(
                    f"Dimension '{dim}' not found in dataset"
                )
                validation_results["valid"] = False
        
        chunk_size_mb = elements * dtype_size_bytes / (1024**2)
        validation_results["chunk_size_mb"] = chunk_size_mb
        
        # Check chunk size limits
        if chunk_size_mb < self.MIN_CHUNK_SIZE_MB:
            validation_results["warnings"].append(
                f"Chunk size ({chunk_size_mb:.1f} MB) is below minimum recommended size "
                f"({self.MIN_CHUNK_SIZE_MB} MB). This may cause performance issues."
            )
        
        if chunk_size_mb > self.MAX_CHUNK_SIZE_MB:
            validation_results["warnings"].append(
                f"Chunk size ({chunk_size_mb:.1f} MB) exceeds maximum recommended size "
                f"({self.MAX_CHUNK_SIZE_MB} MB). This may cause memory issues."
            )
        
        # Check if chunks are too small relative to dimensions
        for dim, chunk_size in user_chunks.items():
            if dim in dimensions:
                dim_size = dimensions[dim]
                if chunk_size > dim_size:
                    validation_results["warnings"].append(
                        f"Chunk size ({chunk_size}) for dimension '{dim}' "
                        f"exceeds dimension size ({dim_size}). Will be clipped."
                    )
                elif chunk_size < dim_size / 100 and dim_size > 1000:
                    validation_results["recommendations"].append(
                        f"Consider increasing chunk size for dimension '{dim}' "
                        f"for better performance with large dimensions."
                    )
        
        return validation_results


# Global analyzer instance
_analyzer = ChunkAnalyzer()


def get_chunk_recommendation(
    dimensions: Dict[str, int],
    dtype_size_bytes: int = 4,
    access_pattern: str = "balanced"
) -> ChunkRecommendation:
    """
    Get chunking recommendation for given dimensions.
    
    Args:
        dimensions: Dictionary of dimension names and sizes
        dtype_size_bytes: Size of data type in bytes (default: 4 for float32)
        access_pattern: Expected access pattern ("temporal", "spatial", "balanced")
        
    Returns:
        ChunkRecommendation with recommended strategy
    """
    return _analyzer.analyze_chunking(dimensions, dtype_size_bytes, access_pattern)


def validate_chunking(
    user_chunks: Dict[str, int],
    dimensions: Dict[str, int],
    dtype_size_bytes: int = 4
) -> Dict[str, Any]:
    """
    Validate user-provided chunking.
    
    Args:
        user_chunks: User-provided chunk sizes
        dimensions: Dataset dimensions
        dtype_size_bytes: Size of data type in bytes
        
    Returns:
        Dictionary with validation results
    """
    return _analyzer.validate_user_chunking(user_chunks, dimensions, dtype_size_bytes)