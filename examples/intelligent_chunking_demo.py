"""
Example demonstrating intelligent chunking analysis with zarrify.

This example shows how zarrify can automatically recommend optimal
chunking strategies based on your data dimensions and access patterns.
"""

import numpy as np
import pandas as pd
import xarray as xr
from zarrify.chunking import get_chunk_recommendation, validate_chunking


def main():
    """Demonstrate intelligent chunking analysis."""
    print("Zarrify Intelligent Chunking Analysis")
    print("=" * 40)
    
    # Example 1: Climate data dimensions
    print("\n1. Climate Data Analysis:")
    print("-" * 25)
    
    # Define dimensions for a typical climate dataset
    climate_dims = {
        "time": 3650,  # 10 years of daily data
        "lat": 721,    # High resolution latitude
        "lon": 1440,   # High resolution longitude
    }
    
    # Analyze for different access patterns
    for pattern in ["temporal", "spatial", "balanced"]:
        print(f"\n{pattern.capitalize()} access pattern:")
        recommendation = get_chunk_recommendation(
            dimensions=climate_dims,
            dtype_size_bytes=4,  # float32
            access_pattern=pattern
        )
        
        print(f"  Recommended chunks: {recommendation.chunks}")
        print(f"  Estimated chunk size: {recommendation.estimated_chunk_size_mb:.1f} MB")
        
        if recommendation.warnings:
            for warning in recommendation.warnings:
                print(f"  Warning: {warning}")
        
        for note in recommendation.notes:
            print(f"  Note: {note}")
    
    # Example 2: Validation of user-provided chunking
    print("\n\n2. User Chunking Validation:")
    print("-" * 28)
    
    # User-provided chunking that might not be optimal
    user_chunks = {
        "time": 1000,  # Very large time chunks
        "lat": 1,      # Very small spatial chunks
        "lon": 1,      # Very small spatial chunks
    }
    
    print(f"User-provided chunks: {user_chunks}")
    
    validation = validate_chunking(
        user_chunks=user_chunks,
        dimensions=climate_dims,
        dtype_size_bytes=4
    )
    
    print(f"Chunk size: {validation['chunk_size_mb']:.1f} MB")
    
    if validation["warnings"]:
        print("Warnings:")
        for warning in validation["warnings"]:
            print(f"  - {warning}")
    else:
        print("No warnings for this chunking strategy")
    
    if validation["recommendations"]:
        print("Recommendations:")
        for rec in validation["recommendations"]:
            print(f"  - {rec}")
    
    # Example 3: Better chunking
    print("\n\n3. Improved Chunking:")
    print("-" * 20)
    
    # Better chunking based on validation feedback
    better_chunks = {
        "time": 100,   # Moderate time chunks
        "lat": 100,    # Larger spatial chunks
        "lon": 100,    # Larger spatial chunks
    }
    
    print(f"Improved chunks: {better_chunks}")
    
    validation = validate_chunking(
        user_chunks=better_chunks,
        dimensions=climate_dims,
        dtype_size_bytes=4
    )
    
    print(f"Chunk size: {validation['chunk_size_mb']:.1f} MB")
    
    if validation["warnings"]:
        print("Warnings:")
        for warning in validation["warnings"]:
            print(f"  - {warning}")
    else:
        print("No warnings for this chunking strategy")
    
    # Example 4: Small dataset analysis
    print("\n\n4. Small Dataset Analysis:")
    print("-" * 25)
    
    # Define dimensions for a small dataset
    small_dims = {
        "time": 100,
        "level": 10,
        "lat": 50,
        "lon": 100,
    }
    
    print("Small dataset dimensions:")
    for dim, size in small_dims.items():
        print(f"  {dim}: {size}")
    
    recommendation = get_chunk_recommendation(
        dimensions=small_dims,
        dtype_size_bytes=8,  # float64
        access_pattern="balanced"
    )
    
    print(f"\nRecommended chunks: {recommendation.chunks}")
    print(f"Estimated chunk size: {recommendation.estimated_chunk_size_mb:.1f} MB")


if __name__ == "__main__":
    main()