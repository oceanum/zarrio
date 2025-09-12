"""
Example demonstrating configurable target chunk sizes in zarrify.

This example shows how to configure target chunk sizes for different environments:
- Local development (smaller chunks)
- Production servers (medium chunks)
- Cloud environments (larger chunks)
"""

import os
from zarrify.chunking import get_chunk_recommendation


def demonstrate_configurable_chunking():
    """Demonstrate how to configure target chunk sizes."""
    print("CONFIGURABLE TARGET CHUNK SIZES")
    print("=" * 35)
    
    # Sample dataset dimensions
    dimensions = {
        "time": 1000,
        "lat": 500,
        "lon": 1000,
    }
    
    print(f"Dataset dimensions: {dimensions}")
    
    # 1. Default target chunk size (50 MB)
    print("\n1. Default target chunk size (50 MB):")
    recommendation = get_chunk_recommendation(
        dimensions=dimensions,
        dtype_size_bytes=4,
        access_pattern="balanced"
    )
    print(f"   Recommended chunks: {recommendation.chunks}")
    print(f"   Estimated chunk size: {recommendation.estimated_chunk_size_mb:.0f} MB")
    
    # 2. Custom target chunk size as function argument (10 MB for local)
    print("\n2. Custom target chunk size (10 MB for local development):")
    recommendation = get_chunk_recommendation(
        dimensions=dimensions,
        dtype_size_bytes=4,
        access_pattern="balanced",
        target_chunk_size_mb=10
    )
    print(f"   Recommended chunks: {recommendation.chunks}")
    print(f"   Estimated chunk size: {recommendation.estimated_chunk_size_mb:.0f} MB")
    
    # 3. Custom target chunk size as environment variable (200 MB for cloud)
    print("\n3. Custom target chunk size via environment variable (200 MB for cloud):")
    os.environ['ZARRIFY_TARGET_CHUNK_SIZE_MB'] = '200'
    
    # Need to recreate the analyzer to pick up the environment variable
    recommendation = get_chunk_recommendation(
        dimensions=dimensions,
        dtype_size_bytes=4,
        access_pattern="balanced"
    )
    print(f"   Recommended chunks: {recommendation.chunks}")
    print(f"   Estimated chunk size: {recommendation.estimated_chunk_size_mb:.0f} MB")
    
    # Clean up environment variable
    del os.environ['ZARRIFY_TARGET_CHUNK_SIZE_MB']
    
    # 4. Different targets for different access patterns
    print("\n4. Environment-specific recommendations:")
    targets = {
        "Local development": 10,
        "Production servers": 50,
        "Cloud environments": 200
    }
    
    for environment, target_size in targets.items():
        recommendation = get_chunk_recommendation(
            dimensions=dimensions,
            dtype_size_bytes=4,
            access_pattern="balanced",
            target_chunk_size_mb=target_size
        )
        print(f"   {environment} ({target_size} MB target): {recommendation.estimated_chunk_size_mb:.0f} MB")


def demonstrate_zarrconverter_config():
    """Demonstrate how to configure target chunk size in ZarrConverter."""
    print("\n\nZARRCONVERTER CONFIGURATION")
    print("=" * 28)
    
    print("You can also configure target chunk size in ZarrConverterConfig:")
    print("""
from zarrify import ZarrConverter
from zarrify.models import ZarrConverterConfig

# Configure target chunk size in the config
config = ZarrConverterConfig(
    target_chunk_size_mb=100,  # 100 MB target chunks
    chunking={"time": 50, "lat": 100, "lon": 200}
)

converter = ZarrConverter(config=config)
""")
    
    print("This allows you to set environment-specific chunking strategies")
    print("in your application configuration files.")


def main():
    """Main function to demonstrate configurable chunk sizes."""
    print("Zarrify Configurable Target Chunk Sizes")
    print("=" * 40)
    
    demonstrate_configurable_chunking()
    demonstrate_zarrconverter_config()
    
    print("\nKEY TAKEAWAYS:")
    print("=" * 15)
    print("1. Target chunk size is configurable for different environments")
    print("2. Use function arguments for programmatic control")
    print("3. Use environment variables for deployment-specific configuration")
    print("4. Configure in ZarrConverterConfig for application-level settings")
    print("5. Recommended sizes:")
    print("   - Local development: 10-25 MB")
    print("   - Production servers: 50-100 MB")
    print("   - Cloud environments: 100-200 MB")


if __name__ == "__main__":
    main()