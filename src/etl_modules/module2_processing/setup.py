"""
setup.py для module2_processing
"""
from setuptools import setup, find_packages

setup(
    name="georetail-module2",
    version="2.0.0",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary>=2.9.7",
        "pyyaml>=6.0.1",
        "python-dotenv>=1.0.0",
        "structlog>=23.2.0",
    ],
    extras_require={
        "fuzzy": ["fuzzywuzzy[speedup]>=0.18.0"],
        "fast": ["rapidfuzz>=3.0.0"],
        "dev": ["pytest>=7.4.0", "black>=23.0.0", "isort>=5.12.0"],
    },
    python_requires=">=3.8",
)