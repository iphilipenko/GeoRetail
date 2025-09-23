"""
Pytest configuration for UC tests
"""

import pytest
from unittest.mock import Mock
from sqlalchemy.orm import Session


@pytest.fixture
def mock_db():
    """Mock database session"""
    return Mock(spec=Session)


@pytest.fixture
def mock_user():
    """Mock authenticated user"""
    return Mock(
        id=1,
        email="test@georetail.com",
        username="testuser",
        permissions=["core.view_map", "explorer.access", "screening.access", "comparison.access"]
    )
