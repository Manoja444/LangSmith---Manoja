"""
Test settings module for the LS Run Handler.

This module provides a function to load test-specific settings.
"""
from ls_py_handler.config.settings import Settings


def get_test_settings() -> Settings:
    """
    Get settings configured for the test environment.

    Returns:
        Settings: Settings instance configured with test values
    """
    return Settings(_env_file=".env.test")
