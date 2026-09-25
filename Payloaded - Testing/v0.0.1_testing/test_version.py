"""Unit test verifying payloaded package initialization and version."""

import payloaded as pld


def test_package_version():
    """Verify that payloaded exposes the correct initial version string."""
    assert pld.__version__ == "0.0.1"
