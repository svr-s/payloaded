"""Unit test verifying payloaded package initialization and version."""

import payloaded as pld


def test_package_version():
    """Verify that payloaded exposes the correct version string."""
    assert pld.__version__ == "0.2.1"
