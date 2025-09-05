from pycof.about import _version


def test_version_exists():
    """Test that version is defined."""
    assert _version is not None
    assert isinstance(_version, str)
    assert len(_version.split(".")) >= 3
