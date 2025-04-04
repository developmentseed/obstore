from obstore import __version__, _object_store_source, _object_store_version


def test_versions_are_str():
    assert isinstance(__version__, str)
    assert isinstance(_object_store_version, str)
    assert isinstance(_object_store_source, str)
