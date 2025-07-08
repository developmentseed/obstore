from obstore.store import Path


def test_path_percent_encoded():
    path = Path.parse("foo/hello%20world.txt")
    assert str(path) == "foo/hello%20world.txt"
