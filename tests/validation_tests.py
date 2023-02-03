def test_version(package):
    import configparser

    import requests

    config = configparser.RawConfigParser()
    config.read_file(open(rf"{package}/setup.cfg"))
    local_version = config.get("metadata", "version")
    releases = requests.get(f"https://pypi.org/pypi/{package}/json").json()["releases"]
    shipped_versions = []
    [shipped_versions.append(version) for version, details in releases.items()]

    assert local_version not in shipped_versions
