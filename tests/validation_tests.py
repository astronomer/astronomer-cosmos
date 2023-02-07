def test_version():
    import requests

    from cosmos import __version__ as curr_version

    releases = requests.get("https://pypi.org/pypi/astronomer-cosmos/json").json()[
        "releases"
    ]
    shipped_versions = []
    [shipped_versions.append(version) for version, details in releases.items()]

    assert curr_version not in shipped_versions
