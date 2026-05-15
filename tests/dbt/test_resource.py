import pytest

from cosmos.dbt.resource import get_resource_name_from_unique_id


class TestGetResourceNameFromUniqueId:
    def test_plain_model(self):
        assert get_resource_name_from_unique_id("model.my_pkg.my_model") == "my_model"

    def test_versioned_model_preserves_version_suffix(self):
        assert get_resource_name_from_unique_id("model.my_pkg.my_model.v1") == "my_model.v1"

    @pytest.mark.parametrize("malformed", ["", "foo", "foo.bar"])
    def test_malformed_unique_id_raises(self, malformed):
        with pytest.raises(IndexError):
            get_resource_name_from_unique_id(malformed)
