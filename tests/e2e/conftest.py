import pytest

from cosmos import settings

from .warehouses import RECLASSIFIED_SETTING


@pytest.fixture(autouse=True, scope="session")
def _configure_cosmos_settings():
    original = (
        settings.enable_cache,
        settings.enable_cache_dbt_ls,
        settings.enable_cache_profile,
        getattr(settings, RECLASSIFIED_SETTING),
    )
    settings.enable_cache = False
    settings.enable_cache_dbt_ls = False
    settings.enable_cache_profile = False
    setattr(settings, RECLASSIFIED_SETTING, True)
    yield
    settings.enable_cache, settings.enable_cache_dbt_ls, settings.enable_cache_profile = original[:3]
    setattr(settings, RECLASSIFIED_SETTING, original[3])
