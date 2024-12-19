from __future__ import annotations

from typing import Any

from cosmos.profiles.base import BaseProfileMapping

DEFAULT_AWS_REGION = "us-west-2"


class SnowflakeBaseProfileMapping(BaseProfileMapping):

    @property
    def profile(self) -> dict[str, Any | None]:
        """Gets profile."""
        profile_vars = {
            **self.mapped_params,
            **self.profile_args,
        }
        return profile_vars

    def transform_account(self, account: str) -> str:
        """Transform the account to the format <account>.<region> if it's not already."""
        region = self.conn.extra_dejson.get("region")
        #
        if region and region != DEFAULT_AWS_REGION and region not in account:
            account = f"{account}.{region}"

        return str(account)
