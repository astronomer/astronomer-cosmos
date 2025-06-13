from __future__ import annotations

import base64
import binascii
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

    def _decode_private_key_content(self, private_key_content: str) -> str:
        """
        Decodes the private key content from either base64-encoded or plain-text PEM format.

        Starting from `apache-airflow-providers-snowflake` version 6.3.0, the provider expects the
        `private_key_content` to be base64-encoded rather than raw PEM text. This method ensures
        compatibility by attempting to decode the content from base64 first. If decoding fails,
        the original content is assumed to be plain-text PEM (as used in older versions).

        This allows backward compatibility while supporting the new expected format.

        Args:
            private_key_content: The private key content, either base64 encoded or plain-text PEM

        Returns:
            The decoded private key in plain-text PEM format
        """
        try:
            return base64.b64decode(private_key_content).decode("utf-8")
        except (UnicodeDecodeError, binascii.Error):
            return private_key_content
