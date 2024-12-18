from ..base import BaseProfileMapping


class SnowflakeBaseProfileMapping(BaseProfileMapping):

    def transform_account(self, account: str) -> str:
        """Transform the account to the format <account>.<region> if it's not already."""
        region = self.conn.extra_dejson.get("region")
        if region and region not in account:
            account = f"{account}.{region}"

        return str(account)
