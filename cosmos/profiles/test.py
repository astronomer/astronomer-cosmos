from __future__ import annotations
from typing import Any, Optional, Literal
from pydantic import BaseModel

from typing import TYPE_CHECKING

from cosmos.log import get_logger

DBT_PROFILE_TYPE_FIELD = "type"
DBT_PROFILE_METHOD_FIELD = "method"

logger = get_logger(__name__)


class DbtProfileConfigVars(BaseModel):
    send_anonymous_usage_stats: Optional[bool] = False
    partial_parse: Optional[bool] = None
    use_experimental_parser: Optional[bool] = None
    static_parser: Optional[bool] = None
    printer_width: Optional[int] = None
    write_json: Optional[bool] = None
    warn_error: Optional[bool] = None
    warn_error_options: Optional[dict[Literal["include", "exclude"], Any]] = None
    log_format: Optional[Literal["text", "json", "default"]] = None
    debug: Optional[bool] = None
    version_check: Optional[bool] = None

    def as_dict(self) -> Optional[dict[str, Any]]:
        try:
            result = self.dict()
        except AttributeError:
            result = self.model_dump()

        result = {field_name: result[field_name] for field_name in result if result[field_name] is not None}
        if result != {}:
            return result
        return None


base = DbtProfileConfigVars().as_dict()
print(base)
