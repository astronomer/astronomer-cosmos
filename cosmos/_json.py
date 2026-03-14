"""JSON wrapper that uses orjson when ``enable_orjson_parser`` is True, falling back to stdlib json.

orjson only supports indent=2 (OPT_INDENT_2); any other indent value falls back to stdlib automatically.
"""

from __future__ import annotations

import json as _json
from typing import IO, Any

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover
    _orjson = None  # type: ignore[assignment]

JSONDecodeError = _json.JSONDecodeError
decoder = _json.decoder

# orjson only supports None (compact) and 2 (OPT_INDENT_2)
_ORJSON_SUPPORTED_INDENTS = (None, 2)


def _use_orjson() -> bool:
    from cosmos import settings  # deferred to avoid circular imports

    if not settings.enable_orjson_parser:
        return False
    if _orjson is None:
        raise ImportError(
            "orjson is not installed but enable_orjson_parser is enabled. "
            "Install it with: pip install 'astronomer-cosmos[orjson]'"
        )
    return True


def _orjson_option(sort_keys: bool = False, indent: int | None = None) -> int:
    option = 0
    if sort_keys:
        option |= _orjson.OPT_SORT_KEYS  # type: ignore[union-attr]
    if indent is not None:
        option |= _orjson.OPT_INDENT_2  # type: ignore[union-attr]
    return option


def loads(s: str | bytes | bytearray | memoryview, **kwargs: Any) -> Any:  # type: ignore[type-arg]
    if _use_orjson() and not kwargs:
        return _orjson.loads(s)  # type: ignore[union-attr]
    return _json.loads(s, **kwargs)  # type: ignore[arg-type]


def load(fp: IO[str] | IO[bytes], **kwargs: Any) -> Any:
    if _use_orjson() and not kwargs:
        return _orjson.loads(fp.read())  # type: ignore[union-attr]
    return _json.load(fp, **kwargs)


def dumps(
    obj: Any,
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    separators: tuple[str, str] | None = None,
    **kwargs: Any,
) -> str:
    """Compatibility wrapper; always returns str. Use dumps_bytes() for bytes."""
    return dumps_str(obj, sort_keys=sort_keys, indent=indent, separators=separators, **kwargs)


def dumps_bytes(
    obj: Any,
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    separators: tuple[str, str] | None = None,
    **kwargs: Any,
) -> bytes:
    if _use_orjson() and indent in _ORJSON_SUPPORTED_INDENTS:
        return _orjson.dumps(obj, option=_orjson_option(sort_keys, indent))  # type: ignore[union-attr,no-any-return]
    return _json.dumps(obj, sort_keys=sort_keys, indent=indent, separators=separators, **kwargs).encode()


def dumps_str(
    obj: Any,
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    separators: tuple[str, str] | None = None,
    **kwargs: Any,
) -> str:
    if _use_orjson() and indent in _ORJSON_SUPPORTED_INDENTS:
        return _orjson.dumps(obj, option=_orjson_option(sort_keys, indent)).decode()  # type: ignore[union-attr,no-any-return]
    return _json.dumps(obj, sort_keys=sort_keys, indent=indent, separators=separators, **kwargs)


def dump(
    obj: Any,
    fp: IO[str],
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    **kwargs: Any,
) -> None:
    if _use_orjson() and indent in _ORJSON_SUPPORTED_INDENTS:
        fp.write(_orjson.dumps(obj, option=_orjson_option(sort_keys, indent)).decode())  # type: ignore[union-attr]
    else:
        _json.dump(obj, fp, sort_keys=sort_keys, indent=indent, **kwargs)
