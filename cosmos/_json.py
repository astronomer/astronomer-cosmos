from __future__ import annotations

import json as _json
from typing import IO, Any

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover
    _orjson = None  # type: ignore[assignment]


JSONDecodeError = _json.JSONDecodeError
decoder = _json.decoder


def _use_orjson() -> bool:
    """Return *True* when orjson should be used based on Cosmos settings."""
    from cosmos import settings

    if not settings.enable_orjson_parser:
        return False
    if _orjson is None:
        raise ImportError(
            "orjson is not installed but enable_orjson_parser is enabled. "
            "Install it with: pip install 'astronomer-cosmos[orjson]'"
        )
    return True


def loads(s: str | bytes | bytearray | memoryview, **kwargs: Any) -> Any:  # type: ignore[type-arg]
    """Deserialize a JSON string/bytes to a Python object."""
    if _use_orjson():
        return _orjson.loads(s)  # type: ignore[union-attr]
    return _json.loads(s, **kwargs)  # type: ignore[arg-type]


def load(fp: IO[str] | IO[bytes], **kwargs: Any) -> Any:
    """Deserialize a JSON file-like object to a Python object."""
    if _use_orjson():
        return _orjson.loads(fp.read())  # type: ignore[union-attr]
    return _json.load(fp, **kwargs)


def _orjson_option(sort_keys: bool = False, indent: int | None = None) -> int | None:
    """Build the ``option`` bitmask for :func:`orjson.dumps`."""
    option = 0
    if sort_keys:
        option |= _orjson.OPT_SORT_KEYS  # type: ignore[union-attr]
    if indent is not None:
        option |= _orjson.OPT_INDENT_2  # type: ignore[union-attr]
    return option or None


def dumps(
    obj: Any,
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    separators: tuple[str, str] | None = None,
    **kwargs: Any,
) -> bytes | str:
    """Serialize *obj* to JSON.

    Returns ``bytes`` when orjson is active, ``str`` otherwise.
    Prefer :func:`dumps_bytes` or :func:`dumps_str` when you need a
    guaranteed return type.
    """
    if _use_orjson():
        return _orjson.dumps(obj, option=_orjson_option(sort_keys, indent))  # type: ignore[union-attr,no-any-return]
    return _json.dumps(obj, sort_keys=sort_keys, indent=indent, separators=separators, **kwargs)


def dumps_bytes(
    obj: Any,
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    separators: tuple[str, str] | None = None,
    **kwargs: Any,
) -> bytes:
    """Serialize *obj* to JSON **bytes**.  Efficient for both backends."""
    if _use_orjson():
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
    """Serialize *obj* to a JSON **string**.  Convenient for both backends."""
    if _use_orjson():
        return _orjson.dumps(obj, option=_orjson_option(sort_keys, indent)).decode()  # type: ignore[union-attr,no-any-return]
    return _json.dumps(obj, sort_keys=sort_keys, indent=indent, separators=separators, **kwargs)


def dump(obj: Any, fp: IO[str], **kwargs: Any) -> None:
    """Serialize *obj* as JSON and write to the file-like *fp*.

    Always writes ``str`` -- file handles are typically opened in text mode.
    """
    if _use_orjson():
        fp.write(_orjson.dumps(obj).decode())  # type: ignore[union-attr]
    else:
        _json.dump(obj, fp, **kwargs)
