"""Renders an LLM diagnosis as plain text for the task logs and the exception message."""

from __future__ import annotations

import textwrap
from typing import Any

from pydantic import BaseModel

# Long unwrapped paragraphs force horizontal scrolling in log viewers.
_WIDTH = 88
_DIVIDER = "-" * 80
_INDENT = "  "


def format_diagnosis(diagnosis: BaseModel) -> str:
    """
    Render a diagnosis as a wrapped, clearly delimited block.

    ``diagnosis`` can be Cosmos's built-in ``DbtFailureDiagnosis`` or a user model set with
    ``AiConfig.diagnosis_output_type``, so the output is driven by the model's fields and not by hardcoded names. A
    ``confidence`` field, if present, goes to the header, and every other field becomes a section. Nested lists,
    dicts and models are rendered recursively, one item per line.
    """
    fields = diagnosis.model_dump(mode="json")
    confidence = fields.pop("confidence", None)
    header = "AI diagnosis" if confidence is None else f"AI diagnosis (confidence: {confidence})"

    lines = [_DIVIDER, header, _DIVIDER]
    for field_name, value in fields.items():
        lines.append(f"{field_name.replace('_', ' ').capitalize()}:")
        lines.extend(_render(value, level=1))
        lines.append("")
    lines.append(_DIVIDER)
    return "\n".join(lines)


def _render(value: Any, level: int) -> list[str]:
    """Render a value as wrapped lines, indented by ``level``."""
    pad = _INDENT * level
    if isinstance(value, dict):
        return _render_dict(value, level, pad)
    if isinstance(value, list):
        return _render_list(value, level, pad)
    return _wrap(str(value), pad, pad)


def _render_dict(value: dict[str, Any], level: int, pad: str) -> list[str]:
    if not value:
        return [f"{pad}(none)"]
    lines: list[str] = []
    for key, item in value.items():
        if _is_nested(item):
            lines.append(f"{pad}{key}:")
            lines.extend(_render(item, level + 1))
        else:
            lines.extend(_wrap(f"{key}: {_scalar(item)}", pad, pad + _INDENT))
    return lines


def _render_list(value: list[Any], level: int, pad: str) -> list[str]:
    if not value:
        return [f"{pad}(none)"]
    lines: list[str] = []
    for item in value:
        if _is_nested(item):
            nested = _render(item, level + 1)
            # Put the bullet on the first line of the nested block, so the rest stays aligned under it.
            nested[0] = f"{pad}- {nested[0].lstrip()}"
            lines.extend(nested)
        else:
            lines.extend(_wrap(f"- {_scalar(item)}", pad, pad + _INDENT))
    return lines


def _scalar(value: Any) -> str:
    return "(none)" if isinstance(value, (dict, list)) else str(value)


def _is_nested(value: Any) -> bool:
    return isinstance(value, (dict, list)) and bool(value)


def _wrap(text: str, initial_indent: str, subsequent_indent: str) -> list[str]:
    """
    Wrap each line of ``text`` on its own, so the line breaks and indentation of a SQL or YAML snippet survive.

    Long tokens, such as table names, paths and URLs, are kept whole: a line that is too long is better than a token
    that can't be copied.
    """
    wrapped: list[str] = []
    for index, line in enumerate(text.splitlines() or [""]):
        wrapped.extend(
            textwrap.wrap(
                line,
                width=_WIDTH,
                initial_indent=initial_indent if index == 0 else subsequent_indent,
                subsequent_indent=subsequent_indent,
                replace_whitespace=False,
                break_long_words=False,
                break_on_hyphens=False,
            )
            or [""]  # a blank line, which ``textwrap`` drops
        )
    return wrapped
