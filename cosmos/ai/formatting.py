"""Renders an LLM diagnosis as plain text for the task logs and the exception message."""

from __future__ import annotations

import textwrap
from typing import Any

from pydantic import BaseModel

_WIDTH = 88  # long unwrapped paragraphs force horizontal scrolling in log viewers
_DIVIDER = "-" * 80
_INDENT = "  "


def format_diagnosis(diagnosis: BaseModel) -> str:
    """
    Render a diagnosis as a wrapped, clearly delimited block.

    ``diagnosis`` can be Cosmos's built-in ``DbtFailureDiagnosis`` or a user model set with
    ``AIConfig.diagnosis_output_type``, so the output is driven by the model's fields and not by hardcoded names. A
    ``confidence`` field, if present, goes to the header, and every other field becomes a section.
    """
    fields = diagnosis.model_dump(mode="json")
    confidence = fields.pop("confidence", None)
    header = "AI diagnosis" if confidence is None else f"AI diagnosis (confidence: {confidence})"

    lines = [_DIVIDER, header, _DIVIDER]
    for name, value in fields.items():
        lines += [f"{name.replace('_', ' ').capitalize()}:", *_render(value, level=1), ""]
    return "\n".join([*lines, _DIVIDER])


def _render(value: Any, level: int) -> list[str]:
    """Render a value as wrapped lines: one item per line for lists and dicts, with the nested ones indented."""
    pad = _INDENT * level
    if isinstance(value, dict):
        items = [(f"{key}:", item) for key, item in value.items()]
    elif isinstance(value, list):
        items = [("-", item) for item in value]
    else:
        return _wrap(str(value), pad, pad)
    if not items:
        return [f"{pad}(none)"]

    lines: list[str] = []
    for label, item in items:
        if isinstance(item, (dict, list)) and item:
            nested = _render(item, level + 1)
            if isinstance(value, list):
                nested[0] = f"{pad}- {nested[0].lstrip()}"  # the bullet goes on the first line of the nested block
                lines += nested
            else:
                lines += [f"{pad}{label}", *nested]
        else:
            lines += _wrap(f"{label} {'(none)' if isinstance(item, (dict, list)) else item}", pad, pad + _INDENT)
    return lines


def _wrap(text: str, initial_indent: str, subsequent_indent: str) -> list[str]:
    """
    Wrap each line of ``text`` on its own, so the line breaks and indentation of a SQL or YAML snippet survive.

    Long tokens, such as table names, paths and URLs, are kept whole: a line that is too long is better than a token
    that can't be copied.
    """
    wrapped: list[str] = []
    for index, line in enumerate(text.splitlines() or [""]):
        rows = textwrap.wrap(
            line,
            width=_WIDTH,
            initial_indent=initial_indent if index == 0 else subsequent_indent,
            subsequent_indent=subsequent_indent,
            replace_whitespace=False,
            break_long_words=False,
            break_on_hyphens=False,
        )
        wrapped += rows or [""]  # an empty line, which ``textwrap`` drops
    return wrapped
