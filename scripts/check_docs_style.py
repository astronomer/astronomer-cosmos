#!/usr/bin/env python3
"""Lightweight style checks for Cosmos RST docs.

Each rule is a small function registered in :data:`RULES`. ``check_file``
walks every line of an ``.rst`` / ``.jinja2`` file, tracks whether the
current line is inside a literal-block directive (code blocks,
literalinclude, or table directives) where structural markers must
not be treated as prose, and
dispatches every other line to each rule. Add a new rule by writing a
function that takes a :class:`LineContext` and yields
``(lineno, message)`` pairs, then append it to ``RULES``.

The script exits non-zero when any rule reports a violation. Style guide
text and rationale belong in ``docs/policy/contributing-docs.rst``, not
here — this module only enforces the rules.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

LITERAL_DIRECTIVE_RE = re.compile(
    r"^(?P<indent>\s*)\.\. (code-block|sourcecode|literalinclude|highlight|list-table|csv-table)::"
)
BULLET_RE = re.compile(r"^(\s*)\* ")
UNDERLINE_CHARS = set("=-~+^*#\"'")


@dataclass
class LineContext:
    """Read-only view of a single line and its surrounding file."""

    path: Path
    lineno: int  # 1-based
    line: str
    lines: list[str]  # full file split by '\n'

    @property
    def stripped(self) -> str:
        return self.line.strip()

    def previous(self) -> str:
        return self.lines[self.lineno - 2] if self.lineno >= 2 else ""

    def next(self) -> str:
        idx = self.lineno
        return self.lines[idx] if idx < len(self.lines) else ""


Rule = Callable[[LineContext], Iterable[tuple[int, str]]]


class LiteralBlockTracker:
    """Track whether the current line is inside an RST directive whose
    content should not be treated as prose: code blocks, literalinclude,
    and table directives where the markers (``* -``) are structural, not
    bullets. Plain ``.. include::`` is excluded because its body is
    re-parsed as RST rather than treated as a literal block.

    A directive like ``.. code-block:: python`` opens a region whose
    content lines are indented strictly further than the directive
    itself. The region ends at the first non-blank line whose indent is
    less than or equal to the directive's indent.
    """

    def __init__(self) -> None:
        self._directive_indent: int | None = None
        self._in_body: bool = False

    def feed(self, line: str) -> bool:
        """Advance state with one line; return True if the line is inside
        a literal-block directive."""
        stripped = line.lstrip()
        line_indent = len(line) - len(stripped)

        m = LITERAL_DIRECTIVE_RE.match(line)
        if m:
            self._directive_indent = len(m.group("indent"))
            self._in_body = False
            return False

        if self._directive_indent is None:
            return False

        if stripped == "":
            return self._in_body

        if not self._in_body:
            if line_indent > self._directive_indent:
                self._in_body = True
                return True
            self._directive_indent = None
            return False

        if line_indent <= self._directive_indent:
            self._directive_indent = None
            self._in_body = False
            return False

        return True


def _is_underline(s: str) -> bool:
    return bool(s) and len(set(s)) == 1 and s[0] in UNDERLINE_CHARS and len(s) >= 3


# --- Rules ----------------------------------------------------------------


def rule_no_star_bullets(ctx: LineContext) -> Iterator[tuple[int, str]]:
    if BULLET_RE.match(ctx.line):
        yield ctx.lineno, "bullet uses '*'; switch to '- '"


def rule_no_decorative_separators(ctx: LineContext) -> Iterator[tuple[int, str]]:
    bare = ctx.stripped
    if not (bare and len(set(bare)) == 1 and bare[0] in "-=" and len(bare) >= 30):
        return
    if ctx.previous().strip() == "" and ctx.next().strip() == "":
        yield (
            ctx.lineno,
            f"decorative separator line of '{bare[0]}'; " "remove it and rely on heading hierarchy",
        )


def rule_underline_long_enough(ctx: LineContext) -> Iterator[tuple[int, str]]:
    bare = ctx.stripped
    if not _is_underline(bare):
        return
    title = ctx.previous()
    if not title.strip():
        return
    if _is_underline(title.strip()):
        return
    title_bytes = len(title.encode("utf-8"))
    if len(bare) < title_bytes:
        yield (
            ctx.lineno,
            f"heading underline length {len(bare)} " f"< title byte length {title_bytes} (title: {title!r})",
        )


RULES: list[Rule] = [
    rule_no_star_bullets,
    rule_no_decorative_separators,
    rule_underline_long_enough,
]


# --- File walk ------------------------------------------------------------


def check_file(path: Path, rules: list[Rule] | None = None) -> list[str]:
    if rules is None:
        rules = RULES
    lines = path.read_text(encoding="utf-8").split("\n")
    tracker = LiteralBlockTracker()
    errors: list[str] = []

    for i, line in enumerate(lines):
        if tracker.feed(line):
            continue
        ctx = LineContext(path=path, lineno=i + 1, line=line, lines=lines)
        for rule in rules:
            for lineno, msg in rule(ctx):
                errors.append(f"{path}:{lineno}: {msg}")

    return errors


def main(argv: list[str]) -> int:
    errors: list[str] = []
    for arg in argv:
        path = Path(arg)
        if path.suffix not in {".rst", ".jinja2"}:
            continue
        errors.extend(check_file(path))

    for e in errors:
        print(e, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
