#!/usr/bin/env python3
# Tighten every bare `# type: ignore` in cosmos/ into `# type: ignore[code, ...]`.
#
# Strategy:
#   1. Strip every bare `# type: ignore` in cosmos/ in-memory and write the
#      patched files to disk.
#   2. Run `hatch -e <env> run python -m mypy cosmos --no-incremental` for each
#      env in ENVS and collect the error codes mypy emits for each (file, line)
#      where we removed an ignore.
#   3. Restore the originals.
#   4. Rewrite each bare ignore as `# type: ignore[<union of codes across envs>]`.
#      Lines where no env emitted a code are left alone and printed as
#      "candidate for deletion" (those belong in a later PR).
#
# Usage:
#   scripts/tighten_type_ignores.py            # dry-run: show what would change
#   scripts/tighten_type_ignores.py --apply    # actually rewrite files
#   scripts/tighten_type_ignores.py --apply --envs tests.py3.11-2.10-1.9 ...
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ENVS = [
    "tests.py3.10-2.9-1.5",
    "tests.py3.11-2.10-1.9",
    "tests.py3.12-3.0-1.9",
    "tests.py3.12-3.2-2.0",
]
ROOT = Path("cosmos")
BARE_RE = re.compile(r"#\s*type:\s*ignore(?!\[)")
ERROR_RE = re.compile(r"^(?P<file>[^:]+):(?P<line>\d+): error: .*\[(?P<code>[a-z0-9\-, ]+)\]\s*$")


def find_bare_ignores() -> dict[Path, set[int]]:
    hits: dict[Path, set[int]] = defaultdict(set)
    for py in ROOT.rglob("*.py"):
        for i, line in enumerate(py.read_text().splitlines(), start=1):
            if BARE_RE.search(line):
                hits[py].add(i)
    return hits


def strip_bare(py: Path) -> str:
    original = py.read_text()
    patched = "\n".join(BARE_RE.sub("", line).rstrip() for line in original.splitlines())
    if original.endswith("\n"):
        patched += "\n"
    py.write_text(patched)
    return original


def codes_per_env(env: str, targets: dict[Path, set[int]]) -> dict[tuple[str, int], set[str]]:
    print(f"[mypy] {env}", file=sys.stderr)
    proc = subprocess.run(
        ["hatch", "-e", env, "run", "python", "-m", "mypy", str(ROOT), "--no-incremental"],
        capture_output=True,
        text=True,
    )
    if not proc.stdout.strip():
        print(f"[mypy] {env} produced no stdout; stderr:\n{proc.stderr}", file=sys.stderr)
        sys.exit(2)
    codes: dict[tuple[str, int], set[str]] = defaultdict(set)
    target_keys = {(str(p), line) for p, lines in targets.items() for line in lines}
    for out_line in proc.stdout.splitlines():
        m = ERROR_RE.match(out_line)
        if not m:
            continue
        key = (m["file"], int(m["line"]))
        if key not in target_keys:
            continue
        for code in m["code"].split(","):
            codes[key].add(code.strip())
    return codes


def rewrite(py: Path, codes_by_line: dict[int, list[str]]) -> tuple[int, list[int]]:
    text = py.read_text()
    out_lines: list[str] = []
    rewritten = 0
    leave_alone: list[int] = []
    for i, line in enumerate(text.splitlines(), start=1):
        if not BARE_RE.search(line):
            out_lines.append(line)
            continue
        codes = codes_by_line.get(i, [])
        if codes:
            replacement = f"# type: ignore[{', '.join(codes)}]"
            out_lines.append(BARE_RE.sub(replacement, line))
            rewritten += 1
        else:
            out_lines.append(line)
            leave_alone.append(i)
    result = "\n".join(out_lines)
    if text.endswith("\n"):
        result += "\n"
    py.write_text(result)
    return rewritten, leave_alone


def gather_codes(envs: list[str], targets: dict[Path, set[int]]) -> dict[tuple[str, int], set[str]]:
    originals: dict[Path, str] = {}
    aggregated: dict[tuple[str, int], set[str]] = defaultdict(set)
    try:
        for py in targets:
            originals[py] = strip_bare(py)
        for env in envs:
            for key, codes in codes_per_env(env, targets).items():
                aggregated[key] |= codes
    finally:
        for py, text in originals.items():
            py.write_text(text)
    return aggregated


def print_plan(
    targets: dict[Path, set[int]],
    rewrites: dict[Path, dict[int, list[str]]],
) -> None:
    print("\n=== Plan ===")
    rewrite_count = 0
    deletion_candidates: list[tuple[Path, int]] = []
    for py, all_lines in sorted(targets.items()):
        for line in sorted(all_lines):
            codes = rewrites.get(py, {}).get(line)
            if codes:
                rewrite_count += 1
                print(f"  rewrite {py}:{line}  -> [{', '.join(codes)}]")
            else:
                deletion_candidates.append((py, line))
    print(f"\nrewrites: {rewrite_count}")
    print(f"deletion candidates (no code in any env): {len(deletion_candidates)}")
    for py, line in deletion_candidates:
        print(f"  drop?   {py}:{line}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--envs", nargs="*", default=ENVS, help="Hatch envs to query")
    args = parser.parse_args()

    targets = find_bare_ignores()
    total = sum(len(v) for v in targets.values())
    print(f"Found {total} bare # type: ignore across {len(targets)} files.", file=sys.stderr)
    if not targets:
        return 0

    aggregated = gather_codes(args.envs, targets)
    rewrites: dict[Path, dict[int, list[str]]] = defaultdict(dict)
    for (file, line), codes in aggregated.items():
        rewrites[Path(file)][line] = sorted(codes)

    print_plan(targets, rewrites)

    if args.apply:
        print("\n=== Applying ===")
        for py in sorted(rewrites):
            n, leave = rewrite(py, rewrites[py])
            print(f"  {py}: rewrote {n}, left {len(leave)} bare (deletion candidates)")
    else:
        print("\nDry run. Re-run with --apply to write changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
