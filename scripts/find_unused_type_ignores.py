#!/usr/bin/env python3
# Find `# type: ignore` comments reported as unused in *every* matrix cell.
#
# Usage:
#   scripts/find_unused_type_ignores.py run [ENV ...]    # run mypy in each env, cache output
#   scripts/find_unused_type_ignores.py intersect        # intersect cached outputs
#
# The default ENV set covers Airflow 2.x + 3.x and old + new dbt so any ignore
# in the intersection should be removable without breaking the support matrix.
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

DEFAULT_ENVS = [
    "tests.py3.10-2.9-1.5",
    "tests.py3.11-2.10-1.9",
    "tests.py3.12-3.0-1.9",
    "tests.py3.12-3.2-2.0",
]
CACHE_DIR = Path(".unused_ignores_cache")
UNUSED_RE = re.compile(r'^(?P<file>[^:]+):(?P<line>\d+): error: Unused "type: ignore.*?" comment')


def env_to_filename(env: str) -> Path:
    return CACHE_DIR / f"{env.replace('.', '_').replace('/', '_')}.txt"


def run_one(env: str) -> Path:
    out_path = env_to_filename(env)
    print(f"[run] {env} -> {out_path}", file=sys.stderr)
    proc = subprocess.run(
        [
            "hatch",
            "-e",
            env,
            "run",
            "python",
            "-m",
            "mypy",
            "cosmos",
            "--warn-unused-ignores",
            "--no-incremental",
        ],
        capture_output=True,
        text=True,
    )
    # mypy exits non-zero when it reports errors (including unused-ignore), which is expected.
    # Only abort if there's no output at all -- that usually means the env failed to provision.
    if not proc.stdout.strip():
        print(f"[run] {env} produced no output. stderr:\n{proc.stderr}", file=sys.stderr)
        sys.exit(2)
    out_path.write_text(proc.stdout)
    return out_path


def parse(path: Path) -> set[tuple[str, int]]:
    hits: set[tuple[str, int]] = set()
    for line in path.read_text().splitlines():
        m = UNUSED_RE.match(line)
        if m:
            hits.add((m["file"], int(m["line"])))
    return hits


def intersect(paths: list[Path]) -> set[tuple[str, int]]:
    if not paths:
        return set()
    sets = [parse(p) for p in paths]
    common = set.intersection(*sets)
    for path, hits in zip(paths, sets):
        print(f"  {path.name}: {len(hits):4d} unused", file=sys.stderr)
    print(f"  intersection: {len(common):4d}", file=sys.stderr)
    return common


def report(common: set[tuple[str, int]]) -> None:
    by_file: dict[str, list[int]] = defaultdict(list)
    for file, line in common:
        by_file[file].append(line)
    for file in sorted(by_file):
        lines = sorted(by_file[file])
        print(f"{len(lines):3d}  {file}  {','.join(str(line) for line in lines)}")
    print(f"---\ntotal files: {len(by_file)}    total ignores: {sum(len(v) for v in by_file.values())}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="Run mypy in each env and cache output")
    run.add_argument("envs", nargs="*", default=DEFAULT_ENVS)
    sub.add_parser("intersect", help="Intersect cached outputs from a previous run")

    args = parser.parse_args()
    CACHE_DIR.mkdir(exist_ok=True)

    if args.cmd == "run":
        envs = args.envs or DEFAULT_ENVS
        paths = [run_one(env) for env in envs]
        report(intersect(paths))
    else:
        paths = sorted(CACHE_DIR.glob("*.txt"))
        if not paths:
            print(f"No cached outputs found in {CACHE_DIR}/. Run 'run' first.", file=sys.stderr)
            return 1
        report(intersect(paths))
    return 0


if __name__ == "__main__":
    sys.exit(main())
