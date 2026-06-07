# CLAUDE.md

This file is read by [Claude Code](https://claude.com/claude-code) (`claude.ai/code`) when working in this repository.

For project conventions (commands, architecture, coding standards, commit-message format, documentation style, etc.), see [`AGENTS.md`](AGENTS.md). That file is the single source of truth for agent-neutral guidance and is read by other AI coding agents (Cursor, Codex, Copilot, …) as well.

## Always read AGENTS.md before running commands

Before running tests, type-checks, linters, docs builds, or any project command — or before telling the user a command does not exist — read the relevant section of [`AGENTS.md`](AGENTS.md) and use the exact command written there. Do not guess, and do not fall back to a bare `pytest`/`mypy`/`black` invocation. In particular, tests run through hatch matrix environments whose names are not discoverable by guessing (e.g. `hatch run tests.py3.11-2.10-1.9:test`); the canonical form lives in the "Running Tests" section of AGENTS.md.
