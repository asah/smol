# Agent Guidance

This file provides personality, collaboration, and coding guidance for the
agent working in this repository. Architecture lives in README.md.
Implementation details live in AGENT_NOTES.md.

Personality & Workflow
- Be concise, direct, and friendly. Explain what you’re about to do before
  running commands. Keep plans small and actionable.
- Use the `update_plan` tool for multi-step work. Keep exactly one step
  in progress. Summarize changes, not the whole plan.
- Group shell reads logically; avoid noisy preambles for trivial reads.
- Prefer surgical patches over sprawling refactors; keep changes minimal.

Coding Conventions
- Language: C; PostgreSQL server style. 4-space indents, K&R braces,
  snake_case for functions/vars, CamelCaps for types.
- Follow PG idioms: palloc/pfree, ereport/elog, Buffer/Page APIs.
- Keep helpers static unless required by IndexAmRoutine.
- Do not add new files unless necessary for the task; keep module in `smol.c`.

Build, Test, Run
- One container name only: `smol`. Always use/reuse the same container.
- Always run in Docker (PostgreSQL 18 toolchain). Build the image with
  `make dockerbuild`, then invoke inside-* targets via Docker exec.
- Quick regression: `docker exec -it smol make insidecheck` (builds, runs, stops PG).
- Clean builds: `docker exec -it smol make insidebuild`.
- Start/stop PG: `docker exec -it smol make insidestart` / `docker exec -it smol make insidestop`.
- Regression tests live in `sql/`. Benchmarks live in `bench/` (run via `insidebench-smol-btree-5m`).
- Keep regression fast; stream long benchmark output to the console.

Docs Discipline
- Architecture and user-facing overview: README.md.
- Code-level design, gotchas, invariants: AGENT_NOTES.md.
- Keep comments up-to-date and focused on current design (no changelogs).

PR/Commit Hygiene
- Commits: imperative subject (≤72 chars) + concise body with what/why.
- Separate mechanical refactors from functional changes.

Decision Principles
- Favor correctness and clarity. Optimize only after invariants are enforced.
- Avoid leaking complexity into user paths; keep defaults sensible.
