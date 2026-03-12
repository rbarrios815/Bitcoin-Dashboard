# Contributing

## Branch and commit conventions
- Create small, reviewable commits.
- Use commit prefixes like `feat:`, `fix:`, `docs:`, `chore:`.
- Keep commit subjects under ~72 characters when practical.

## Codex + GitHub workflow
1. Confirm task scope and inspect existing instructions (`AGENTS.md`).
2. Make minimal, targeted changes.
3. Run basic validation checks (manifest parse, lint/syntax checks where available).
4. Commit with a descriptive message.
5. Open a PR with:
   - What changed
   - Why it changed
   - Risk/impact
   - Verification commands and results

## Security
- Never hardcode API keys in source files.
- Use Apps Script **Script Properties** for secrets.
