# AGENTS.md

## Project scope
These instructions apply to the entire repository.

## Project overview
- Stack: Google Apps Script backend (`Code.gs`) + HtmlService frontend (`Index.html`).
- Deployment target: Apps Script web app backed by Google Sheets history data.

## Codex working agreement
1. **Preserve deployment safety**
   - Do not commit API keys, tokens, or private sheet IDs.
   - Keep all sensitive values in Script Properties.
2. **Change discipline**
   - Make focused edits and avoid broad refactors unless explicitly requested.
   - Prefer additive, backward-compatible changes for dashboard data contracts.
3. **Validation before commit**
   - At minimum: run `node -e "JSON.parse(require('fs').readFileSync('appsscript.json','utf8'))"` after manifest edits.
   - If UI/behavior changes, sanity check by reviewing generated HTML/JS sections for syntax errors.
4. **GitHub hygiene**
   - Use clear commit messages: `<type>: <short summary>` (e.g., `docs: add agent and workflow guidance`).
   - In PRs, include: summary, risk, test evidence, and rollback notes when relevant.
5. **Documentation updates**
   - Update this file when recurring workflow lessons emerge.
   - Keep file headers in `Code.gs` and `Index.html` intact and current.

## Suggested PR checklist
- [ ] No secrets or credentials added.
- [ ] Scope is clear and minimal.
- [ ] Manifest remains valid JSON.
- [ ] Any user-facing behavior changes are documented.
- [ ] Tests/checks and outcomes are listed in the PR description.
