---
name: hyperredis-commit-guidelines
description: Commit and Pull Request guidelines for HyperRedis. Use when the user asks to commit, push, or wrap up a PR to ensure consistent commit messages and mandatory build/test verification.
---

# HyperRedis Commit Guidelines

Follow these rules for staging, committing, and pushing changes.

## Commit Message Style

Use short, imperative commit messages with a bracketed type prefix.
- `[feature]`: New features or capabilities.
- `[fix]`: Bug fixes.
- `[chore]`: Build system changes, documentation, or other non-production code updates.

Examples:
- `[feature] add skip list skeleton`
- `[fix] correct object lifetime handling`
- `[chore] update CMake source lists`

## Direct Release Path (Mandatory)

When asked to commit and push:
1. **Build**: Run `cmake --build build`.
2. **Test**: Run `ctest --test-dir build --output-on-failure`.
3. **Stage**: If tests pass, stage only the requested or related changes. Do not perform opportunistic cleanup or formatting.
4. **Commit & Push**: Commit with the project style and push to the upstream branch immediately.

**If tests fail:** Report the concrete blocker to the user and stop. Do not attempt to fix unrelated issues or proceed with the commit.

## Pull Request Guidelines

When preparing a PR description:
- Describe the behavior change.
- List touched modules.
- Mention any `CMakeLists.txt` updates.
- Include evidence of successful builds or tests.
