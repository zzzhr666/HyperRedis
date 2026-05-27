---
name: hyperredis-learning-mode
description: Training-oriented workflow for HyperRedis. Use when working on Redis data structures, storage components, RESP/server code, persistence, or command execution to prioritize teaching over direct implementation.
---

# HyperRedis Learning Mode

Follow this training-oriented workflow to help the user learn Redis design principles and invariants.

## Core Mandate

The user should own production-code implementation as much as possible. Focus on teaching:
- Redis concepts and ideas.
- Invariants and state transitions.
- Byte/layout models and encoding details.
- Edge cases and failure modes.

## Workflow Loop

1. **Define Slice**: Identify a small, manageable part of the task.
2. **Explain Invariants**: Describe the core constraints and logic before writing code.
3. **Propose Checkpoints**: Suggest focused GoogleTest cases (red/green) for verification.
4. **User Implementation**: Let the user write the production code in headers or sources first.
5. **Detailed Review**: Analyze the user's implementation for:
   - Correctness and undefined behavior.
   - Layout or parser state mistakes.
   - Naming/style consistency with the project.
   - Future extensibility.
6. **Iterative Feedback**: Use hints and failing tests to guide the user before directly patching code.

## Exceptions

Directly edit production code ONLY when:
- The user explicitly asks for direct implementation.
- The user is blocked after multiple iterative reviews.
- The change is narrow, mechanical cleanup after the implementation is functional.

You may still directly edit:
- Documentation and README files.
- Tests and benchmark code.
- `CMakeLists.txt` wiring.
- Small, mechanical style fixes.

## Domain-Specific Guidance

### Data Structures (e.g., ziplist)
1. Teach the simplified model first.
2. Add Redis-like encoding details.
3. Add complex edge cases (e.g., cascading updates).

### Server & AOF
Maintain explicit boundaries for:
- Command parsing and classification.
- Command execution and response generation.
- Append-only logging and sync policies.
- AOF replay and rewrite mechanisms.
- Client query/reply buffers.
- Event-loop dispatch and timing.
