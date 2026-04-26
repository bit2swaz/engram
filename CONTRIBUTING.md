# Contributing to engram

Welcome! Contributions of all kinds are encouraged and appreciated.

## Development Environment Setup

1. **Prerequisites:**
   - Rust (stable)
   - Docker (for Redis)
   - OpenAI API key
2. **Clone the repository:**
   ```sh
   git clone https://github.com/bit2swaz/engram.git
   cd engram
   ```
3. **Set up environment variables:**
   ```sh
   cp .env.example .env
   # Edit .env and fill in your OpenAI API key
   ```
4. **Start Redis:**
   ```sh
   docker compose up -d redis
   # or
   docker run -d --name engram-redis -p 6379:6379 redis:7-alpine
   ```
5. **Build the project:**
   ```sh
   cargo build
   ```

## Running Tests

- **Unit tests:**
  ```sh
  cargo test
  ```
- **Integration tests (requires Docker):**
  ```sh
  cargo test --test integration
  ```
- **All tests:**
  ```sh
  cargo test --all-targets
  ```
- Note: Integration tests use `testcontainers` to spin up real dependencies (Redis, LanceDB).

## TDD Workflow

This project uses a strict test-driven development (TDD) workflow:
- **Red:** Write a failing test that describes the desired behavior.
- **Green:** Implement the minimum code needed to make the test pass.
- **Refactor:** Clean up the code while keeping all tests green.

## Branch Naming Convention

- `feat/short-description` (new features)
- `fix/issue-number` (bug fixes)
- `docs/readme-update` (documentation)
- `test/unit-coverage` (tests)
- `chore/dependency-update` (maintenance)

## Commit Message Format

Follow [Conventional Commits](https://www.conventionalcommits.org/):
- `type: short description`
- Types: `feat`, `fix`, `docs`, `test`, `chore`, `bench`, `ci`
- Example: `feat: add redis-backed short term store`

## Pull Requests

- PRs should link to an issue when possible.
- Include a clear summary of the change.
- All tests and CI must pass before merging.

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md).
