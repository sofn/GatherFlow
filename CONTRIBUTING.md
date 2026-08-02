# Contributing to GatherFlow

## Requirements

- Java 25+
- Gradle 8+

The project uses preview features from [JEP 485](https://openjdk.org/jeps/485) (`java.util.stream.Gatherer`). The build script already enables `--enable-preview` for compilation and test execution, so no manual flag is needed when running Gradle tasks.

## Build and Test

```bash
# Compile, run all tests, and verify JaCoCo coverage
./gradlew check

# Run tests only
./gradlew test

# Generate JaCoCo HTML report (opens build/reports/jacoco/index.html)
./gradlew jacocoTestReport
```

Coverage verification is part of `check` and enforces:

- Line coverage >= 90%
- Branch coverage >= 80%

## Git Hooks

To enable project-specific Git hooks, run:

```bash
git config core.hooksPath .githooks
```

If `.githooks/commit-msg` exists, also make it executable:

```bash
chmod +x .githooks/commit-msg
```

If the commit-msg hook file is not present, only `git config core.hooksPath .githooks` is required; the `chmod` step can be skipped.

## Submitting Changes

- Ensure `./gradlew check` passes before opening a pull request.
- Keep `README.md` and `README.zh-CN.md` in sync: same structure and examples, with Chinese translations in the Chinese file.
- Do not add Devin-generated signatures to commit messages.
