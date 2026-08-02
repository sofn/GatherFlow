# Changelog

## [Unreleased]

### Build / CI

- Upgraded CI workflows to JDK 25 and pinned GitHub Actions.
- Restored the official Gradle wrapper.
- Fixed dependency versions and JaCoCo configuration; `check` now depends on coverage verification.

### Core Operator Correctness

- Fixed `downstream.push` return propagation across gatherers so short-circuiting downstream operators (e.g. `limit`, `findFirst`) correctly stop upstream processing.
- Rewrote `sliding` semantics and `slidingTimeWindow` implementation to handle trailing/partial windows and out-of-order or negative timestamps correctly.
- Fixed `SequenceGatherers` null handling and edge cases in `scanRight` accumulation order and `cycle` total-count behavior.
- Fixed `WindowGatherers` window correctness, push propagation, and timestamp edge cases.
- Fixed `ReactiveGatherers` downstream push propagation and exception/timestamp handling.

### Null and Timestamp Handling

- Added null-aware handling for time-based and window operators.
- Hardened timestamp extraction edge cases for `debounce`, `throttleFirst`, `throttleLast`, `bufferTime`, `delay`, `timeInterval`, `timestamp`, `sessionWindow`, `tumblingTimeWindow`, and `slidingTimeWindow`.

### Error Handling and Side-Effect Semantics

- Corrected `doOnError` to observe only exceptions thrown while pushing an element downstream, then re-throw them; it does not catch upstream `map`/`filter` errors.
- Corrected `doFinally` to fire only on normal completion with the string `"complete"`; Java Streams have no error channel, so it cannot observe errors.
- Clarified `withLatestFrom` semantics: it consumes one `other` value per main element, reuses the latest value once `other` is exhausted, and skips main elements until the first `other` value arrives.

### Documentation

- Fixed README license from `[MIT](./LICENSE)` to `[Apache-2.0](./LICENSE)`.
- Added `Project Goals / Non-Goals`, `API Stability: Experimental`, `What this library is NOT`, and `Getting Started` sections.
- Corrected operator descriptions and signatures (`scanRight`, `mapWithIndex`, `doFinally`, `doOnError`, `withLatestFrom`, `connect`/`coMap`, `cycle`).
- Added `windowApply` and `windowCount` to the Window Result Operators table.
- Updated the Window Feasibility Summary table with `Union` as Partial and added caveats to Global, Tumbling, Sliding, and Session windows.
- Fixed project structure tree indentation for `StreamingOperatorTest.java`.
- Added `ARCHITECTURE.md`, `CONTRIBUTING.md`, and this `CHANGELOG.md`.
